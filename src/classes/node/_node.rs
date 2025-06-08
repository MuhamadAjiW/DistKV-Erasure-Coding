use core::panic;
use std::sync::{Arc, Mutex};

use actix_web::{web, App, HttpServer};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use tokio::sync::{mpsc, oneshot};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{debug, error, info, instrument, warn};

use crate::base_libs::_ec::ECKeyValue;
use crate::base_libs::network::_messages::receive_omnipaxos_message;
use crate::config::_constants::ELECTION_TICK_TIMEOUT;
use crate::{
    base_libs::{
        _types::{OmniPaxosECKV, OmniPaxosECMessage},
        network::_address::Address,
    },
    classes::config::_config::Config,
    config::_constants::RECONNECT_INTERVAL,
};
use omnipaxos::{
    erasure::ec_service::ECService, ClusterConfigEC, OmniPaxosECConfig, ServerConfigEC,
};

#[derive(Debug)]
pub enum OmniPaxosRequest {
    Client {
        entry: ECKeyValue,
        response: oneshot::Sender<String>,
    },
    Network {
        message: OmniPaxosECMessage,
    },
}

pub struct Node {
    // Base attributes
    pub running: bool,

    // HTTP Interface
    pub http_address: Address,
    pub http_max_payload: usize,

    // Internode communication
    pub address: Address,
    pub socket: Arc<TcpListener>,

    // Cluster info
    pub cluster_list: Arc<RwLock<Vec<String>>>,
    pub cluster_index: usize,

    // Omnipaxos
    pub omnipaxos_sender: mpsc::Sender<OmniPaxosRequest>,
    pub omnipaxos: Arc<Mutex<OmniPaxosECKV>>, // Store the actual OmniPaxosECKV instance
}

impl Node {
    pub async fn from_config(address: Address, config_path: &str) -> Self {
        let config = Config::get_config(config_path).await;
        let index = Config::get_node_index(&config, &address);
        let mut cluster_list: Vec<String> = Config::get_node_addresses(&config)
            .iter()
            .map(|a| a.to_string())
            .collect();
        cluster_list.sort(); // Ensure deterministic order
        info!("[INIT] Cluster list (sorted): {:?}", cluster_list);
        if let Some(index) = index {
            info!("[INIT] This node index: {} (NodeId: {})", index, index + 1);
            let node_conf = &config.nodes[index];
            let http_address = Address::new(&node_conf.ip, node_conf.http_port);

            // Build NodeIds from sorted cluster_list
            let nodes: Vec<u64> = cluster_list
                .iter()
                .enumerate()
                .map(|(i, _)| (i + 1) as u64)
                .collect();
            let cluster_config = ClusterConfigEC {
                configuration_id: 1,
                nodes,
                flexible_quorum: None,
            };
            let server_config = ServerConfigEC {
                pid: (index + 1) as u64,
                election_tick_timeout: ELECTION_TICK_TIMEOUT,
                erasure_coding_service: ECService::new(
                    config.storage.shard_count,
                    config.storage.parity_count,
                )
                .unwrap(),
                ..Default::default()
            };
            let omnipaxos_config = OmniPaxosECConfig {
                cluster_config,
                server_config,
            };
            let storage_config =
                PersistentStorageConfig::with_path(config.nodes[index].rocks_db.path.clone());
            let omnipaxos: OmniPaxosECKV = omnipaxos_config
                .build(PersistentStorage::new(storage_config))
                .unwrap();

            Node::new(
                address,
                http_address,
                config.storage.max_payload_size,
                cluster_list,
                index,
                omnipaxos,
            )
            .await
        } else {
            panic!("Error: Failed to get node config");
        }
    }

    pub async fn new(
        address: Address,
        http_address: Address,
        http_max_payload: usize,
        cluster_list: Vec<String>,
        cluster_index: usize,
        omnipaxos: OmniPaxosECKV,
    ) -> Self {
        info!("[INIT] binding address: {}", address);
        let socket = loop {
            match TcpListener::bind(address.to_string()).await {
                Ok(sock) => {
                    debug!("[TCP] Successfully bound to address: {}", address);
                    break Arc::new(sock);
                }
                Err(e) => {
                    warn!("[INIT] Failed to bind TCP: {}. Retrying...", e);
                    tokio::time::sleep(RECONNECT_INTERVAL).await;
                }
            }
        };
        let (omnipaxos_sender, mut omnipaxos_receiver) = mpsc::channel(128);
        let omnipaxos_arc = Arc::new(Mutex::new(omnipaxos));
        // Spawn OmniPaxos main event loop (handles incoming network messages and ticks)
        let omnipaxos_clone = omnipaxos_arc.clone();
        let peer_addresses = {
            let mut map = std::collections::HashMap::new();
            for (i, addr) in cluster_list.iter().enumerate() {
                map.insert((i + 1) as u64, addr.clone());
            }
            map
        };
        // Generate random jitter before entering async block to avoid Send issues
        let jitter_ms = {
            use rand::Rng;
            rand::rng().random_range(0..1000)
        };
        tokio::spawn(async move {
            debug!("[OMNIPAXOS] Initial jitter: {}ms", jitter_ms);
            tokio::time::sleep(std::time::Duration::from_millis(jitter_ms)).await;
            let mut tick_interval = tokio::time::interval(std::time::Duration::from_millis(200));
            let mut msg_buffer = Vec::with_capacity(32);
            loop {
                tokio::select! {
                    biased;
                    Some(req) = omnipaxos_receiver.recv() => {
                        let mut send_msgs = Vec::new();
                        match req {
                            OmniPaxosRequest::Network { message } => {
                                debug!("[OMNIPAXOS] Incoming BLE message: from {:?} to {:?} (msg: {:?})", message.get_sender(), message.get_receiver(), message);
                                {
                                    let mut omni = omnipaxos_clone.lock().unwrap();
                                    debug!("[OMNIPAXOS] Handling incoming network message: {:?}", message);
                                    omni.handle_incoming(message);
                                    omni.take_outgoing_messages(&mut msg_buffer);
                                } // MutexGuard dropped here
                                send_msgs.append(&mut msg_buffer);
                            }
                            OmniPaxosRequest::Client { .. } => {
                                // Client requests are ignored/commented for now
                            }
                        }
                        for msg in send_msgs.drain(..) {
                            let receiver = msg.get_receiver();
                            let sender = msg.get_sender();
                            if let Some(addr) = peer_addresses.get(&receiver) {
                                debug!("[OMNIPAXOS] Outgoing BLE message: from {:?} to {:?} at {} (msg: {:?})", sender, receiver, addr, msg);
                                let _ = crate::base_libs::network::_messages::send_omnipaxos_message(msg, addr, None).await;
                            } else {
                                debug!("[OMNIPAXOS] No peer address for receiver {}", receiver);
                            }
                        }
                    }
                    _ = tick_interval.tick() => {
                        let mut send_msgs = Vec::new();
                        {
                            let mut omni = omnipaxos_clone.lock().unwrap();
                            debug!("[OMNIPAXOS] Tick");
                            omni.tick();
                            if let Some((leader, _)) = omni.get_current_leader() {
                                debug!("[OMNIPAXOS] Current leader: {}", leader);
                            }
                            omni.take_outgoing_messages(&mut msg_buffer);
                        } // MutexGuard dropped here
                        send_msgs.append(&mut msg_buffer);
                        for msg in send_msgs.drain(..) {
                            let receiver = msg.get_receiver();
                            if let Some(addr) = peer_addresses.get(&receiver) {
                                debug!("[OMNIPAXOS] Sending outgoing BLE message to {} at {}: {:?}", receiver, addr, msg);
                                let _ = crate::base_libs::network::_messages::send_omnipaxos_message(msg, addr, None).await;
                            } else {
                                debug!("[OMNIPAXOS] No peer address for receiver {}", receiver);
                            }
                        }
                    }
                }
            }
        });
        info!(
            "[INIT] NodeId: {} | Cluster list: {:?}",
            cluster_index + 1,
            cluster_list
        );
        Node {
            running: true,
            http_address,
            http_max_payload,
            address,
            socket,
            cluster_list: Arc::new(RwLock::new(cluster_list)),
            cluster_index,
            omnipaxos_sender,
            omnipaxos: omnipaxos_arc,
        }
    }

    // Information logging
    pub async fn print_info(&self) {
        info!("-------------------------------------");
        info!("[INFO] Node info:");
        info!("Address: {}", &self.address.to_string());
        info!("Cluster list: {:?}", &self.cluster_list.read().await);
    }

    #[instrument(level = "debug", skip_all)]
    pub fn run_tcp_loop(node_arc: Arc<RwLock<Node>>) {
        tokio::spawn(async move {
            let socket = {
                let node = node_arc.read().await;
                node.socket.clone()
            };
            debug!(
                "[TCP] Listening for incoming TCP connections on {}",
                socket.local_addr().unwrap()
            );
            loop {
                match receive_omnipaxos_message(&socket).await {
                    Ok((_stream, message)) => {
                        debug!("[TCP] Received message: {:?}", message);
                        let node = node_arc.read().await;
                        let send_result = node
                            .omnipaxos_sender
                            .send(OmniPaxosRequest::Network { message })
                            .await;
                        if let Err(e) = send_result {
                            error!("[TCP] Failed to forward message to OmniPaxos: {}", e);
                        } else {
                            debug!("[TCP] Forwarded message to OmniPaxos event loop");
                        }
                    }
                    Err(e) => {
                        warn!("[TCP] Error receiving message: {}", e);
                        continue;
                    }
                }
            }
        });
    }

    #[instrument(level = "debug", skip_all)]
    pub fn run_http_loop(node_arc: Arc<RwLock<Node>>) {
        info!("[INIT] Starting HTTP loop...");

        // Actix web is not compatible with tokio spawn
        std::thread::spawn(move || {
            let sys = actix_web::rt::System::new();

            sys.block_on(async move {
                let (address, max_payload) = {
                    let node = node_arc.read().await;
                    (node.http_address.clone(), node.http_max_payload.clone())
                };

                info!(
                    "[INIT] Starting HTTP server on {}:{}",
                    address.ip, address.port
                );

                let http_server = loop {
                    let node_arc_clone = node_arc.clone();
                    match HttpServer::new(move || {
                        App::new()
                            .app_data(web::Data::new(node_arc_clone.clone()))
                            .app_data(web::PayloadConfig::new(max_payload))
                            .app_data(web::JsonConfig::default().limit(max_payload))
                            .route("/health", web::get().to(Node::http_healthcheck))
                            .route("/put", web::post().to(Node::http_put))

                        // TODO: These are not implemented yet
                        // .route("/get", web::post().to(Node::http_get))
                        // .route("/delete", web::post().to(Node::http_delete))
                    })
                    .bind((address.ip.as_str(), address.port))
                    {
                        Ok(server) => break server,
                        Err(e) => {
                            error!(
                                "[INIT] Failed to bind HTTP server to {}:{}: {}. Retrying in 1s...",
                                address.ip, address.port, e
                            );
                            std::thread::sleep(RECONNECT_INTERVAL);
                        }
                    }
                };

                http_server
                    .run()
                    .await
                    .expect("[ERROR] Actix server crashed");
            })
        });
    }

    pub async fn run(node_arc: Arc<RwLock<Node>>) {
        use crate::base_libs::network::_messages::send_omnipaxos_message;
        use crate::base_libs::network::_server::OmniPaxosServerEC;
        use omnipaxos::util::NodeId;
        use std::collections::HashMap;
        use tokio::sync::mpsc;

        // Build peer_addresses: NodeId -> String ("ip:port")
        let (my_pid, peer_addresses) = {
            let node = node_arc.read().await;
            let mut cluster_list = node.cluster_list.read().await.clone();
            cluster_list.sort(); // Ensure deterministic order
            let mut map = HashMap::new();
            for (i, addr) in cluster_list.iter().enumerate() {
                map.insert((i + 1) as NodeId, addr.clone());
            }
            info!("[INIT] Peer address map: {:?}", map);
            ((node.cluster_index + 1) as NodeId, map)
        };

        // Start TCP listener loop (already done in run_tcp_loop)
        Self::run_tcp_loop(node_arc.clone());
        info!("[INIT] Node is now running");

        // Start OmniPaxosServerEC with a local mpsc channel for self (not used for network)
        let (local_tx, local_rx) = mpsc::channel(128);
        let omni_paxos = {
            let node = node_arc.read().await;
            node.omnipaxos.clone()
        };
        let mut server = OmniPaxosServerEC {
            omni_paxos,
            incoming: local_rx, // not used for network
            outgoing: {
                let mut map = HashMap::new();
                map.insert(my_pid, local_tx.clone());
                map
            },
            peer_addresses: peer_addresses.clone(),
            message_buffer: vec![],
        };
        tokio::spawn(async move {
            loop {
                // Tick and send outgoing messages periodically
                server.omni_paxos.lock().unwrap().tick();
                server
                    .omni_paxos
                    .lock()
                    .unwrap()
                    .take_outgoing_messages(&mut server.message_buffer);
                for msg in server.message_buffer.drain(..) {
                    let receiver = msg.get_receiver();
                    if let Some(addr) = server.peer_addresses.get(&receiver) {
                        let _ = send_omnipaxos_message(msg, addr, None).await;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        });

        // Wait for shutdown
        tokio::signal::ctrl_c()
            .await
            .expect("[ERROR] Failed to listen for Ctrl+C signal");
        info!("[INIT] Ctrl+C signal received, stopping node...");
        {
            let mut node = node_arc.write().await;
            node.running = false;
            info!("[INIT] Node is stopping...");
            tokio::time::sleep(RECONNECT_INTERVAL).await;
        }
    }
}
