use core::panic;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use actix_web::{web, App, HttpServer};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, sleep};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{debug, error, info, instrument, warn};

use crate::base_libs::_ec::ECKeyValue;
use crate::base_libs::network::_messages::receive_omnipaxos_message;
use crate::base_libs::network::_messages::send_omnipaxos_message;
use crate::base_libs::network::_server::OmniPaxosServerEC;
use crate::config::_constants::{BUFFER_SIZE, ELECTION_TICK_TIMEOUT};
use crate::{
    base_libs::{
        _types::{OmniPaxosECKV, OmniPaxosECMessage},
        network::_address::Address,
    },
    classes::config::_config::Config,
    config::_constants::RECONNECT_INTERVAL,
};
use omnipaxos::erasure::log_entry::OperationType;
use omnipaxos::util::LogEntry;
use omnipaxos::util::NodeId;
use omnipaxos::{
    erasure::ec_service::ECService, ClusterConfigEC, OmniPaxosECConfig, ServerConfigEC,
};
use std::collections::HashMap;

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
    pub start_time: std::time::Instant,

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
    pub omnipaxos: Arc<Mutex<OmniPaxosECKV>>,
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
                    info!("[TCP] Successfully bound to address: {}", address);
                    break Arc::new(sock);
                }
                Err(e) => {
                    warn!("[INIT] Failed to bind TCP: {}. Retrying...", e);
                    tokio::time::sleep(RECONNECT_INTERVAL).await;
                }
            }
        };
        let (omnipaxos_sender, mut omnipaxos_receiver) = mpsc::channel(4096);
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
            info!("[OMNIPAXOS] Initial jitter: {}ms", jitter_ms);
            sleep(Duration::from_millis(jitter_ms)).await;
            let mut tick_interval = interval(Duration::from_millis(200));
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
                                    omni.take_outgoing_messages(&mut send_msgs);
                                }
                            }
                            OmniPaxosRequest::Client { entry, response } => {
                                debug!("[OMNIPAXOS] Client request: {:?}", entry);
                                let mut result = "Operation failed".to_string();
                                {
                                    let mut omni = omnipaxos_clone.lock().unwrap();

                                    // Handle the operation based on type
                                    match entry.op {
                                        OperationType::SET => {
                                            info!("[OMNIPAXOS] Appending SET entry for key: {}", entry.key);
                                            match omni.append(entry) {
                                                Ok(_) => result = "Value set successfully".to_string(),
                                                Err(e) => {
                                                    error!("[OMNIPAXOS] Failed to set value: {:?}", e);
                                                    result = format!("Failed to set value: {:?}", e)
                                                },
                                            }
                                        },
                                        OperationType::GET => {
                                            info!("[OMNIPAXOS] Processing GET for key: {}", entry.key);

                                            // _TODO: Create memory cache and retrieval from paxos nodes
                                            // Search through decided entries to find the key
                                            let mut key_found = false;
                                            if let Some(log_entries) = omni.read_decided_suffix(0) {
                                                for log_entry in log_entries.iter().rev() {

                                                    // Match on the LogEntry enum
                                                    match log_entry {
                                                        LogEntry::Decided(entry_data) => {
                                                            if entry_data.key == entry.key && entry_data.op == OperationType::SET {
                                                                // Found the most recent SET for this key
                                                                key_found = true;
                                                                let fragment = &entry_data.fragment;
                                                                if let Ok(value) = String::from_utf8(fragment.data.clone()) {
                                                                    result = value;
                                                                    break;
                                                                }
                                                            } else if entry_data.key == entry.key && entry_data.op == OperationType::DELETE {
                                                                // Key was deleted
                                                                key_found = true;
                                                                result = "Key not found (deleted)".to_string();
                                                                break;
                                                            }
                                                        },
                                                        // Ignore other types of entries
                                                        _ => continue,
                                                    }
                                                }
                                            }

                                            if !key_found {
                                                result = "Key not found".to_string();
                                            }
                                        },
                                        OperationType::DELETE => {
                                            info!("[OMNIPAXOS] Appending DELETE entry for key: {}", entry.key);
                                            match omni.append(entry) {
                                                Ok(_) => result = "Value deleted successfully".to_string(),
                                                Err(e) => {
                                                    error!("[OMNIPAXOS] Failed to delete value: {:?}", e);
                                                    result = format!("Failed to delete value: {:?}", e)
                                                },
                                            }
                                        },
                                        _ => {
                                            result = "Unsupported operation".to_string();
                                        }
                                    }

                                    // Send the result back to the HTTP handler
                                    if let Err(e) = response.send(result) {
                                        error!("[OMNIPAXOS] Failed to send response: {:?}", e);
                                    }
                                }
                            }
                        }
                        // Batch outgoing messages per peer
                        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosECMessage>> = HashMap::new();
                        for msg in send_msgs.drain(..) {
                            let receiver = msg.get_receiver();
                            peer_batches.entry(receiver).or_default().push(msg);
                        }
                        for (receiver, batch) in peer_batches {
                            if let Some(addr) = peer_addresses.get(&receiver) {
                                debug!("[OMNIPAXOS] Sending batch of {} messages to {} at {}", batch.len(), receiver, addr);
                                let _ = crate::base_libs::network::_messages::send_omnipaxos_message(batch, addr, None).await;
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
                            omni.take_outgoing_messages(&mut send_msgs);
                        }

                        // Batch outgoing messages per peer
                        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosECMessage>> = HashMap::new();
                        for msg in send_msgs.drain(..) {
                            let receiver = msg.get_receiver();
                            peer_batches.entry(receiver).or_default().push(msg);
                        }
                        for (receiver, batch) in peer_batches {
                            if let Some(addr) = peer_addresses.get(&receiver) {
                                debug!("[OMNIPAXOS] Sending batch of {} messages to {} at {}", batch.len(), receiver, addr);
                                let _ = crate::base_libs::network::_messages::send_omnipaxos_message(batch, addr, None).await;
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
            start_time: std::time::Instant::now(),
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
            info!(
                "[TCP] Listening for incoming TCP connections on {}",
                socket.local_addr().unwrap()
            );
            loop {
                match receive_omnipaxos_message(&socket).await {
                    Ok((_stream, messages)) => {
                        for message in messages {
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
                            .configure(Node::register_services)
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

    pub async fn run_omnipaxos_loop(node_arc: Arc<RwLock<Node>>) {
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

        let (local_tx, local_rx) = mpsc::channel(BUFFER_SIZE);
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
                {
                    let mut buffer = Vec::new();
                    {
                        let mut omni = server.omni_paxos.lock().unwrap();
                        omni.take_outgoing_messages(&mut buffer);
                    }
                    // Now send messages in buffer after lock is released
                    let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosECMessage>> = HashMap::new();
                    for msg in buffer {
                        let receiver = msg.get_receiver();
                        if let Some(local_channel) = server.outgoing.get_mut(&receiver) {
                            // Fast-path: local delivery
                            let _ = local_channel.send(msg).await;
                        } else if let Some(_addr) = server.peer_addresses.get(&receiver) {
                            peer_batches.entry(receiver).or_default().push(msg);
                        }
                    }
                    for (receiver, batch) in peer_batches {
                        if let Some(addr) = server.peer_addresses.get(&receiver) {
                            let _ = send_omnipaxos_message(batch, addr, None).await;
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        });
    }

    // Utility function to send a request to OmniPaxos and get the response
    pub async fn send_omnipaxos_request(&self, entry: ECKeyValue) -> String {
        let (tx, rx) = tokio::sync::oneshot::channel();

        match self
            .omnipaxos_sender
            .send(OmniPaxosRequest::Client {
                entry,
                response: tx,
            })
            .await
        {
            Ok(_) => match rx.await {
                Ok(response) => response,
                Err(e) => {
                    let error_msg = format!("Failed to receive response from OmniPaxos: {}", e);
                    error!("[OMNIPAXOS] {}", error_msg);
                    error_msg
                }
            },
            Err(e) => {
                let error_msg = format!("Failed to send request to OmniPaxos: {}", e);
                error!("[OMNIPAXOS] {}", error_msg);
                error_msg
            }
        }
    }

    pub async fn run(node_arc: Arc<RwLock<Node>>) {
        // Start TCP listener loop (already done in run_tcp_loop)
        Self::run_tcp_loop(node_arc.clone());
        Self::run_http_loop(node_arc.clone());
        Self::run_omnipaxos_loop(node_arc.clone()).await;
        info!("[INIT] Node is now running");

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
