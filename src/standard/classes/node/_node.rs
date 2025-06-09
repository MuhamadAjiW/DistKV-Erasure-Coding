use core::panic;
use omnipaxos::erasure::log_entry::OperationType;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Duration;

use actix_web::{web, App, HttpServer};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use tokio::time::{interval, sleep};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{debug, error, info, instrument, warn};

use crate::config::_address::Address;
use crate::config::_config::Config;
use crate::config::_constants::{
    BUFFER_SIZE, ELECTION_TICK_TIMEOUT, RECONNECT_INTERVAL, TICK_PERIOD,
};
use crate::standard::base_libs::network::_messages::send_omnipaxos_message;
use crate::standard::base_libs::{
    _types::{OmniPaxosKV, OmniPaxosMessage},
    network::_messages::receive_omnipaxos_message,
};
use crate::standard::classes::_entry::KeyValue;
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
use std::collections::HashMap;

#[derive(Debug)]
pub enum OmniPaxosRequest {
    Client {
        entry: KeyValue,
        response: oneshot::Sender<String>,
    },
    Network {
        message: OmniPaxosMessage,
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
    pub omnipaxos: Arc<Mutex<OmniPaxosKV>>,
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
            let cluster_config = ClusterConfig {
                configuration_id: 1,
                nodes,
                flexible_quorum: None,
            };
            let server_config = ServerConfig {
                pid: (index + 1) as u64,
                election_tick_timeout: ELECTION_TICK_TIMEOUT,
                ..Default::default()
            };
            let omnipaxos_config = OmniPaxosConfig {
                cluster_config,
                server_config,
            };
            let storage_config =
                PersistentStorageConfig::with_path(config.nodes[index].rocks_db.path.clone());
            let omnipaxos: OmniPaxosKV = omnipaxos_config
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
        omnipaxos: OmniPaxosKV,
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
        let (omnipaxos_sender, mut omnipaxos_receiver) = mpsc::channel(BUFFER_SIZE);
        let omnipaxos_arc = Arc::new(tokio::sync::Mutex::new(omnipaxos));

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

        // Main OmniPaxos event loop
        tokio::spawn(async move {
            info!("[OMNIPAXOS] Initial jitter: {}ms", jitter_ms);
            sleep(Duration::from_millis(jitter_ms)).await;
            let mut tick_interval = interval(TICK_PERIOD);
            loop {
                tokio::select! {
                    biased;
                    Some(req) = omnipaxos_receiver.recv() => {
                        let mut send_msgs = Vec::new();
                        match req {
                            OmniPaxosRequest::Network { message } => {
                                debug!("[OMNIPAXOS] Incoming BLE message: from {:?} to {:?} (msg: {:?})", message.get_sender(), message.get_receiver(), message);
                                {
                                    let mut omni = omnipaxos_clone.lock().await;
                                    omni.handle_incoming(message);
                                    omni.take_outgoing_messages(&mut send_msgs);
                                }
                            }
                            OmniPaxosRequest::Client { entry, response } => {
                                debug!("[OMNIPAXOS] Client request: {:?}", entry);
                                let mut result = "Operation failed".to_string();
                                {
                                    let mut omni = omnipaxos_clone.lock().await;

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
                                                                let value = &entry_data.value;
                                                                if let Ok(value) = String::from_utf8(value.clone()) {
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
                        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosMessage>> = HashMap::new();
                        for msg in send_msgs.drain(..) {
                            let receiver = msg.get_receiver();
                            peer_batches.entry(receiver).or_default().push(msg);
                        }
                        for (receiver, batch) in peer_batches {
                            if let Some(addr) = peer_addresses.get(&receiver) {
                                debug!("[OMNIPAXOS] Sending batch of {} messages to {} at {}", batch.len(), receiver, addr);
                                let _ = send_omnipaxos_message(batch, addr, None).await;
                            }
                        }
                    }
                    _ = tick_interval.tick() => {
                        let mut send_msgs = Vec::new();
                        {
                            let mut omni = omnipaxos_clone.lock().await;
                            debug!("[OMNIPAXOS] Tick");
                            omni.tick();
                            if let Some((leader, _)) = omni.get_current_leader() {
                                debug!("[OMNIPAXOS] Current leader: {}", leader);
                            }
                            omni.take_outgoing_messages(&mut send_msgs);
                        }

                        // Batch outgoing messages per peer
                        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosMessage>> = HashMap::new();
                        for msg in send_msgs.drain(..) {
                            let receiver = msg.get_receiver();
                            peer_batches.entry(receiver).or_default().push(msg);
                        }
                        for (receiver, batch) in peer_batches {
                            if let Some(addr) = peer_addresses.get(&receiver) {
                                debug!("[OMNIPAXOS] Sending batch of {} messages to {} at {}", batch.len(), receiver, addr);
                                let _ = send_omnipaxos_message(batch, addr, None).await;
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

    pub async fn run(node_arc: Arc<RwLock<Node>>) {
        Self::run_tcp_loop(node_arc.clone());
        Self::run_http_loop(node_arc.clone());
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
