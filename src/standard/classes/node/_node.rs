use core::panic;
use omnipaxos::erasure::log_entry::OperationType;
use omnipaxos::messages::sequence_paxos::PaxosMsg;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Duration;

use actix_web::{web, App, HttpServer};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use tokio::time::{interval, sleep};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{error, info, instrument, trace, warn};

use crate::config::_address::Address;
use crate::config::_config::Config;
use crate::config::_constants::{
    BUFFER_SIZE, ELECTION_TICK_TIMEOUT, RECONNECT_INTERVAL, TICK_PERIOD,
};
use crate::config::_pending_set::PendingSet;
use crate::standard::base_libs::network::_messages::send_omnipaxos_message;
use crate::standard::base_libs::{
    _types::{OmniPaxosKV, OmniPaxosMessage},
    network::_messages::receive_omnipaxos_message,
};
use crate::standard::classes::_entry::KeyValue;
use crate::store::_memory_store::KvMemory;
use crate::store::_persistent_store::KvPersistent;
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

    // Key value storage
    pub memory_storage: Arc<KvMemory>,
    pub persistent_storage: Arc<KvPersistent<KeyValue>>,
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
            let storage_config = PersistentStorageConfig::with_path(
                config.nodes[index].rocks_db.transaction_log.clone(),
            );
            let omnipaxos: OmniPaxosKV = omnipaxos_config
                .build(PersistentStorage::new(storage_config))
                .unwrap();
            let persistent_path = config.nodes[index].rocks_db.kvstore.clone();

            Node::new(
                address,
                http_address,
                config.storage.max_payload_size,
                cluster_list,
                index,
                omnipaxos,
                persistent_path,
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
        persistent_path: String,
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

        // Setting up memory storage for key index map
        let memory_store = Arc::new(KvMemory::new().await);
        let memory_store_for_task = memory_store.clone();
        let memory_store_for_struct = memory_store.clone();

        // Setting up persistent storage
        let persistent_store: Arc<KvPersistent<KeyValue>> =
            Arc::new(KvPersistent::new(&persistent_path));
        let persistent_store_for_task = persistent_store.clone();
        let persistent_store_for_struct = persistent_store.clone();

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
            let mut pending_sets: Vec<PendingSet> = Vec::new();
            let mut tick_interval = interval(TICK_PERIOD);

            loop {
                tokio::select! {
                    biased;
                    Some(req) = omnipaxos_receiver.recv() => {
                        let mut send_msgs = Vec::new();
                        match req {
                            OmniPaxosRequest::Network { message } => {
                                trace!("[OMNIPAXOS] Incoming BLE message: from {:?} to {:?} (msg: {:?})", message.get_sender(), message.get_receiver(), message);
                                {
                                    let mut omni = omnipaxos_clone.lock().await;
                                    // Handle AcceptDecide (SET) messages and update memory store
                                    if let OmniPaxosMessage::SequencePaxos(paxos_msg) = &message {
                                        if let PaxosMsg::AcceptDecide(entries) = &paxos_msg.msg {
                                            trace!("[OMNIPAXOS] Received AcceptDecide with {} entries", entries.entries.len());
                                            for entry in &entries.entries {
                                                if entry.op == OperationType::SET {
                                                    info!("[OMNIPAXOS] SET operation received for key: {} with version: {}", entry.key, entry.version);
                                                    memory_store_for_task.set(&entry.key, &entry.value).await;
                                                    // Store the entire versioned entry
                                                    persistent_store_for_task.set(&entry.key, entry.clone());
                                                }
                                            }
                                        }
                                    }

                                    omni.handle_incoming(message);
                                    omni.take_outgoing_messages(&mut send_msgs);
                                }
                            }
                            OmniPaxosRequest::Client { entry, response } => {
                                trace!("[OMNIPAXOS] Client request: {:?}", entry);
                                let response_arc = Arc::new(Mutex::new(Some(response)));
                                // Flag to determine if immediate HTTP response should be sent
                                let mut should_respond = true;
                                let mut result = "Operation failed".to_string();
                                {
                                    let mut omni = omnipaxos_clone.lock().await;

                                    // Handle the operation based on type
                                    match entry.op {
                                        OperationType::SET => {
                                            info!("[OMNIPAXOS] Appending SET entry for key: {}", entry.key);

                                            // Determine the next version for this key before consensus
                                            let next_version = if let Some(existing_entry) = persistent_store_for_task.get(&entry.key) {
                                                existing_entry.version + 1
                                            } else {
                                                1
                                            };

                                            // Create versioned entry for consensus
                                            let versioned_entry = KeyValue::with_version(
                                                entry.key.clone(),
                                                entry.value.clone(),
                                                entry.op,
                                                next_version,
                                            );

                                            // Clone data for pending set tracking
                                            let key_clone = versioned_entry.key.clone();
                                            let value_data_clone = versioned_entry.value.clone();

                                            match omni.append(versioned_entry) {
                                                Ok(_) => {
                                                    // Queue for durability with version info
                                                    pending_sets.push(PendingSet {
                                                        key: key_clone.clone(),
                                                        fragment: value_data_clone.clone(),
                                                        response: response_arc.clone()
                                                    });
                                                    // Defer HTTP response until persistence
                                                    should_respond = false;
                                                },
                                                Err(e) => {
                                                    error!("[OMNIPAXOS] Failed to set value: {:?}", e);
                                                    result = format!("Failed to set value: {:?}", e)
                                                },
                                            }
                                        },
                                        OperationType::GET => {
                                            info!("[OMNIPAXOS] Processing GET for key: {}", entry.key);

                                            // 1. Check from memory store first (fastest)
                                            if let Some(value) = memory_store.get(&entry.key).await {
                                                result = String::from_utf8(value)
                                                    .unwrap_or_else(|_| "Failed to decode value".to_string());
                                            }
                                            // 2. Check persistent store before searching logs
                                            else if let Some(stored_entry) = persistent_store_for_task.get(&entry.key) {
                                                result = String::from_utf8(stored_entry.value)
                                                    .unwrap_or_else(|_| "Failed to decode value".to_string());
                                            }
                                            // 3. Search through decided entries to find the key
                                            else {
                                                let mut key_found = false;
                                                if let Some(log_entries) = omni.read_decided_suffix(0) {
                                                    for log_entry in log_entries.iter().rev() {
                                                        match log_entry {
                                                            LogEntry::Decided(entry_data) => {
                                                                if entry_data.key == entry.key && entry_data.op == OperationType::SET {
                                                                    key_found = true;
                                                                    result = String::from_utf8(entry_data.value.clone())
                                                                        .unwrap_or_else(|_| "Failed to decode value".to_string());
                                                                    break;
                                                                } else if entry_data.key == entry.key && entry_data.op == OperationType::DELETE {
                                                                    key_found = true;
                                                                    result = "Key not found (deleted)".to_string();
                                                                    break;
                                                                }
                                                            },
                                                            _ => continue,
                                                        }
                                                    }
                                                }
                                                if !key_found {
                                                    result = "Key not found".to_string();
                                                }
                                            }
                                        },
                                        OperationType::DELETE => {
                                            info!("[OMNIPAXOS] Appending DELETE entry for key: {}", entry.key);
                                            let key_clone = entry.key.clone();
                                            match omni.append(entry) {
                                                Ok(_) => {
                                                    // Remove from memory and persistent storage
                                                    memory_store_for_task.remove(&key_clone).await;
                                                    persistent_store_for_task.remove(&key_clone);
                                                    result = "Value deleted successfully".to_string();
                                                },
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

                                    // Send the result back to the HTTP handler if not deferred
                                    if should_respond {
                                        if let Some(sender) = response_arc.lock().await.take() {
                                            if let Err(e) = sender.send(result) {
                                                error!("[OMNIPAXOS] Failed to send response: {:?}", e);
                                            }
                                        }
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
                                trace!("[OMNIPAXOS] Sending batch of {} messages to {} at {}", batch.len(), receiver, addr);
                                let _ = send_omnipaxos_message(batch, addr, None).await;
                            }
                        }

                        // Process any pending SETs whose entry is now decided
                        let mut i = 0;
                        while i < pending_sets.len() {
                            let ps = &pending_sets[i];

                            let log_entries = {
                                let omni = omnipaxos_clone.lock().await;
                                omni.read_decided_suffix(0)
                            };

                            if let Some(log_entries) = log_entries {
                                for log_entry in log_entries.iter().rev() {
                                    if let LogEntry::Decided(entry_data) = log_entry {
                                        if entry_data.key == ps.key && entry_data.op == OperationType::SET {
                                            info!("[OMNIPAXOS] Successfully decided SET entry for key: {} with version: {}", ps.key, entry_data.version);
                                            memory_store_for_task.set(&ps.key, &ps.fragment).await;
                                            // Store the entire versioned entry
                                            persistent_store_for_task.set(&ps.key, entry_data.clone());
                                            if let Some(sender) = ps.response.lock().await.take() {
                                                let _ = sender.send("Value set successfully".to_string());
                                            }
                                            pending_sets.remove(i);
                                            break;
                                        }
                                    }
                                }
                            }

                            i += 1;
                        }
                    }
                    _ = tick_interval.tick() => {
                        let mut send_msgs = Vec::new();
                        {
                            let mut omni = omnipaxos_clone.lock().await;
                            trace!("[OMNIPAXOS] Tick");
                            omni.tick();
                            if let Some((leader, _)) = omni.get_current_leader() {
                                trace!("[OMNIPAXOS] Current leader: {}", leader);
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
                                trace!("[OMNIPAXOS] Sending batch of {} messages to {} at {}", batch.len(), receiver, addr);
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
            memory_storage: memory_store_for_struct,
            persistent_storage: persistent_store_for_struct,
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
                            trace!("[TCP] Received message: {:?}", message);
                            let node = node_arc.read().await;
                            let send_result = node
                                .omnipaxos_sender
                                .send(OmniPaxosRequest::Network { message })
                                .await;
                            if let Err(e) = send_result {
                                error!("[TCP] Failed to forward message to OmniPaxos: {}", e);
                            } else {
                                trace!("[TCP] Forwarded message to OmniPaxos event loop");
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
