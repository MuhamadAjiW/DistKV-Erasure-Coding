use core::panic;
use omnipaxos::erasure::ec_service::EntryFragment;
use omnipaxos::erasure::log_entry::OperationType;
use rand::Rng;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
use crate::ec::base_libs::_types::{OmniPaxosECKV, OmniPaxosECMessage};
use crate::ec::base_libs::network::_messages::send_omnipaxos_message;
use crate::ec::base_libs::network::_reconstruct::{
    deserialize_reconstruct_message, serialize_reconstruct_message, ReconstructMessage,
};
use crate::ec::classes::_entry::ECKeyValue;
use crate::store::_memory_store::KvMemory;
use crate::store::_persistent_store::KvPersistent;
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{
    erasure::ec_service::ECService, ClusterConfigEC, OmniPaxosECConfig, ServerConfigEC,
};
use std::collections::HashMap;
use tokio::net::TcpStream;

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

    // Key value storage
    pub memory_storage: Arc<KvMemory>,
    pub persistent_storage: Arc<KvPersistent>,
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
            let storage_config = PersistentStorageConfig::with_path(
                config.nodes[index].rocks_db.transaction_log.clone(),
            );
            let omnipaxos: OmniPaxosECKV = omnipaxos_config
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
        omnipaxos: OmniPaxosECKV,
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
        let persistent_store = Arc::new(KvPersistent::new(&persistent_path));
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
        let jitter_ms = { rand::rng().random_range(0..1000) };

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
                                            // Clone key and fragment data before moving entry
                                            let key_clone = entry.key.clone();
                                            let fragment_data_clone = entry.fragment.data.clone();
                                            match omni.append(entry) {
                                                Ok(_) => {
                                                    result = "Value set successfully".to_string();
                                                    memory_store_for_task.set(&key_clone, &fragment_data_clone).await;
                                                    // Persist the decided erasure-coded entry to persistent storage
                                                    // Find the most recent decided SET entry for this key
                                                    if let Some(log_entries) = omni.read_decided_suffix(0) {
                                                        for log_entry in log_entries.iter().rev() {
                                                            if let LogEntry::Decided(entry_data) = log_entry {
                                                                if entry_data.key == key_clone && entry_data.op == OperationType::SET {
                                                                    // Persist the decided erasure-coded fragment
                                                                    persistent_store_for_task.set(&key_clone, &entry_data.fragment.data);
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                Err(e) => {
                                                    error!("[OMNIPAXOS] Failed to set value: {:?}", e);
                                                    result = format!("Failed to set value: {:?}", e)
                                                },
                                            }
                                        },

                                        OperationType::GET => {
                                            info!("[OMNIPAXOS] Processing GET for key: {}", entry.key);

                                            // Check from memory store first
                                            if let Some(value) = memory_store.get(&entry.key).await {
                                                if let Ok(value_str) = String::from_utf8(value) {
                                                    result = value_str;
                                                } else {
                                                    result = "Failed to decode value".to_string();
                                                }
                                            } else {
                                                // Search through decided entries to find the key
                                                let mut key_found = false;
                                                if let Some(log_entries) = omni.read_decided_suffix(0) {
                                                    for log_entry in log_entries.iter().rev() {
                                                        match log_entry {
                                                            LogEntry::Decided(entry_data) => {
                                                                if entry_data.key == entry.key && entry_data.op == OperationType::SET {
                                                                    // Found the most recent SET for this key
                                                                    key_found = true;
                                                                    let ec_service = omni.ec_service();
                                                                    let k = ec_service.data_shards;

                                                                    // Use peer_addresses for distributed fragment collection via TCP
                                                                    let mut fragments: Vec<EntryFragment> = Vec::new();
                                                                    let mut handles = Vec::new();
                                                                    let key = entry.key.clone();
                                                                    info!("[EC-RECONSTRUCT] Collecting fragments for key {} from {} peers (TCP)", key, peer_addresses.len());
                                                                    for (_peer_id, peer_addr) in peer_addresses.iter() {
                                                                        let key = key.clone();
                                                                        let peer_addr = peer_addr.clone();
                                                                        handles.push(tokio::spawn(async move {
                                                                            let msg = ReconstructMessage::Request { key };
                                                                            let bytes = serialize_reconstruct_message(&msg);
                                                                            match TcpStream::connect(&peer_addr).await {
                                                                                Ok(mut stream) => {
                                                                                    let _ = stream.write_all(&(bytes.len() as u32).to_be_bytes()).await;
                                                                                    let _ = stream.write_all(&bytes).await;
                                                                                    let mut len_buf = [0u8; 4];
                                                                                    if stream.read_exact(&mut len_buf).await.is_err() {
                                                                                        return None;
                                                                                    }
                                                                                    let len = u32::from_be_bytes(len_buf) as usize;
                                                                                    if len == 0 { return None; }
                                                                                    let mut buffer = vec![0; len];
                                                                                    if stream.read_exact(&mut buffer).await.is_err() {
                                                                                        return None;
                                                                                    }
                                                                                    match bincode::deserialize::<EntryFragment>(&buffer) {
                                                                                        Ok(fragment) => Some(fragment),
                                                                                        Err(_) => None,
                                                                                    }
                                                                                }
                                                                                Err(_) => None,
                                                                            }
                                                                        }));
                                                                    }
                                                                    // Wait for all responses, but with a timeout
                                                                    let timeout = tokio::time::Duration::from_millis(1000);
                                                                    let mut timed_out = false;
                                                                    for handle in handles {
                                                                        match tokio::time::timeout(timeout, handle).await {
                                                                            Ok(Ok(Some(fragment))) => {
                                                                                fragments.push(fragment);
                                                                                if fragments.len() >= k {
                                                                                    break;
                                                                                }
                                                                            }
                                                                            Ok(_) => {}, // Task finished but no fragment
                                                                            Err(_) => { timed_out = true; }, // Timeout
                                                                        }
                                                                    }
                                                                    if fragments.len() >= k {
                                                                        // Attempt to reconstruct value
                                                                        match ec_service.decode(&fragments) {
                                                                            Ok(data) => {
                                                                                if let Ok(value_str) = String::from_utf8(data) {
                                                                                    result = value_str;
                                                                                } else {
                                                                                    result = "Failed to decode value".to_string();
                                                                                }
                                                                            }
                                                                            Err(e) => {
                                                                                result = format!("Failed to reconstruct value: {:?}", e);
                                                                            }
                                                                        }
                                                                    } else if timed_out {
                                                                        result = "Timeout while collecting fragments from peers".to_string();
                                                                    } else {
                                                                        result = "Not enough fragments to reconstruct value".to_string();
                                                                    }
                                                                    break;
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
                                            }
                                        },
                                        OperationType::RECONSTRUCT => {
                                            info!("[OMNIPAXOS] Processing RECONSTRUCT for key: {}", entry.key);

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
                        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosECMessage>> = HashMap::new();
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
                let (mut stream, _src) = match socket.accept().await {
                    Ok(pair) => pair,
                    Err(e) => {
                        warn!("[TCP] Error accepting connection: {}", e);
                        continue;
                    }
                };
                // Read length prefix
                let mut len_buf = [0u8; 4];
                if let Err(e) = stream.read_exact(&mut len_buf).await {
                    warn!("[TCP] Error reading length prefix: {}", e);
                    continue;
                }
                let len = u32::from_be_bytes(len_buf) as usize;
                let mut buffer = vec![0; len];
                if let Err(e) = stream.read_exact(&mut buffer).await {
                    warn!("[TCP] Error reading message body: {}", e);
                    continue;
                }
                // Try to deserialize as OmniPaxosECMessage batch first
                let try_omnipaxos: Result<Vec<OmniPaxosECMessage>, _> =
                    bincode::deserialize(&buffer);
                if let Ok(messages) = try_omnipaxos {
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
                } else if let Some(recon_msg) = deserialize_reconstruct_message(&buffer) {
                    match recon_msg {
                        ReconstructMessage::Request { key } => {
                            info!("[TCP] Received RECONSTRUCT request for key {}", key);
                            // Find fragment for key
                            let node = node_arc.read().await;
                            let omni = node.omnipaxos.lock().await;
                            let mut found = None;
                            if let Some(log_entries) = omni.read_decided_suffix(0) {
                                for log_entry in log_entries.iter().rev() {
                                    if let LogEntry::Decided(entry_data) = log_entry {
                                        if entry_data.key == key
                                            && entry_data.op == OperationType::SET
                                        {
                                            found = Some(entry_data.fragment.clone());
                                            break;
                                        }
                                    }
                                }
                            }
                            // Send raw fragment bytes directly (no base64, just bincode)
                            if let Some(fragment) = found {
                                let fragment_bytes = bincode::serialize(&fragment).unwrap();
                                let fragment_len = (fragment_bytes.len() as u32).to_be_bytes();
                                let _ = stream.write_all(&fragment_len).await;
                                let _ = stream.write_all(&fragment_bytes).await;
                            } else {
                                // Send zero-length to indicate not found
                                let _ = stream.write_all(&0u32.to_be_bytes()).await;
                            }
                        }
                        _ => {}
                    }
                } else {
                    warn!("[TCP] Received unknown or invalid message format");
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
