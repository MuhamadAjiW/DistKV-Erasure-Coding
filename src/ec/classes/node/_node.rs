use core::panic;
use omnipaxos::erasure::ec_service::EntryFragment;
use omnipaxos::erasure::log_entry::OperationType;
use omnipaxos::messages::sequence_paxos::PaxosMsg;
use rand::Rng;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
    BUFFER_SIZE, ELECTION_TICK_TIMEOUT, RECONNECT_INTERVAL, RECONSTRUCTION_WAIT_TIMEOUT,
    TICK_PERIOD,
};
use crate::config::_pending_set::PendingSet;
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
    pub persistent_storage: Arc<KvPersistent<ECKeyValue>>,
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
        let persistent_store: Arc<KvPersistent<ECKeyValue>> =
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
        let jitter_ms = { rand::rng().random_range(0..1000) };

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
                                    if let OmniPaxosECMessage::SequencePaxos(paxos_msg) = &message {
                                        if let PaxosMsg::AcceptDecide(entries) = &paxos_msg.msg {
                                            trace!("[OMNIPAXOS] Received AcceptDecide with {} entries", entries.entries.len());
                                            for entry in &entries.entries {
                                                if entry.op == OperationType::SET {
                                                    info!("[OMNIPAXOS] SET operation received for key: {} with version: {}", entry.key, entry.version);
                                                    memory_store_for_task.remove(&entry.key).await;
                                                    // Store the entire versioned entry
                                                    persistent_store_for_task.set(&entry.key, &entry);
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
                                            let versioned_entry = ECKeyValue::with_version(
                                                entry.key.clone(),
                                                entry.fragment.clone(),
                                                entry.op,
                                                next_version,
                                            );

                                            // Clone data for pending set tracking
                                            let key_clone = versioned_entry.key.clone();
                                            let fragment_data_clone = versioned_entry.fragment.data.clone();

                                            match omni.append(versioned_entry) {
                                                Ok(_) => {
                                                    // Queue for durability with version info
                                                    pending_sets.push(PendingSet {
                                                        key: key_clone.clone(),
                                                        fragment: fragment_data_clone.clone(),
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

                                            // For benchmark purposes we do not use memory store so it's always reconstructed

                                            // Check from memory store first
                                            // 1. Check from memory store first (fast path)
                                            // if let Some(value) = memory_store.get(&entry.key).await {
                                            if let Some(value) = memory_store.get("__DISABLED__").await {
                                                if let Ok(value_str) = String::from_utf8(value) {
                                                    result = value_str;
                                                } else {
                                                    result = "Failed to decode value".to_string();
                                                }
                                            }

                                            // 2. Check persistent storage and collect fragments from peers
                                            else {
                                                let mut version_fragments: HashMap<u64, Vec<EntryFragment>> = HashMap::new();
                                                let ec_service = omni.ec_service();
                                                let k = ec_service.data_shards;

                                                // Add local fragment if we have one in persistent storage
                                                if let Some(persisted_entry) = persistent_store.get(&entry.key) {
                                                    version_fragments.entry(persisted_entry.version).or_insert_with(Vec::new).push(EntryFragment {
                                                        data: persisted_entry.fragment.data.clone(),
                                                        ..Default::default()
                                                    });
                                                }

                                                // Collect additional fragments from peers
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
                                                                match bincode::deserialize::<ECKeyValue>(&buffer) {
                                                                    Ok(entry) => Some((entry.version, entry.fragment)),
                                                                    Err(_) => None,
                                                                }
                                                            }
                                                            Err(_) => None,
                                                        }
                                                    }));
                                                }

                                                // Wait for responses and collect fragments by version
                                                let timeout = tokio::time::Duration::from_millis(RECONSTRUCTION_WAIT_TIMEOUT);
                                                for handle in handles {
                                                    match tokio::time::timeout(timeout, handle).await {
                                                        Ok(Ok(Some((version, fragment)))) => {
                                                            let fragments = version_fragments.entry(version).or_insert_with(Vec::new);
                                                            // Avoid duplicate fragments
                                                            if !fragments.iter().any(|f| f.data == fragment.data) {
                                                                fragments.push(fragment);
                                                            }
                                                        }
                                                        Ok(_) => {}, // Task finished but no fragment
                                                        Err(_) => {}, // Timeout on individual request
                                                    }
                                                }

                                                // Find the version with enough fragments (k shards minimum)
                                                let best_version = version_fragments.iter()
                                                    .filter(|(_, fragments)| fragments.len() >= k)
                                                    .max_by_key(|(version, fragments)| (fragments.len(), *version))
                                                    .map(|(version, _)| *version);

                                                if let Some(best_version) = best_version {
                                                    if let Some(fragments) = version_fragments.get(&best_version) {
                                                        // Attempt to reconstruct value
                                                        match ec_service.decode(fragments) {
                                                            Ok(data) => {
                                                                let data_clone = data.clone();
                                                                if let Ok(value_str) = String::from_utf8(data) {
                                                                    result = value_str;
                                                                    memory_store_for_task.set(&entry.key, &data_clone).await;
                                                                } else {
                                                                    result = "Failed to decode value".to_string();
                                                                }
                                                            }
                                                            Err(e) => {
                                                                result = format!("Failed to reconstruct value: {:?}", e);
                                                            }
                                                        }
                                                    }
                                                }

                                                // If no version had enough fragments, check the log as fallback
                                                else if let Some(log_entries) = omni.read_decided_suffix(0) {
                                                    let mut key_found = false;
                                                    for log_entry in log_entries.iter().rev() {
                                                        match log_entry {
                                                            LogEntry::Decided(entry_data) => {
                                                                if entry_data.key == entry.key && entry_data.op == OperationType::SET {
                                                                    // Found the most recent SET for this key, add its fragment to collection
                                                                    key_found = true;
                                                                    let log_version = entry_data.version;
                                                                    version_fragments.entry(log_version).or_insert_with(Vec::new).push(EntryFragment {
                                                                        data: entry_data.fragment.data.clone(),
                                                                        ..Default::default()
                                                                    });

                                                                    // Try to find version with enough fragments again
                                                                    if let Some(fragments) = version_fragments.get(&log_version) {
                                                                        if fragments.len() >= k {
                                                                            match ec_service.decode(fragments) {
                                                                                Ok(data) => {
                                                                                    let data_clone = data.clone();
                                                                                    if let Ok(value_str) = String::from_utf8(data) {
                                                                                        result = value_str;
                                                                                        memory_store_for_task.set(&entry.key, &data_clone).await;
                                                                                    } else {
                                                                                        result = "Failed to decode value".to_string();
                                                                                    }
                                                                                }
                                                                                Err(e) => {
                                                                                    result = format!("Failed to reconstruct value: {:?}", e);
                                                                                }
                                                                            }
                                                                            break;
                                                                        }
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

                                                    if !key_found {
                                                        result = "Key not found".to_string();
                                                    } else if result == "Operation failed" {
                                                        result = "Not enough fragments to reconstruct value".to_string();
                                                    }
                                                } else {
                                                    result = "Key not found".to_string();
                                                }
                                            }
                                        },
                                        OperationType::RECONSTRUCT => {
                                            info!("[OMNIPAXOS] Processing RECONSTRUCT for key: {}", entry.key);

                                            // Search through decided entries to find the key
                                            // First, check persistent storage for the fragment
                                            let mut key_found = false;
                                            if let Some(persisted_entry) = persistent_store.get(&entry.key) {
                                                key_found = true;
                                                if let Ok(value) = String::from_utf8(persisted_entry.fragment.data.clone()) {
                                                    result = value;
                                                } else {
                                                    result = "Failed to decode value".to_string();
                                                }
                                            } else if let Some(log_entries) = omni.read_decided_suffix(0) {
                                                for log_entry in log_entries.iter().rev() {
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
                        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosECMessage>> = HashMap::new();
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
                                            persistent_store_for_task.set(&ps.key, &entry_data);
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
                        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosECMessage>> = HashMap::new();
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
                } else if let Some(recon_msg) = deserialize_reconstruct_message(&buffer) {
                    match recon_msg {
                        ReconstructMessage::Request { key } => {
                            info!("[TCP] Received RECONSTRUCT request for key {}", key);
                            // Find fragment for key
                            let node = node_arc.read().await;
                            let mut found: Option<ECKeyValue> = None;

                            // 1. Try persistent storage first (fast path)
                            if let Some(entry) = node.persistent_storage.get(&key) {
                                found = Some(entry.clone());
                            } else {
                                // 2. Fallback: Search through decided log entries for the key
                                let omni = node.omnipaxos.lock().await;
                                if let Some(log_entries) = omni.read_decided_suffix(0) {
                                    for log_entry in log_entries.iter().rev() {
                                        if let LogEntry::Decided(entry_data) = log_entry {
                                            if entry_data.key == key
                                                && entry_data.op == OperationType::SET
                                            {
                                                found = Some(entry_data.clone());
                                                break;
                                            }
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
