use core::panic;
use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use omnipaxos_storage::persistent_storage::{PersistentStorage, PersistentStorageConfig};
use tokio::sync::{mpsc, oneshot};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{info, instrument, warn};

use crate::base_libs::_ec::ECKeyValue;
use crate::base_libs::network::_messages::receive_omnipaxos_message;
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
}

impl Node {
    pub async fn from_config(address: Address, config_path: &str) -> Self {
        let config = Config::get_config(config_path).await;
        let index = Config::get_node_index(&config, &address);
        let cluster_list: Vec<String> = Config::get_node_addresses(&config)
            .iter()
            .map(|a| a.to_string())
            .collect();
        if let Some(index) = index {
            let node_conf = &config.nodes[index];
            let http_address = Address::new(&node_conf.ip, node_conf.http_port);
            let cluster_config = ClusterConfigEC {
                configuration_id: 1,
                nodes: (0..config.nodes.len()).map(|i| (i + 1) as u64).collect(),
                flexible_quorum: None,
            };
            let server_config = ServerConfigEC {
                pid: (index + 1) as u64,
                election_tick_timeout: 5,
                resend_message_tick_timeout: 5,
                buffer_size: 10000,
                batch_size: 1,
                flush_batch_tick_timeout: 5, // default value, adjust as needed
                leader_priority: 0,          // default value, adjust as needed
                erasure_coding_service: ECService::new(
                    config.storage.shard_count,
                    config.storage.parity_count,
                )
                .unwrap(),
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
                Ok(sock) => break Arc::new(sock),
                Err(e) => {
                    warn!("[INIT] Failed to bind TCP: {}. Retrying...", e);
                    tokio::time::sleep(RECONNECT_INTERVAL).await;
                }
            }
        };
        let (omnipaxos_sender, mut omnipaxos_receiver) = mpsc::channel(128);
        tokio::spawn(async move {
            let mut omnipaxos = omnipaxos;
            use crate::base_libs::_ec::ECKeyValue;
            use omnipaxos::erasure::log_entry::OperationType;
            use omnipaxos::util::LogEntry;
            while let Some(req) = omnipaxos_receiver.recv().await {
                match req {
                    OmniPaxosRequest::Client { entry, response } => {
                        let op_type = entry.op.clone();
                        let key = entry.key.clone();
                        let _ = omnipaxos.append(entry);
                        let result = match op_type {
                            OperationType::GET => {
                                let mut found = None;
                                for _ in 0..100 {
                                    if let Some(entries) = omnipaxos.read_decided_suffix(0) {
                                        for entry in entries {
                                            if let LogEntry::Decided(ECKeyValue {
                                                key: k,
                                                fragment,
                                                ..
                                            }) = entry
                                            {
                                                if k == key {
                                                    found = Some(
                                                        String::from_utf8_lossy(&fragment.data)
                                                            .to_string(),
                                                    );
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if found.is_some() {
                                        break;
                                    }
                                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                }
                                found.unwrap_or_else(|| "Not found".to_string())
                            }
                            OperationType::SET => {
                                let mut committed = false;
                                for _ in 0..100 {
                                    if let Some(entries) = omnipaxos.read_decided_suffix(0) {
                                        for entry in entries {
                                            if let LogEntry::Decided(ECKeyValue {
                                                key: k, ..
                                            }) = entry
                                            {
                                                if k == key {
                                                    committed = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    if committed {
                                        break;
                                    }
                                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                }
                                if committed {
                                    "OK".to_string()
                                } else {
                                    "Timeout".to_string()
                                }
                            }
                            OperationType::DELETE => {
                                let mut deleted = false;
                                for _ in 0..100 {
                                    if let Some(entries) = omnipaxos.read_decided_suffix(0) {
                                        if !entries.iter().any(|entry| {
                                            if let LogEntry::Decided(ECKeyValue {
                                                key: k, ..
                                            }) = entry
                                            {
                                                k == &key
                                            } else {
                                                false
                                            }
                                        }) {
                                            deleted = true;
                                            break;
                                        }
                                    }
                                    if deleted {
                                        break;
                                    }
                                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                }
                                if deleted {
                                    "Deleted".to_string()
                                } else {
                                    "Timeout".to_string()
                                }
                            }
                            _ => "Unsupported operation".to_string(),
                        };
                        let _ = response.send(result);
                    }
                    OmniPaxosRequest::Network { message } => {
                        omnipaxos.handle_incoming(message);
                    }
                }
            }
        });
        Node {
            running: true,
            http_address,
            http_max_payload,
            address,
            socket,
            cluster_list: Arc::new(RwLock::new(cluster_list)),
            cluster_index,
            omnipaxos_sender,
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
            loop {
                match receive_omnipaxos_message(&socket).await {
                    Ok((_stream, message)) => {
                        let node = node_arc.read().await;
                        let _ = node
                            .omnipaxos_sender
                            .send(OmniPaxosRequest::Network { message })
                            .await;
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
        let http_addr = {
            let node = node_arc.blocking_read();
            node.http_address.to_string()
        };
        let data = web::Data::new(node_arc.clone());
        tokio::spawn(async move {
            HttpServer::new(move || {
                App::new()
                    .app_data(data.clone())
                    .route("/health", web::get().to(Self::http_healthcheck))
                    .route("/get", web::post().to(Self::http_get))
                    .route("/put", web::post().to(Self::http_put))
                    .route("/delete", web::post().to(Self::http_delete))
            })
            .bind(http_addr)
            .unwrap()
            .run()
            .await
            .unwrap();
        });
    }

    pub async fn run(node_arc: Arc<RwLock<Node>>) {
        Self::run_tcp_loop(node_arc.clone());
        Self::run_http_loop(node_arc.clone());

        info!("[INIT] Node is now running");

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
