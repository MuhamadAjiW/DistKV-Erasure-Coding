use core::panic;
use std::sync::Arc;

use actix_web::{web, App, HttpServer};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{error, info, instrument, warn};

use crate::{
    base_libs::_operation::Operation,
    base_libs::network::_address::Address,
    classes::{config::_config::Config, ec::_ec::ECService},
    config::_constants::{RECONNECT_INTERVAL, STOP_INTERVAL},
};
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;

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

    // Erasure coding (optional, for future use)
    pub ec: Arc<ECService>,

    // Omnipaxos
    pub omnipaxos: Arc<RwLock<OmniPaxos<Operation, MemoryStorage<Operation>>>>,
}

impl Node {
    pub async fn from_config(address: Address, config_path: &str) -> Self {
        let config = Config::get_config(config_path).await;
        let index = Config::get_node_index(&config, &address);
        let cluster_list: Vec<String> = Config::get_node_addresses(&config)
            .into_iter()
            .map(|addr| addr.to_string())
            .collect();
        if let Some(index) = index {
            info!("[INIT] Storage Config: {:?}", config.storage);
            info!("[INIT] Node Config: {:?}", config.nodes[index]);
            info!("[INIT] Index: {}", index);

            let ec = Arc::new(ECService::new(
                config.storage.erasure_coding,
                config.storage.shard_count,
                config.storage.parity_count,
            ));

            // Omnipaxos config
            let omnipaxos_config = OmniPaxosConfig {
                cluster_config: ClusterConfig {
                    configuration_id: 1,
                    nodes: (0..cluster_list.len() as u64).collect(),
                    ..Default::default()
                },
                server_config: ServerConfig {
                    pid: index as u64,
                    ..Default::default()
                },
            };
            let omnipaxos = Arc::new(RwLock::new(
                omnipaxos_config
                    .build(MemoryStorage::<Operation>::default())
                    .unwrap(),
            ));

            return Node::new(
                address,
                Address::new(&config.nodes[index].ip, config.nodes[index].http_port),
                config.storage.max_payload_size,
                cluster_list,
                index,
                ec,
                omnipaxos,
            )
            .await;
        }
        panic!("Error: Failed to get node config");
    }

    pub async fn new(
        address: Address,
        http_address: Address,
        http_max_payload: usize,
        cluster_list: Vec<String>,
        cluster_index: usize,
        ec: Arc<ECService>,
        omnipaxos: Arc<RwLock<OmniPaxos<Operation, MemoryStorage<Operation>>>>,
    ) -> Self {
        info!("[INIT] binding address: {}", address);
        let socket = loop {
            match TcpListener::bind(address.to_string()).await {
                Ok(listener) => break Arc::new(listener),
                Err(e) => {
                    error!(
                        "[INIT] Failed to bind to {}: {}. Retrying in 1s...",
                        address, e
                    );
                    tokio::time::sleep(RECONNECT_INTERVAL).await;
                }
            }
        };

        Node {
            running: true,
            http_address,
            http_max_payload,
            address,
            socket,
            cluster_list: Arc::new(RwLock::new(cluster_list)),
            cluster_index,
            ec,
            omnipaxos,
        }
    }

    // Information logging
    pub async fn print_info(&self) {
        info!("-------------------------------------");
        info!("[INFO] Node info:");
        info!("Address: {}", &self.address.to_string());
        info!("Cluster list: {:?}", &self.cluster_list.read().await);
        info!("Cluster index: {}", &self.cluster_index);
        info!("\nErasure coding configuration:");
        info!("Shard count: {}", &self.ec.data_shard_count);
        info!("Parity count: {}", &self.ec.parity_shard_count);
        info!("-------------------------------------");
    }

    pub fn run_timer_task(node_arc: Arc<RwLock<Node>>) {
        // Omnipaxos requires periodic ticking for timeouts and leader election
        tokio::spawn(async move {
            loop {
                {
                    let node = node_arc.read().await;
                    if !node.running {
                        break;
                    }
                    // Call Omnipaxos tick periodically
                    let mut omnipaxos = node.omnipaxos.write().await;
                    omnipaxos.tick();
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        });
    }

    #[instrument(level = "debug", skip_all)]
    pub fn run_tcp_loop(node_arc: Arc<RwLock<Node>>) {
        info!("[INIT] Starting TCP loop...");
        tokio::spawn(async move {
            loop {
                let socket = {
                    let node = node_arc.read().await;
                    if !node.running {
                        info!("[TIMEOUT] Running is false, stopping");
                        break;
                    }
                    node.socket.clone()
                };
                let (_stream, message) =
                    match crate::base_libs::network::_messages::receive_omnipaxos_message(&socket)
                        .await
                    {
                        Ok(msg) => msg,
                        Err(_) => {
                            error!("[ERROR] Received bad message on socket, continuing...");
                            continue;
                        }
                    };
                // Forward to Omnipaxos
                {
                    let node = node_arc.read().await;
                    let mut omnipaxos = node.omnipaxos.write().await;
                    omnipaxos.handle_incoming(message);
                }
                // Send any outgoing Omnipaxos messages
                {
                    let node = node_arc.read().await;
                    node.send_omnipaxos_messages().await;
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
                            .route("/", web::get().to(Node::http_healthcheck))
                            .route("/kv/range", web::post().to(Node::http_get))
                            .route("/kv/put", web::post().to(Node::http_put))
                            .route("/kv/deleterange", web::post().to(Node::http_delete))
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

    pub async fn initialize(node_arc: &Arc<RwLock<Node>>) {
        let mut node = node_arc.write().await;
        node.running = true;
        node.print_info().await;
    }

    pub async fn run(node_arc: Arc<RwLock<Node>>) {
        info!("[INIT] Running node...");

        Node::initialize(&node_arc).await;

        Node::run_timer_task(node_arc.clone());
        Node::run_tcp_loop(node_arc.clone());
        Node::run_http_loop(node_arc.clone());

        info!("[INIT] Node is now running");

        tokio::signal::ctrl_c()
            .await
            .expect("[ERROR] Failed to listen for Ctrl+C signal");

        info!("[INIT] Ctrl+C signal received, stopping node...");

        {
            let mut node = node_arc.write().await;
            node.running = false;
            info!("[INIT] Node is stopping...");

            tokio::time::sleep(STOP_INTERVAL).await;
        }
    }
}
