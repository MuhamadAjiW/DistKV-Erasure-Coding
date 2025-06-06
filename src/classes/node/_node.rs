use core::panic;
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use actix_web::{web, App, HttpServer};
use tokio::{net::TcpListener, sync::RwLock, time::Instant};
use tracing::{error, info, instrument, warn};

use crate::{
    base_libs::{
        _paxos_types::PaxosMessage,
        network::{_address::Address, _messages::listen},
    },
    classes::{config::_config::Config, ec::_ec::ECService, kvstore::_kvstore::KvStoreModule},
    config::_constants::{RECONNECT_INTERVAL, STOP_INTERVAL},
};

use super::paxos::_paxos_state::PaxosState;

pub struct Node {
    // Base attributes
    pub running: bool,

    // HTTP Interface
    pub http_address: Address,
    pub http_max_payload: usize,

    // Internode communication
    pub address: Address,
    pub socket: Arc<TcpListener>,

    // Paxos related attributes
    pub leader_address: Option<Address>,
    pub cluster_list: Arc<RwLock<Vec<String>>>,
    pub cluster_index: usize,
    pub state: PaxosState,
    pub epoch: AtomicU64,
    pub request_id: AtomicU64,
    pub commit_id: AtomicU64,
    pub last_heartbeat: Arc<RwLock<Instant>>,
    pub timeout_duration: Arc<Duration>,
    pub vote_count: AtomicUsize,

    // Application attributes
    pub store: KvStoreModule,
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

            let db_path = &config.nodes[index].rocks_db.path;
            let tlog_path = &config.nodes[index].transaction_log;

            let ec = Arc::new(ECService::new(
                config.storage.erasure_coding,
                config.storage.shard_count,
                config.storage.parity_count,
            ));
            let store = KvStoreModule::new(db_path.as_str(), tlog_path.as_str(), ec).await;

            return Node::new(
                address,
                Address::new(&config.nodes[index].ip, config.nodes[index].http_port),
                config.storage.max_payload_size,
                cluster_list,
                index,
                store,
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
        store: KvStoreModule,
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

        let timeout_duration = Arc::new(Duration::from_millis(
            1000 + (rand::random::<u64>() % 20000),
        ));

        let node = Node {
            http_address,
            http_max_payload,
            address,
            socket,
            running: false,
            leader_address: None,
            cluster_list: Arc::new(RwLock::new(cluster_list)),
            cluster_index,
            state: PaxosState::Follower,
            epoch: std::sync::atomic::AtomicU64::new(0),
            request_id: std::sync::atomic::AtomicU64::new(0),
            commit_id: std::sync::atomic::AtomicU64::new(0),
            last_heartbeat: Arc::new(RwLock::new(Instant::now())),
            timeout_duration,
            vote_count: AtomicUsize::new(0),
            store,
        };

        node
    }

    // Information logging
    pub async fn print_info(&self) {
        info!("-------------------------------------");
        info!("[INFO] Node info:");
        info!("Address: {}", &self.address.to_string());
        info!("State: {}", &self.state.to_string());
        info!("Erasure Coding: {}", &self.store.ec.active.to_string());
        if let Some(leader) = &self.leader_address {
            info!("Leader: {}", &leader.to_string());
        } else {
            info!("Leader: None");
        }
        info!("Cluster list: {:?}", &self.cluster_list.read().await);
        info!("Cluster index: {}", &self.cluster_index);
        info!("Epoch: {:?}", self.epoch.load(Ordering::SeqCst));
        info!("Request ID: {:?}", self.request_id.load(Ordering::SeqCst));
        info!("Commit ID: {:?}", self.commit_id.load(Ordering::SeqCst));
        info!("Last heartbeat: {:?}", &self.last_heartbeat.read().await);
        info!("Timeout duration: {:?}", &self.timeout_duration);

        info!("\nErasure coding configuration:");
        info!("Shard count: {}", &self.store.ec.data_shard_count);
        info!("Parity count: {}", &self.store.ec.parity_shard_count);
        info!("-------------------------------------");
    }

    pub fn run_timer_task(node_arc: Arc<RwLock<Node>>) {
        info!("[INIT] Starting timer task...");
        let node_clone = Arc::clone(&node_arc);

        tokio::spawn(async move {
            let (last_heartbeat, timeout_duration) = {
                let node = node_arc.read().await;
                (
                    Arc::clone(&node.last_heartbeat),
                    Arc::clone(&node.timeout_duration),
                )
            };
            let timeout = *timeout_duration;
            let heartbeat_delay = timeout / 3;

            info!("[TIMEOUT] Spawning task to check for timeout");

            loop {
                let state = {
                    let node = node_clone.read().await;
                    node.state
                };

                match state {
                    PaxosState::Leader => {
                        tokio::time::sleep(heartbeat_delay).await;
                        info!("[TIMEOUT] Timeout reached, sending heartbeat...");

                        {
                            let mut node = node_clone.write().await;
                            node.vote_count
                                .store(0, std::sync::atomic::Ordering::Relaxed);
                            let _ = node.send_heartbeat().await;
                        }
                    }
                    _ => {
                        tokio::time::sleep(timeout).await;

                        let last = *last_heartbeat.read().await;

                        if last.elapsed() > timeout {
                            warn!("[TIMEOUT] Timeout reached, starting leader election. Time since last heartbeat: {:?}, timeout is {:?}", last.elapsed(), timeout);

                            {
                                let mut node = node_clone.write().await;
                                node.state = PaxosState::Candidate;
                                let _ = node.start_leader_election().await;
                            }

                            let mut last_heartbeat_mut = last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }

                        let running = {
                            let node = node_clone.read().await;
                            node.running
                        };

                        if !running {
                            break;
                        }
                    }
                }
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
                        info!("[TIMEOUT] Running is false, stopping",);
                        break;
                    }
                    node.socket.clone()
                };

                let (stream, message) = match listen(&socket).await {
                    Ok(msg) => msg,
                    Err(_) => {
                        error!("[ERROR] Received bad message on socket, continuing...");
                        continue;
                    }
                };

                match message {
                    // Leader election
                    PaxosMessage::Heartbeat {
                        epoch,
                        commit_id,
                        source,
                    } => {
                        info!("[REQUEST] Received Heartbeat from {}", source);
                        {
                            let node = node_arc.read().await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                        {
                            let mut node = node_arc.write().await;
                            node.handle_heartbeat(&source, stream, epoch, commit_id)
                                .await;
                        }
                    }
                    PaxosMessage::LeaderVote { epoch, source } => {
                        info!("[REQUEST] Received LeaderVote from {}", source);
                        {
                            let node = node_arc.read().await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                        {
                            let mut node = node_arc.write().await;
                            node.handle_leader_vote(&source, stream, epoch).await;
                        }
                    }
                    PaxosMessage::LeaderDeclaration {
                        epoch,
                        commit_id,
                        source,
                    } => {
                        info!("[REQUEST] Received LeaderDeclaration from {}", source);
                        {
                            let node = node_arc.read().await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                        {
                            let mut node = node_arc.write().await;
                            node.handle_leader_declaration(&source, stream, epoch, commit_id)
                                .await;
                        }
                    }

                    PaxosMessage::ElectionRequest {
                        epoch,
                        request_id,
                        source,
                    } => {
                        info!(
                            "[REQUEST] Received ElectionRequest from {}, request id is {}",
                            source, request_id
                        );
                        {
                            let node = node_arc.read().await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                        {
                            let mut node = node_arc.write().await;
                            node.handle_leader_request(&source, stream, epoch, request_id)
                                .await;
                        }
                    }

                    // Transactional
                    PaxosMessage::AcceptRequest {
                        epoch,
                        request_id,
                        operation,
                        source,
                    } => {
                        info!("[REQUEST] Received AcceptRequest from {}", source);
                        {
                            let mut node = node_arc.write().await;
                            _ = node
                                .handle_leader_accept(
                                    &source, stream, epoch, request_id, &operation,
                                )
                                .await;
                        }
                    }

                    PaxosMessage::LearnRequest {
                        epoch,
                        commit_id,
                        source,
                    } => {
                        info!("[REQUEST] Received LearnRequest from {}", source);
                        {
                            let mut node = node_arc.write().await;
                            _ = node
                                .handle_leader_learn(&source, stream, epoch, commit_id)
                                .await;
                        }
                    }

                    PaxosMessage::Ack {
                        epoch: _,
                        request_id,
                        source,
                    } => {
                        info!("[REQUEST] Received FollowerAck from {}", source);
                        {
                            let node = node_arc.write().await;
                            node.handle_follower_ack(&source, stream, request_id).await;
                        }
                    }

                    // Client request
                    PaxosMessage::ClientRequest { operation, source } => {
                        info!("[REQUEST] Received ClientRequest from {}", source);
                        {
                            let mut node = node_arc.write().await;
                            node.handle_client_request(&source, stream, operation).await;
                        }
                    }

                    PaxosMessage::RecoveryRequest { key, source } => {
                        info!("[REQUEST] Received RecoveryRequest from {}", source);
                        {
                            let node = node_arc.read().await;
                            node.handle_recovery_request(&source, stream, &key).await;
                        }
                    }

                    _ => {
                        error!(
                            "[REQUEST] Received invalid message from {:?}",
                            stream.peer_addr()
                        );
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
        node.store.initialize().await;
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
