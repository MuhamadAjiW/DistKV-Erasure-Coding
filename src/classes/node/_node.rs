use core::panic;
use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use actix_web::{web, App, HttpServer};
use tokio::{net::TcpListener, sync::Mutex, sync::RwLock, time::Instant};
use tracing::instrument;

use crate::{
    base_libs::{
        _paxos_types::PaxosMessage,
        network::{_address::Address, _messages::listen},
    },
    classes::{
        config::_config::Config, ec::_ec_service::ECService,
        store::_storage_controller::StorageController,
    },
};

use super::paxos::_paxos::PaxosState;

pub struct Node {
    // Base attributes
    pub address: Address,
    pub http_address: Address,
    pub socket: Arc<TcpListener>,
    pub running: bool,

    // Paxos related attributes
    pub leader_address: Option<Address>,
    pub cluster_list: Arc<Mutex<Vec<String>>>,
    pub cluster_index: usize,
    pub state: PaxosState,
    pub request_id: u64,
    pub last_heartbeat: Arc<RwLock<Instant>>,
    pub timeout_duration: Arc<RwLock<Duration>>,
    pub vote_count: AtomicUsize,

    // Application attributes
    pub store: StorageController,
    pub ec: Option<Arc<ECService>>,
    pub ec_active: bool,
}

impl Node {
    pub async fn from_config(address: Address, config_path: &str) -> Self {
        let config = Config::get_config(config_path).await;
        let index = Config::get_node_index(&config, &address);
        let node_list: Vec<String> = Config::get_node_addresses(&config)
            .into_iter()
            .map(|addr| addr.to_string())
            .collect();

        if let Some(index) = index {
            println!("[INIT] Storage Config: {:?}", config.storage);
            println!("[INIT] Node Config: {:?}", config.nodes[index]);
            println!("[INIT] Index: {}", index);
            return Node::new(
                address,
                Address::new(&config.nodes[index].ip, config.nodes[index].http_port),
                Address::new(
                    &config.nodes[index].memcached.ip,
                    config.nodes[index].memcached.port,
                ),
                node_list,
                index,
                config.storage.shard_count,
                config.storage.parity_count,
                config.storage.erasure_coding,
                config.nodes[index].rocks_db.path.as_str(),
            )
            .await;
        }

        panic!("Error: Failed to get node config");
    }

    pub async fn new(
        address: Address,
        http_address: Address,
        memcached_address: Address,
        cluster_list: Vec<String>,
        cluster_index: usize,
        shard_count: usize,
        parity_count: usize,
        ec_active: bool,
        db_path: &str,
    ) -> Self {
        let socket = Arc::new(TcpListener::bind(address.to_string()).await.unwrap());
        let running = false;

        let leader_address = None;
        let cluster_list = Arc::new(Mutex::new(cluster_list));
        let cluster_index = cluster_index;
        let state = PaxosState::Follower;
        let request_id = 0;

        let memcached_url = format!(
            "memcache://{}:{}",
            memcached_address.ip, memcached_address.port
        );
        let store = StorageController::new(db_path, &memcached_url);

        let ec = if ec_active {
            Some(Arc::new(ECService::new(shard_count, parity_count)))
        } else {
            None
        };

        let last_heartbeat = Arc::new(RwLock::new(Instant::now()));
        let timeout_duration = Arc::new(RwLock::new(Duration::from_millis(
            5000 + (rand::random::<u64>() % 100) * 50,
        )));
        let vote_count = AtomicUsize::new(0);

        let node = Node {
            address,
            http_address,
            socket,
            running,
            leader_address,
            cluster_list,
            cluster_index,
            state,
            request_id,
            last_heartbeat,
            timeout_duration,
            vote_count,
            store,
            ec,
            ec_active,
        };

        node
    }

    // Information logging
    pub async fn print_info(&self) {
        println!("-------------------------------------");
        println!("[INFO] Node info:");
        println!("Address: {}", &self.address.to_string());
        println!("State: {}", &self.state.to_string());
        println!("Erasure Coding: {}", &self.ec_active.to_string());
        if let Some(leader) = &self.leader_address {
            println!("Leader: {}", &leader.to_string());
        } else {
            println!("Leader: None");
        }
        println!("Cluster list: {:?}", &self.cluster_list.lock().await);
        println!("Cluster index: {}", &self.cluster_index);

        if let Some(_ec) = &self.ec {
            println!("\nErasure coding configuration:");
            println!("Shard count: {}", &self.ec.as_ref().unwrap().shard_count);
            println!("Parity count: {}", &self.ec.as_ref().unwrap().parity_count);
        } else {
            println!("Erasure coding is not active");
        }
        println!("-------------------------------------");
    }

    pub fn run_timer_task(node_arc: Arc<Mutex<Node>>) {
        println!("[INIT] Starting timer task...");
        let node_clone = Arc::clone(&node_arc);

        tokio::spawn(async move {
            let (last_heartbeat, timeout_duration) = {
                let mut node = node_arc.lock().await;
                node.running = true;
                (
                    Arc::clone(&node.last_heartbeat),
                    Arc::clone(&node.timeout_duration),
                )
            };

            println!("[TIMEOUT] Spawning task to check for timeout");

            loop {
                let state = {
                    let node = node_clone.lock().await;
                    node.state
                };

                match state {
                    PaxosState::Leader => {
                        let heartbeat_delay = *timeout_duration.read().await / 3;

                        tokio::time::sleep(heartbeat_delay).await;
                        println!("[TIMEOUT] Timeout reached, sending heartbeat...");

                        {
                            let mut node = node_clone.lock().await;
                            node.vote_count
                                .store(0, std::sync::atomic::Ordering::Relaxed);
                            let _ = node.send_heartbeat().await;
                        }
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_millis(50)).await;

                        let last = *last_heartbeat.read().await;
                        let timeout = *timeout_duration.read().await;

                        if last.elapsed() > timeout {
                            {
                                let mut node = node_clone.lock().await;
                                node.state = PaxosState::Candidate;
                                node.request_id += 1;
                                node.vote_count
                                    .store(1, std::sync::atomic::Ordering::Relaxed);
                                let _ = node.start_leader_election().await;
                            }

                            let mut last_heartbeat_mut = last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }

                        let running = {
                            let node = node_clone.lock().await;
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

    #[instrument(skip_all)]
    pub fn run_tcp_loop(node_arc: Arc<Mutex<Node>>) {
        println!("[INIT] Starting TCP loop...");

        tokio::spawn(async move {
            loop {
                let socket = {
                    let node = node_arc.lock().await;
                    if !node.running {
                        println!("[TIMEOUT] Running is false, stopping",);
                        break;
                    }
                    node.socket.clone()
                };

                let (stream, message) = match listen(&socket).await {
                    Ok(msg) => msg,
                    Err(_) => {
                        println!("[ERROR] Received bad message on socket, continuing...");
                        continue;
                    }
                };

                match message {
                    // Leader election
                    PaxosMessage::Heartbeat { request_id, source } => {
                        println!("[REQUEST] Received Heartbeat from {}", source);
                        {
                            let mut node = node_arc.lock().await;
                            node.handle_heartbeat(&source, stream, request_id).await;

                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }
                    PaxosMessage::LeaderVote { request_id, source } => {
                        println!("[REQUEST] Received LeaderVote from {}", source);
                        {
                            let mut node = node_arc.lock().await;
                            node.handle_leader_vote(&source, stream, request_id).await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }
                    PaxosMessage::LeaderDeclaration { request_id, source } => {
                        println!("[REQUEST] Received LeaderDeclaration from {}", source);
                        {
                            let mut node = node_arc.lock().await;
                            node.handle_leader_declaration(&source, stream, request_id)
                                .await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }

                    // Transactional
                    PaxosMessage::LeaderRequest { request_id, source } => {
                        println!(
                            "[REQUEST] Received LeaderRequest from {}, request id is {}",
                            source, request_id
                        );
                        {
                            let mut node = node_arc.lock().await;
                            node.handle_leader_request(&source, stream, request_id)
                                .await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }

                    PaxosMessage::LeaderLearn { request_id, source } => {
                        println!("[REQUEST] Received LeaderAccepted from {}", source);
                        {
                            let mut node = node_arc.lock().await;
                            _ = node
                                .handle_leader_accepted(&source, stream, request_id)
                                .await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }

                    PaxosMessage::LeaderAccepted {
                        request_id,
                        operation,
                        source,
                    } => {
                        println!("[REQUEST] Received LeaderAccepted from {}", source);
                        {
                            let mut node = node_arc.lock().await;
                            _ = node
                                .handle_leader_learn(&source, stream, request_id, &operation)
                                .await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }

                    PaxosMessage::FollowerAck { request_id, source } => {
                        println!("[REQUEST] Received FollowerAck from {}", source);
                        {
                            let node = node_arc.lock().await;
                            node.handle_follower_ack(&source, stream, request_id).await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }

                    // Client request
                    PaxosMessage::ClientRequest { operation, source } => {
                        println!("[REQUEST] Received ClientRequest from {}", source);
                        {
                            let mut node = node_arc.lock().await;
                            node.handle_client_request(&source, stream, operation).await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }

                    PaxosMessage::RecoveryRequest { key, source } => {
                        println!("[REQUEST] Received RecoveryRequest from {}", source);
                        {
                            let node = node_arc.lock().await;
                            node.handle_recovery_request(&source, stream, &key).await;
                            let mut last_heartbeat_mut = node.last_heartbeat.write().await;
                            *last_heartbeat_mut = Instant::now();
                        }
                    }

                    _ => {
                        println!(
                            "[REQUEST] Received invalid message from {:?}",
                            stream.peer_addr()
                        );
                    }
                }
            }
        });
    }

    #[instrument(skip_all)]
    pub fn run_http_loop(node_arc: Arc<Mutex<Node>>) {
        println!("[INIT] Starting HTTP loop...");

        // Actix web is not compatible with tokio spawn
        std::thread::spawn(move || {
            let sys = actix_web::rt::System::new();

            sys.block_on(async move {
                let address = {
                    let node = node_arc.lock().await;
                    node.http_address.clone()
                };

                println!(
                    "[INIT] Starting HTTP server on {}:{}",
                    address.ip, address.port
                );

                HttpServer::new(move || {
                    App::new()
                        .app_data(web::Data::new(node_arc.clone()))
                        .route("/", web::get().to(Node::http_healthcheck))
                        .route("/kv/range", web::post().to(Node::http_get))
                        .route("/kv/put", web::post().to(Node::http_put))
                        .route("/kv/deleterange", web::post().to(Node::http_delete))
                })
                .bind((address.ip, address.port))
                .expect("[ERROR] Failed to bind HTTP server")
                .run()
                .await
                .expect("[ERROR] Actix server crashed");
            })
        });
    }

    #[instrument(skip_all)]
    pub async fn run(node_arc: Arc<Mutex<Node>>) {
        println!("[INIT] Running node...");

        Node::run_timer_task(node_arc.clone());
        Node::run_tcp_loop(node_arc.clone());
        Node::run_http_loop(node_arc.clone());

        loop {}
    }
}
