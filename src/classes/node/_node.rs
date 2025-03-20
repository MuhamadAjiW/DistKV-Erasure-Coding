use core::{panic, time};
use std::{io, sync::Arc, time::Duration};

use tokio::{net::TcpListener, sync::Mutex, sync::RwLock, time::Instant};

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
            5000 + (rand::random::<u64>() % 20) * 200,
        )));

        let node = Node {
            address,
            socket,
            running,
            leader_address,
            cluster_list,
            cluster_index,
            state,
            request_id,
            last_heartbeat,
            timeout_duration,
            store,
            ec,
            ec_active,
        };

        node
    }

    pub async fn run(&mut self) -> Result<(), io::Error> {
        self.running = true;

        self.print_info().await;

        // Timer task
        let running_flag = Arc::new(Mutex::new(self.running));
        let last_heartbeat_clone = Arc::clone(&self.last_heartbeat);
        let timeout_duration = Arc::clone(&self.timeout_duration);
        let state = Arc::new(Mutex::new(self.state.clone()));
        let request_id = Arc::new(Mutex::new(self.request_id));

        tokio::spawn(async move {
            println!("[TIMEOUT] Spawning task to check for timeout");

            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let last = *last_heartbeat_clone.read().await;

                // println!(
                //     "[TIMEOUT] Checking timeout elapsed: {:?}, timeout duration:{:?}",
                //     last.elapsed(),
                //     timeout_duration
                // );
                if last.elapsed() > timeout_duration.read().await.clone() {
                    println!("[TIMEOUT] Timeout reached, starting leader election...");
                    let mut state = state.lock().await;
                    *state = PaxosState::Candidate;
                    let mut request_id = request_id.lock().await;
                    *request_id += 1;
                    let mut timeout_duration = timeout_duration.write().await;
                    *timeout_duration =
                        Duration::from_millis(5000 + (rand::random::<u64>() % 20) * 200);

                    

                    let mut last_heartbeat_mut = last_heartbeat_clone.write().await;
                    *last_heartbeat_mut = Instant::now();
                }

                // Stop the task if the node is no longer running
                if !*running_flag.lock().await {
                    break;
                }
            }
        });

        // Event loop
        while self.running {
            let (stream, message) = match listen(&self.socket).await {
                Ok(msg) => msg,
                Err(_) => {
                    println!("[ERROR] Received bad message on socket, continuing...");
                    continue;
                }
            };

            match message {
                // Paxos messages
                PaxosMessage::LeaderRequest { request_id, source } => {
                    println!("[REQUEST] Received LeaderRequest from {}", source);
                    self.handle_leader_request(&source, stream, request_id)
                        .await
                }

                PaxosMessage::LeaderAccepted {
                    request_id,
                    operation,
                    source,
                } => {
                    println!("[REQUEST] Received LeaderAccepted from {}", source);
                    self.handle_leader_accepted(&source, stream, request_id, &operation)
                        .await?;
                }

                PaxosMessage::FollowerAck { request_id, source } => {
                    println!("[REQUEST] Received FollowerAck from {}", source);
                    self.handle_follower_ack(&source, stream, request_id).await
                }

                // Client messages
                PaxosMessage::ClientRequest { operation, source } => {
                    println!("[REQUEST] Received ClientRequest from {}", source);
                    self.handle_client_request(&source, stream, operation).await;
                }

                PaxosMessage::RecoveryRequest { key, source } => {
                    println!("[REQUEST] Received RecoveryRequest from {}", source);
                    self.handle_recovery_request(&source, stream, &key).await
                }

                _ => {
                    println!(
                        "[REQUEST] Received invalid message from {:?}",
                        stream.peer_addr()
                    );
                }
            }
        }

        Ok(())
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
}
