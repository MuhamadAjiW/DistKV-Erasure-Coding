use core::panic;
use std::{
    io,
    sync::{Arc, Mutex},
};

use tokio::net::TcpListener;

use crate::{
    base_libs::{
        _paxos_types::PaxosMessage,
        network::{_address::Address, _messages::receive_message},
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
        let store = StorageController::new(&address.to_string(), &memcached_url);

        let ec = if ec_active {
            Some(Arc::new(ECService::new(shard_count, parity_count)))
        } else {
            None
        };

        let node = Node {
            address,
            socket,
            running,
            leader_address,
            cluster_list,
            cluster_index,
            state,
            request_id,
            store,
            ec,
            ec_active,
        };

        node
    }

    // Main Event Loop
    pub async fn run(&mut self) -> Result<(), io::Error> {
        self.running = true;

        self.print_info();
        while self.running {
            let (message, src_addr) = match receive_message(&self.socket).await {
                Ok(msg) => msg,
                Err(_) => {
                    println!("[ERROR] Received bad message on socket, continuing...");
                    continue;
                }
            };

            match message {
                // Paxos messages
                PaxosMessage::LeaderRequest { request_id } => {
                    println!(
                        "[REQUEST] Received LeaderRequest from {}",
                        src_addr.to_string()
                    );
                    self.handle_leader_request(&src_addr, request_id).await
                }

                PaxosMessage::LeaderAccepted {
                    request_id,
                    operation,
                } => {
                    println!(
                        "[REQUEST] Received LeaderAccepted from {}",
                        src_addr.to_string()
                    );
                    self.handle_leader_accepted(&src_addr, request_id, &operation)
                        .await?;
                }

                PaxosMessage::FollowerAck { request_id } => {
                    println!(
                        "[REQUEST] Received FollowerAck from {}",
                        src_addr.to_string()
                    );
                    self.handle_follower_ack(&src_addr, request_id).await
                }

                // Client messages
                PaxosMessage::ClientRequest {
                    request_id,
                    payload,
                } => {
                    println!(
                        "[REQUEST] Received ClientRequest from {}",
                        src_addr.to_string()
                    );
                    self.handle_client_request(&src_addr, request_id, &payload)
                        .await;
                }

                PaxosMessage::RecoveryRequest { key } => {
                    println!(
                        "[REQUEST] Received RecoveryRequest from {}",
                        src_addr.to_string()
                    );
                    self.handle_recovery_request(&src_addr, &key).await
                }

                _ => {
                    println!(
                        "[REQUEST] Received invalid message from {}",
                        src_addr.to_string()
                    );
                }
            }
        }

        Ok(())
    }

    // Information logging
    pub fn print_info(&self) {
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
        println!("Cluster list: {:?}", &self.cluster_list.lock().unwrap());
        println!("Cluster index: {}", &self.cluster_index);

        if let Some(ec) = &self.ec {
            println!("\nErasure coding configuration:");
            println!("Shard count: {}", &self.ec.as_ref().unwrap().shard_count);
            println!("Parity count: {}", &self.ec.as_ref().unwrap().parity_count);
        } else {
            println!("Erasure coding is not active");
        }
        println!("-------------------------------------");
    }
}
