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
    classes::{ec::_ec_service::ECService, store::_storage_controller::StorageController},
};

use super::paxos::_paxos::PaxosState;

pub struct Node {
    // Base attributes
    pub address: Address,
    pub socket: Arc<TcpListener>,
    pub running: bool,

    // Paxos related attributes
    pub load_balancer_address: Address,
    pub leader_address: Address,
    pub cluster_list: Arc<Mutex<Vec<String>>>,
    pub cluster_index: usize,
    pub state: PaxosState,
    pub request_id: u64,

    // Application attributes
    pub store: StorageController,
    pub ec: Arc<ECService>,
    pub ec_active: bool,
}

impl Node {
    pub async fn new(
        address: Address,
        leader_address: Address,
        load_balancer_address: Address,
        state: PaxosState,
        shard_count: usize,
        parity_count: usize,
        ec_active: bool,
    ) -> Self {
        let socket = Arc::new(TcpListener::bind(address.to_string()).await.unwrap());
        let running = false;
        let cluster_list = Arc::new(Mutex::new(Vec::new()));
        let cluster_index = std::usize::MAX;
        let memcached_url = format!("memcache://{}:{}", address.ip, address.port + 10000);

        let ec = Arc::new(ECService::new(shard_count, parity_count));
        let store = StorageController::new(&address.to_string(), &memcached_url);
        let request_id = 0;

        let node = Node {
            address,
            socket,
            running,
            load_balancer_address,
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
        println!("Starting node...");
        self.running = true;

        match self.state {
            PaxosState::Follower => {
                println!("[SETUP] Setting up as follower...");
                if !self.follower_send_register().await {
                    return Ok(());
                }
            }
            PaxosState::Leader => {
                println!("[SETUP] Setting up as leader...");
                let mut followers_guard = self.cluster_list.lock().unwrap();
                followers_guard.push(self.address.to_string());

                self.cluster_index = 0;
            }
        }

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

                PaxosMessage::FollowerRegisterRequest(follower) => {
                    println!(
                        "[REQUEST] Received FollowerRegisterRequest from {}",
                        src_addr.to_string()
                    );
                    self.handle_follower_register_request(&src_addr, &follower)
                        .await
                }

                PaxosMessage::FollowerRegisterReply(follower) => {
                    println!(
                        "[REQUEST] Received FollowerRegisterReply from {}",
                        src_addr.to_string()
                    );
                    self.handle_follower_register_reply(&src_addr, &follower)
                        .await
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

                // _TODO: Handle faulty requests
                PaxosMessage::RecoveryReply {
                    index: _,
                    payload: _,
                } => {
                    println!(
                        "[REQUEST] Received RecoveryReply from {}",
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
        println!("Address: {}", self.address.to_string());
        println!("Leader: {}", self.leader_address.to_string());
        println!("Load Balancer: {}", self.load_balancer_address.to_string());
        println!("State: {}", self.state.to_string());
        println!("Erasure Coding: {}", self.ec_active.to_string());
        println!("Shard count: {}", self.ec.shard_count);
        println!("Parity count: {}", self.ec.parity_count);
        println!("Cluster list: {:?}", self.cluster_list.lock().unwrap());
        println!("Cluster index: {}", self.cluster_index);
        println!("-------------------------------------");
    }
}
