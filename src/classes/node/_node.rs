use std::{
    io,
    sync::{Arc, Mutex},
};

use tokio::net::UdpSocket;

use crate::{
    base_libs::{
        _paxos_types::PaxosMessage,
        network::{
            _address::Address,
            _messages::{receive_message, send_message},
        },
    },
    classes::{ec::_ec_service::ECService, store::_storage_controller::StorageController},
};

use super::paxos::_paxos::PaxosState;

pub struct Node {
    // Base attributes
    pub address: Address,
    pub socket: Arc<UdpSocket>,
    pub running: bool,

    // Paxos related attributes
    pub load_balancer_address: Address,
    pub leader_address: Address,
    pub cluster_list: Arc<Mutex<Vec<String>>>,
    pub cluster_index: usize,
    pub state: PaxosState,
    pub request_id: u64,

    // Application attributes
    pub store: StorageController<'static>,
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
        let socket = Arc::new(UdpSocket::bind(address.to_string()).await.unwrap());
        let running = false;
        let cluster_list = Arc::new(Mutex::new(Vec::new()));
        let cluster_index = std::usize::MAX;
        let memcached_url = format!("memcached://{}:{}", address.ip, address.port + 10000);

        let ec = Arc::new(ECService::new(shard_count, parity_count));
        let store = StorageController::new(&address.to_string(), &memcached_url, None);
        let request_id = 0;

        let mut node = Node {
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
        let node_ref = &node as *const _;
        node.store.assign_node(unsafe { &*node_ref });

        node
    }

    pub async fn run(&mut self) -> Result<(), io::Error> {
        println!("Starting node...");
        self.print_info();
        self.running = true;

        match self.state {
            PaxosState::Follower => {
                if !self.follower_send_register().await {
                    return Ok(());
                }
            }
            PaxosState::Leader => {
                let mut followers_guard = self.cluster_list.lock().unwrap();
                followers_guard.push(self.address.to_string());

                self.cluster_index = 0;
            }
        }

        while self.running {
            let (message, src_addr) = receive_message(&self.socket).await.unwrap();

            match message {
                // Paxos messages
                PaxosMessage::LeaderRequest { request_id } => {
                    self.handle_leader_request(&src_addr, request_id).await
                }

                PaxosMessage::LeaderAccepted {
                    request_id,
                    operation,
                } => {
                    self.handle_leader_accepted(&src_addr, request_id, &operation)
                        .await?;
                }

                // Client messages
                PaxosMessage::ClientRequest {
                    request_id,
                    payload,
                } => {
                    self.handle_client_request(&src_addr, request_id, &payload)
                        .await?;
                }

                PaxosMessage::FollowerAck { request_id } => {
                    self.handle_follower_ack(&src_addr, request_id).await
                }

                PaxosMessage::FollowerRegisterRequest(follower) => {
                    self.handle_follower_register_request(&src_addr, &follower)
                        .await
                }

                PaxosMessage::FollowerRegisterReply(follower) => {
                    self.handle_follower_register_reply(&src_addr, &follower)
                        .await
                }

                PaxosMessage::RecoveryRequest { key } => {
                    self.handle_recovery_request(&src_addr, &key).await
                }

                // _TODO: Handle faulty requests
                PaxosMessage::RecoveryReply {
                    index: _,
                    payload: _,
                } => {}
            }
        }

        Ok(())
    }

    pub async fn forward_to_leader(&self, payload: Vec<u8>) {
        send_message(
            &self.socket,
            PaxosMessage::ClientRequest {
                request_id: self.request_id,
                payload: payload.clone(),
            },
            &self.leader_address.to_string() as &str,
        )
        .await
        .unwrap();
    }

    pub fn print_info(&self) {
        println!("Node info:");
        println!("Address: {}", self.address.to_string());
        println!("Leader: {}", self.leader_address.to_string());
        println!("Load Balancer: {}", self.load_balancer_address.to_string());
        println!("State: {}", self.state.to_string());
        println!("Erasure Coding: {}", self.ec_active.to_string());
    }
}
