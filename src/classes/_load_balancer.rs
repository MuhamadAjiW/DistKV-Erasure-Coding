use std::sync::{Arc, Mutex};
use tokio::net::UdpSocket;

use crate::base_libs::{_paxos_types::PaxosMessage, network::_messages::send_message};

pub struct LoadBalancer {
    leader_addr: Option<String>, // Handle dynamic leader registration
    follower_addrs: Arc<Mutex<Vec<String>>>,
    next_idx: Arc<Mutex<usize>>,
    client_addr: Arc<Mutex<Option<String>>>, // Keep track of the client's address
}

impl LoadBalancer {
    pub fn new() -> Self {
        LoadBalancer {
            leader_addr: None,
            follower_addrs: Arc::new(Mutex::new(Vec::new())),
            next_idx: Arc::new(Mutex::new(0)),
            client_addr: Arc::new(Mutex::new(None)),
        }
    }

    fn get_next_target(&self) -> Option<String> {
        let mut idx = self.next_idx.lock().unwrap();
        let follower_addrs = self.follower_addrs.lock().unwrap();

        // Prefer sending to leader if available, otherwise followers
        if let Some(leader_addr) = &self.leader_addr {
            if follower_addrs.is_empty() || *idx == 0 {
                *idx = (*idx + 1) % (follower_addrs.len() + 1);
                return Some(leader_addr.clone());
            }
        }

        if !follower_addrs.is_empty() {
            let target = follower_addrs[*idx - 1].clone();
            *idx = (*idx + 1) % (follower_addrs.len() + 1);
            return Some(target);
        }

        None // No target available
    }

    pub async fn register_follower(&self, follower_addr: String) {
        let mut follower_addrs = self.follower_addrs.lock().unwrap();
        follower_addrs.push(follower_addr.clone());
        println!("Load balancer registered a new follower: {}", follower_addr);
    }

    pub async fn register_leader(&mut self, leader_addr: String) {
        self.leader_addr = Some(leader_addr.clone());
        println!("Load balancer registered the leader: {}", leader_addr);
    }

    pub async fn listen_and_route(&mut self, listen_addr: &str) {
        let socket = UdpSocket::bind(listen_addr).await.unwrap();
        println!(
            "Load balancer listening for UDP messages on {}",
            listen_addr
        );

        let mut buf = vec![0; 65536];

        loop {
            let (size, client_addr) = socket.recv_from(&mut buf).await.unwrap();
            let message = String::from_utf8_lossy(&buf[..size]).to_string();
            println!(
                "Load balancer received message from {}: {}",
                client_addr, message
            );

            if message.starts_with("register:") {
                let addr = message.trim_start_matches("register:").trim().to_string();
                if self.leader_addr.is_none() {
                    self.register_leader(addr).await; // Register the leader
                } else {
                    self.register_follower(addr).await;
                }
            } else {
                // Save the client address
                {
                    let mut client_addr_guard = self.client_addr.lock().unwrap();
                    *client_addr_guard = Some(client_addr.to_string());
                    println!("Client address saved: {}", client_addr);
                }

                // Forward request to leader or followers
                let request_id = rand::random::<u64>();
                let payload = message.into_bytes();
                if let Some(target) = self.get_next_target() {
                    send_message(
                        &socket,
                        PaxosMessage::ClientRequest {
                            request_id,
                            payload,
                        },
                        &target,
                    )
                    .await
                    .unwrap();
                    println!("Load balancer forwarded request to {}", target);

                    // Wait for leader response and forward it to the client
                    let mut buf = vec![0; 65536];
                    let (size, src_addr) = socket.recv_from(&mut buf).await.unwrap();
                    let response = String::from_utf8_lossy(&buf[..size]).to_string();
                    println!(
                        "Load balancer received response from leader at {}",
                        src_addr
                    );
                    println!("Response from leader: {}", response);

                    // Send the leader response to the original client
                    if let Some(client_addr) = self.client_addr.lock().unwrap().as_ref() {
                        match socket.send_to(response.as_bytes(), client_addr).await {
                            Ok(_) => {
                                println!(
                                    "Load balancer forwarded response from {} to client {}",
                                    src_addr, client_addr
                                );
                            }
                            Err(e) => {
                                println!("Failed to send response to client: {}", e);
                            }
                        }
                    } else {
                        println!("No client address found to forward response.");
                    }
                } else {
                    println!("No target available to handle request");
                }
            }
        }
    }
}
