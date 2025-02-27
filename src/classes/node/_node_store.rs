use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, RwLock},
    task::JoinSet,
    time::timeout,
};

use crate::base_libs::{
    _operation::{BinKV, Operation, OperationType},
    _paxos_types::PaxosMessage,
    network::_messages::{receive_message, send_message},
};

use super::_node::Node;

impl Node {
    pub async fn forward_to_leader(&self, payload: Vec<u8>) {
        println!(
            "[FORWARD] Forwarding request to leader at {}",
            self.leader_address
        );
        send_message(
            &self.socket,
            PaxosMessage::ClientRequest {
                request_id: self.request_id,
                payload: payload,
            },
            &self.leader_address.to_string() as &str,
        )
        .await
        .unwrap();
    }

    pub async fn handle_client_request(
        &self,
        _src_addr: &String,
        _request_id: u64,
        payload: &Vec<u8>,
    ) -> () {
        let req = Operation::parse(payload);
        if matches!(req, None) {
            println!("Request was invalid, dropping request");
            return ();
        }
        let operation = req.unwrap();
        let initial_request_id = self.request_id;
        let load_balancer_addr = &self.load_balancer_address.to_string() as &str;
        let message = format!("Handled by {}", self.address.to_string());
        let result: String;

        match operation.op_type {
            OperationType::BAD => {
                println!("Invalid request");
                result = "Invalid request".to_string();
            }
            OperationType::PING => {
                println!("Received PING request");
                result = "PONG".to_string();
            }
            OperationType::GET | OperationType::DELETE | OperationType::SET => {
                println!("Received request: {:?}", operation);
                result = self
                    .store
                    .process_request(&operation, &self)
                    .await
                    .unwrap_or_default();
            }
        }

        let response = format!(
            "Request ID: {}\nMessage: {}\nReply: {}.",
            initial_request_id, message, result
        );
        println!("{}", response);

        self.socket
            .send_to(response.as_bytes(), load_balancer_addr)
            .await
            .unwrap();
    }

    pub async fn handle_recovery_request(&self, src_addr: &String, key: &str) {
        match self.store.persistent.get(key) {
            Some(value) => {
                // Send the data to the requestor
                send_message(
                    &self.socket,
                    PaxosMessage::RecoveryReply {
                        index: self.cluster_index,
                        payload: value,
                    },
                    &src_addr,
                )
                .await
                .unwrap();
                println!("Sent data request to follower at {}", src_addr);
            }
            None => {
                println!("No value found");
            }
        }
    }

    pub async fn broadcast_prepare(&self, follower_list: &Vec<String>) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 1;
        let mut tasks = JoinSet::new();

        for follower_addr in follower_list.iter() {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let socket = Arc::clone(&self.socket);
            let follower_addr = follower_addr.clone();
            let request_id = self.request_id.clone();

            tasks.spawn(async move {
                // Send the request to the follower
                send_message(
                    &socket,
                    PaxosMessage::LeaderRequest {
                        request_id: request_id,
                    },
                    &follower_addr,
                )
                .await
                .unwrap();
                // println!(
                //     "Leader broadcasted request to follower at {}",
                //     follower_addr
                // );

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(2), receive_message(&socket)).await {
                    Ok(Ok((ack, _))) => {
                        if let PaxosMessage::FollowerAck { .. } = ack {
                            // println!(
                            //     "Leader received acknowledgment from follower at {}",
                            //     follower_addr
                            // );
                            return Some(1);
                        }
                    }
                    Ok(Err(e)) => {
                        println!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        println!(
                            "Timeout waiting for acknowledgment from follower at {}",
                            follower_addr
                        );
                    }
                }

                return Some(0);
            });
        }

        while let Some(response) = tasks.join_next().await {
            match response {
                Ok(Some(value)) => {
                    acks += value;
                }
                _ => {} // Handle errors or None responses if needed
            }
        }

        acks
    }

    pub async fn broadcast_accept_ec(
        &self,
        follower_list: &Vec<String>,
        operation: &Operation,
        encoded_shard: &Vec<Vec<u8>>,
    ) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 1;
        let mut tasks = JoinSet::new();

        for (index, follower_addr) in follower_list.iter().enumerate() {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let socket = Arc::clone(&self.socket);
            let follower_addr = follower_addr.clone();
            let request_id = self.request_id.clone();

            let sent_operation = Operation {
                op_type: operation.op_type.clone(),
                kv: BinKV {
                    key: operation.kv.key.clone(),
                    value: encoded_shard[index].clone(),
                },
            };

            tasks.spawn(async move {
                // Send the request to the follower
                send_message(
                    &socket,
                    PaxosMessage::LeaderAccepted {
                        request_id: request_id,
                        operation: sent_operation,
                    },
                    follower_addr.as_str(),
                )
                .await
                .unwrap();
                // println!(
                //     "Leader broadcasted request to follower at {}",
                //     follower_addr
                // );

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(2), receive_message(&socket)).await {
                    Ok(Ok((ack, _))) => {
                        if let PaxosMessage::FollowerAck { .. } = ack {
                            // println!(
                            //     "Leader received acknowledgment from follower at {}",
                            //     follower_addr
                            // );

                            return Some(1);
                        }
                    }
                    Ok(Err(e)) => {
                        println!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        println!(
                            "Timeout waiting for acknowledgment from follower at {}",
                            follower_addr
                        );
                    }
                }
                return Some(0);
            });
        }

        while let Some(response) = tasks.join_next().await {
            match response {
                Ok(Some(value)) => {
                    acks += value;
                }
                _ => {} // Handle errors or None responses if needed
            }
        }

        acks
    }

    pub async fn broadcast_accept_replication(
        &self,
        follower_list: &Vec<String>,
        operation: &Operation,
    ) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 1;
        let mut tasks = JoinSet::new();

        for follower_addr in follower_list {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let socket = Arc::clone(&self.socket);
            let follower_addr = follower_addr.clone();
            let request_id = self.request_id.clone();
            let operation = operation.clone();

            tasks.spawn(async move {
                // Send the request to the follower
                send_message(
                    &socket,
                    PaxosMessage::LeaderAccepted {
                        request_id: request_id,
                        operation: operation,
                    },
                    follower_addr.as_str(),
                )
                .await
                .unwrap();
                // println!(
                //     "Leader broadcasted request to follower at {}",
                //     follower_addr
                // );

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(2), receive_message(&socket)).await {
                    Ok(Ok((ack, _))) => {
                        if let PaxosMessage::FollowerAck { .. } = ack {
                            // println!(
                            //     "Leader received acknowledgment from follower at {}",
                            //     follower_addr
                            // );
                            return Some(1);
                        }
                    }
                    Ok(Err(e)) => {
                        println!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        println!(
                            "Timeout waiting for acknowledgment from follower at {}",
                            follower_addr
                        );
                    }
                }
                return Some(0);
            });
        }

        while let Some(response) = tasks.join_next().await {
            match response {
                Ok(Some(value)) => {
                    acks += value;
                }
                _ => {} // Handle errors or None responses if needed
            }
        }

        acks
    }

    pub async fn broadcast_get_shards(
        &self,
        follower_list: &Vec<String>,
        own_shard: &Option<Vec<u8>>,
        key: &str,
    ) -> Vec<Option<Vec<u8>>> {
        let recovery_shards = Arc::new(RwLock::new(vec![
            None;
            self.ec.shard_count + self.ec.parity_count
        ]));
        recovery_shards.write().await[self.cluster_index] = own_shard.clone();

        let size = follower_list.len();
        let response_count = Arc::new(AtomicUsize::new(1));
        let required_count = self.ec.shard_count;
        let notify = Arc::new(Notify::new());

        let mut tasks = JoinSet::new();

        for follower_addr in follower_list.iter() {
            let socket = Arc::clone(&self.socket);
            let key = key.to_string();
            let follower_addr = follower_addr.clone();
            let notify = Arc::clone(&notify);
            let recovery_shards = Arc::clone(&recovery_shards);
            let response_count = Arc::clone(&response_count);

            tasks.spawn(async move {
                if let Err(_e) = send_message(
                    &socket,
                    PaxosMessage::RecoveryRequest { key },
                    follower_addr.as_str(),
                )
                .await
                {
                    println!(
                        "Failed to broadcast request to follower at {}",
                        follower_addr
                    );
                    return;
                }
                // println!("Broadcasted request to follower at {}", follower_addr);

                match timeout(Duration::from_secs(2), receive_message(&socket)).await {
                    Ok(Ok((ack, _))) => {
                        if let PaxosMessage::RecoveryReply { index, payload } = ack {
                            // println!("Received acknowledgment from {} ({})", follower_addr, index);

                            if index < size {
                                let mut shards = recovery_shards.write().await;
                                shards[index] = Some(payload);

                                let count = response_count.fetch_add(1, Ordering::SeqCst);
                                if count >= required_count {
                                    notify.notify_one();
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        println!(
                            "Error receiving acknowledgment from {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        println!("Timeout waiting for acknowledgment from {}", follower_addr);
                    }
                }
            });
        }

        // Process tasks and exit early if enough responses are gathered
        while response_count.load(Ordering::SeqCst) < required_count {
            tokio::select! {
                Some(_) = tasks.join_next() => {},
                _ = notify.notified() => break
            }
        }

        let recovery_shards = recovery_shards.read().await;
        recovery_shards.clone()
    }
}
