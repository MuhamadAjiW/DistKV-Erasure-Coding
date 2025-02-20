use std::{io, sync::Arc, time::Duration};

use tokio::{
    task::JoinSet,
    time::{self, timeout},
};

use crate::{
    base_libs::{
        _operation::{BinKV, Operation, OperationType},
        _paxos_types::{FollowerRegistrationReply, FollowerRegistrationRequest, PaxosMessage},
        network::_messages::{receive_message, send_message},
    },
    classes::node::_node::Node,
};

impl Node {
    // ---Active Commands---
    async fn leader_broadcast_membership(
        &self,
        follower_list: &Vec<String>,
        index: usize,
    ) -> usize {
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
            let index = index.clone();
            let follower_addr = follower_addr.clone();
            let follower_list = follower_list.clone();

            tasks.spawn(async move {
                // Send the request to the follower
                if let Err(_e) = send_message(
                    &socket,
                    PaxosMessage::FollowerRegisterReply(FollowerRegistrationReply {
                        follower_list,
                        index,
                    }),
                    follower_addr.as_str(),
                )
                .await
                {
                    println!(
                        "Failed to broadcast request to follower at {}",
                        follower_addr
                    );
                    return None;
                }
                // println!("Broadcasted request to follower at {}", follower_addr);

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

    async fn leader_broadcast_prepare(&self, follower_list: &Vec<String>) -> usize {
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

    async fn leader_broadcast_accept_ec(
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

    async fn leader_broadcast_accept_replication(
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

    // ---Handlers---
    pub async fn leader_handle_client_request(
        &mut self,
        src_addr: &String,
        _request_id: u64,
        payload: &Vec<u8>,
    ) -> Result<(), io::Error> {
        let original_message = String::from_utf8_lossy(&payload).to_string(); // Capture original client message

        let req = Operation::parse(payload);
        if matches!(req, None) {
            println!("Request was invalid, dropping request");
            return Ok(());
        }
        let operation = req.unwrap();
        let mut result: String;
        let message: &str;
        let initial_request_id = self.request_id;
        let load_balancer_addr = &self.load_balancer_address.to_string() as &str;

        // Benchmarking variables
        let mut start = time::Instant::now();
        let operation_time: time::Duration;

        match operation.op_type {
            OperationType::BAD | OperationType::PING => {
                message = "Request is handled by leader";
                result = self.store.process_request(&operation);
            }
            OperationType::GET => {
                message = "Request is handled by leader";
                result = self.store.process_request(&operation);

                if result.is_empty() {
                    result = self
                        .get_from_cluster(
                            &("".to_string()
                                + &operation.kv.key
                                + ".."
                                + &self.address.ip.to_string()
                                + ".."
                                + &self.address.port.to_string()
                                + ".log"),
                            &operation.kv.key,
                        )
                        .await
                        .expect("Failed to get from cluster");
                }
            }
            _ => {
                let follower_list: Vec<String> = {
                    let followers_guard = self.cluster_list.lock().unwrap();
                    followers_guard.iter().cloned().collect()
                };

                let majority = follower_list.len() / 2 + 1;
                let mut acks = self.leader_broadcast_prepare(&follower_list).await;

                if acks >= majority {
                    // Benchmarking starts here
                    start = time::Instant::now();

                    result = self.store.process_request(&operation);

                    if self.ec_active {
                        let encoded_shard = self.ec.encode(&operation.kv.value);
                        self.store
                            .persist_request_ec(
                                &("".to_string()
                                    + &operation.kv.key
                                    + ".."
                                    + &self.address.ip.to_string()
                                    + ".."
                                    + &self.address.port.to_string()
                                    + ".log"),
                                &Operation {
                                    op_type: operation.op_type.clone(),
                                    kv: BinKV {
                                        key: operation.kv.key.clone(),
                                        value: encoded_shard[self.cluster_index].clone(),
                                    },
                                },
                            )
                            .await?;

                        acks = self
                            .leader_broadcast_accept_ec(&follower_list, &operation, &encoded_shard)
                            .await;
                    } else {
                        acks = self
                            .leader_broadcast_accept_replication(&follower_list, &operation)
                            .await;
                    }

                    if acks > majority {
                        message = "Leader broadcasted the message successfully";
                    } else {
                        message = "Accept broadcast is not accepted by majority";
                    }

                    self.request_id += 1;
                } else {
                    result = "Request failed.".to_string();
                    message = "Request broadcast is not accepted by majority";
                }
            }
        }

        operation_time = start.elapsed();
        println!(
            "{};{};{};{}",
            operation.kv.value.len(),
            operation_time.as_secs_f64(),
            src_addr,
            &self.request_id
        );

        let response = format!(
            "Request ID: {}\nMessage: {}\nReply: {}.",
            initial_request_id, message, result
        );
        self.socket
            .send_to(response.as_bytes(), load_balancer_addr)
            .await
            .unwrap();

        Ok(())
    }

    pub async fn leader_handle_follower_register_request(
        &self,
        follower: &FollowerRegistrationRequest,
    ) {
        let mut followers_guard = self.cluster_list.lock().unwrap();
        let index = followers_guard.len();
        followers_guard.push(follower.follower_addr.clone());
        println!("Follower registered: {}", follower.follower_addr);

        self.leader_broadcast_membership(&followers_guard, index)
            .await;
    }

    // _TODO: handle false leader
    pub async fn leader_handle_leader_request(&self, _src_addr: &String, _request_id: u64) {}
    pub async fn leader_handle_leader_accepted(
        &self,
        _src_addr: &String,
        _request_id: u64,
        _operation: &Operation,
    ) {
    }
    // _TODO: handle false message
    pub async fn leader_handle_follower_ack(&self, _src_addr: &String, _request_id: u64) {}
    pub async fn leader_handle_follower_register_reply(
        &self,
        _src_addr: &String,
        _follower: &FollowerRegistrationReply,
    ) {
    }
}
