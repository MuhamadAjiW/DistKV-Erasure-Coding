use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    net::TcpStream,
    sync::{Notify, RwLock},
    task::JoinSet,
    time::timeout,
};

use crate::{
    base_libs::{
        _operation::{BinKV, Operation, OperationType},
        _paxos_types::PaxosMessage,
        network::_messages::{receive_message, reply_message, reply_string, send_message},
    },
    classes::node::paxos::_paxos::PaxosState,
};

use super::_node::Node;

impl Node {
    pub async fn handle_client_request(
        &mut self,
        _src_addr: &String,
        stream: TcpStream,
        operation: Operation,
    ) {
        let result: String;

        match operation.op_type {
            OperationType::BAD => {
                println!("[REQUEST] Invalid request");
                result = "Invalid request".to_string();
            }
            OperationType::PING => {
                println!("[REQUEST] Received PING request");
                result = "PONG".to_string();
            }
            OperationType::GET | OperationType::DELETE | OperationType::SET => {
                if self.state == PaxosState::Leader {
                    self.request_id += 1;
                }

                println!("[REQUEST] Received request: {:?}", operation.op_type);
                result = self
                    .store
                    .process_request(&operation, &self)
                    .await
                    .unwrap_or_default();
            }
        }

        _ = reply_string(result.as_str(), stream).await;
    }

    pub async fn handle_recovery_request(&self, src_addr: &String, stream: TcpStream, key: &str) {
        match self.store.persistent.get(key) {
            Some(value) => {
                // Send the data to the requestor
                _ = reply_message(
                    PaxosMessage::RecoveryReply {
                        index: self.cluster_index,
                        payload: value,
                        source: self.address.to_string(),
                    },
                    stream,
                )
                .await;
                println!("Sent data request to follower at {}", src_addr);
            }
            None => {
                println!("No value found");
            }
        }
    }

    pub async fn broadcast_accept(&self, follower_list: &Vec<String>) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 1;
        let mut tasks = JoinSet::new();
        let source = Arc::new(self.address.clone());

        for follower_addr in follower_list.iter() {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let source = Arc::clone(&source);
            let follower_addr = follower_addr.clone();
            let request_id = self.request_id.clone();

            tasks.spawn(async move {
                // Send the request to the follower
                let stream = send_message(
                    PaxosMessage::LeaderAccepted {
                        request_id: request_id,
                        source: source.to_string(),
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
                match timeout(Duration::from_secs(2), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
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

    pub async fn broadcast_learn_ec(
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
        let source = Arc::new(self.address.clone());

        for (index, follower_addr) in follower_list.iter().enumerate() {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let follower_addr = follower_addr.clone();
            let source = Arc::clone(&source);
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
                let stream = send_message(
                    PaxosMessage::LeaderLearn {
                        request_id,
                        operation: sent_operation,
                        source: source.to_string(),
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
                match timeout(Duration::from_secs(2), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
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

    pub async fn broadcast_learn_replication(
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
        let source = Arc::new(self.address.clone());

        for follower_addr in follower_list {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let follower_addr = follower_addr.clone();
            let request_id = self.request_id.clone();
            let source = Arc::clone(&source);
            let operation = operation.clone();

            tasks.spawn(async move {
                // Send the request to the follower
                let stream = send_message(
                    PaxosMessage::LeaderLearn {
                        request_id,
                        operation,
                        source: source.to_string(),
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
                match timeout(Duration::from_secs(2), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
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
    ) -> Option<Vec<Option<Vec<u8>>>> {
        let ec = match &self.ec {
            Some(ec) => ec,
            None => {
                println!("[ERROR] Leader address is not set");
                return None;
            }
        };

        let recovery_shards = Arc::new(RwLock::new(vec![None; ec.shard_count + ec.parity_count]));
        recovery_shards.write().await[self.cluster_index] = own_shard.clone();

        let size = follower_list.len();
        let response_count = Arc::new(AtomicUsize::new(1));
        let required_count = ec.shard_count;
        let notify = Arc::new(Notify::new());
        let source = Arc::new(self.address.clone());

        let mut tasks = JoinSet::new();

        for follower_addr in follower_list.iter() {
            let key = key.to_string();
            let follower_addr = follower_addr.clone();
            let notify = Arc::clone(&notify);
            let recovery_shards = Arc::clone(&recovery_shards);
            let response_count = Arc::clone(&response_count);
            let source = Arc::clone(&source);

            tasks.spawn(async move {
                let stream = match send_message(
                    PaxosMessage::RecoveryRequest {
                        key,
                        source: source.to_string(),
                    },
                    follower_addr.as_str(),
                )
                .await
                {
                    Ok(stream) => stream,
                    Err(_e) => {
                        println!(
                            "Failed to broadcast request to follower at {}",
                            follower_addr
                        );
                        return;
                    }
                };
                // println!("Broadcasted request to follower at {}", follower_addr);

                match timeout(Duration::from_secs(2), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
                        if let PaxosMessage::RecoveryReply {
                            index,
                            payload,
                            source: _,
                        } = ack
                        {
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
        Some(recovery_shards.clone())
    }
}
