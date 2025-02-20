use std::{sync::Arc, time::Duration};

use tokio::{task::JoinSet, time::timeout};

use crate::base_libs::{
    _operation::{BinKV, Operation},
    _paxos_types::PaxosMessage,
    network::_messages::{receive_message, send_message},
};

use super::_node::Node;

impl Node {
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
}
