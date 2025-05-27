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
use tracing::{error, info, instrument, warn};

use crate::{
    base_libs::{
        _operation::{BinKV, Operation, OperationType},
        _paxos_types::PaxosMessage,
        network::_messages::{receive_message, reply_message, reply_string, send_message},
    },
    classes::node::paxos::_paxos_state::PaxosState,
};

use super::_node::Node;

impl Node {
    #[instrument(level = "debug", skip_all)]
    pub async fn handle_client_request(
        &mut self,
        _src_addr: &String,
        stream: TcpStream,
        operation: Operation,
    ) {
        let result: String;

        match operation.op_type {
            OperationType::BAD => {
                warn!("[REQUEST] Invalid request");
                result = "Invalid request".to_string();
            }
            OperationType::PING => {
                info!("[REQUEST] Received PING request");
                result = "PONG".to_string();
            }
            OperationType::GET | OperationType::DELETE | OperationType::SET => {
                if self.state == PaxosState::Leader {
                    self.request_id += 1;
                }

                info!("[REQUEST] Received request: {:?}", operation.op_type);
                result = self
                    .process_request(&operation, self.request_id)
                    .await
                    .unwrap_or_default();
            }
        }

        _ = reply_string(result.as_str(), stream).await;
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn handle_recovery_request(&self, src_addr: &String, stream: TcpStream, key: &str) {
        match self.store.persistent.get(key) {
            Some(value) => {
                _ = reply_message(
                    PaxosMessage::RecoveryReply {
                        index: self.cluster_index,
                        payload: value,
                        source: self.address.to_string(),
                    },
                    stream,
                )
                .await;
                info!("Sent data request to follower at {}", src_addr);
            }
            None => {
                warn!("No value found");
            }
        }
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn broadcast_accept(&self, follower_list: &Vec<String>) -> usize {
        if follower_list.is_empty() {
            error!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 1;
        let mut tasks = JoinSet::new();
        let source = Arc::new(self.address.clone());
        let epoch = Arc::new(self.epoch.clone());

        for follower_addr in follower_list.iter() {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let source = Arc::clone(&source);
            let follower_addr = follower_addr.clone();
            let request_id = self.request_id.clone();
            let epoch = Arc::clone(&epoch);

            tasks.spawn(async move {
                // Send the request to the follower
                let stream = send_message(
                    PaxosMessage::LearnRequest {
                        epoch: (*epoch).clone(),
                        commit_id: request_id,
                        source: source.to_string(),
                    },
                    &follower_addr,
                )
                .await
                .unwrap();

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(10), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
                        if let PaxosMessage::Ack { .. } = ack {
                            return Some(1);
                        }
                    }
                    Ok(Err(e)) => {
                        error!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        error!(
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

    #[instrument(level = "debug", skip_all)]
    pub async fn broadcast_accept_ec(
        &self,
        follower_list: &Vec<String>,
        operation: &Operation,
        encoded_shard: &Vec<Vec<u8>>,
        commit_id: u64,
    ) -> usize {
        if follower_list.is_empty() {
            error!("No followers registered. Cannot proceed.");
            return 0;
        }

        let majority = follower_list.len() / 2 + 1;
        let mut acks = 1;

        let mut tasks = JoinSet::new();

        // Share as much as possible and clone as little as possible
        let source = Arc::new(self.address.clone().to_string());
        let leader_addr = self.address.to_string();
        let commit_id = Arc::new(commit_id);
        let epoch = Arc::new(self.epoch.clone());
        let shared_op_type = Arc::new(operation.op_type.clone());
        let shared_key = Arc::new(operation.kv.key.clone());
        let shared_encoded_shards = Arc::new(encoded_shard.clone());

        for (index, follower_addr) in follower_list.iter().enumerate() {
            if follower_addr == &leader_addr {
                continue;
            }

            let follower_addr = follower_addr.clone();
            let source = Arc::clone(&source);
            let commit_id = Arc::clone(&commit_id);
            let epoch = Arc::clone(&epoch);
            let op_type = Arc::clone(&shared_op_type);
            let key = Arc::clone(&shared_key);
            let encoded_shard_arc = Arc::clone(&shared_encoded_shards);

            tasks.spawn(async move {
                let sent_shard = encoded_shard_arc[index].clone();
                let sent_operation = Operation {
                    op_type: (*op_type).clone(),
                    kv: BinKV {
                        key: (*key).clone(),
                        value: sent_shard,
                    },
                };

                // Send the request to the follower
                let stream = send_message(
                    PaxosMessage::AcceptRequest {
                        epoch: (*epoch).clone(),
                        request_id: (*commit_id).clone(),
                        operation: sent_operation,
                        source: (*source).clone(),
                    },
                    follower_addr.as_str(),
                )
                .await
                .unwrap();

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(10), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
                        if let PaxosMessage::Ack { .. } = ack {
                            return Some(1);
                        } else {
                            return Some(0);
                        }
                    }
                    Ok(Err(e)) => {
                        error!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
                        );
                        return Some(0);
                    }
                    Err(_) => {
                        warn!(
                            "Timeout waiting for acknowledgment from follower at {}",
                            follower_addr
                        );
                        return Some(0);
                    }
                }
            });
        }

        while let Some(response) = tasks.join_next().await {
            match response {
                Ok(Some(value)) => {
                    acks += value;
                    if acks >= majority {
                        // TODO: Reconsider. This don't seem to be a good idea? What if some haven't received the packages?
                        tasks.abort_all();
                        break;
                    }
                }
                _ => {} // Handle errors or None responses if needed
            }
        }

        acks
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn broadcast_accept_replication(
        &self,
        follower_list: &Vec<String>,
        operation: &Operation,
        commit_id: u64,
    ) -> usize {
        if follower_list.is_empty() {
            error!("No followers registered. Cannot proceed.");
            return 0;
        }

        let majority = follower_list.len() / 2 + 1;
        let mut acks = 1;

        let mut tasks = JoinSet::new();

        let source = Arc::new(self.address.clone().to_string());
        let leader_addr = self.address.to_string();
        let commit_id = Arc::new(commit_id);
        let operation = Arc::new(operation.clone());

        for follower_addr in follower_list {
            if follower_addr == &leader_addr {
                continue;
            }

            let follower_addr = follower_addr.clone();
            let source = Arc::clone(&source);
            let commit_id = Arc::clone(&commit_id);
            let epoch = Arc::new(self.epoch.clone());
            let source = Arc::clone(&source);
            let operation = Arc::clone(&operation);

            tasks.spawn(async move {
                // Send the request to the follower
                let stream = send_message(
                    PaxosMessage::AcceptRequest {
                        epoch: (*epoch).clone(),
                        request_id: (*commit_id).clone(),
                        operation: (*operation).clone(),
                        source: source.to_string(),
                    },
                    follower_addr.as_str(),
                )
                .await
                .unwrap();

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(10), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
                        if let PaxosMessage::Ack { .. } = ack {
                            return Some(1);
                        }
                    }
                    Ok(Err(e)) => {
                        error!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        warn!(
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
                    if acks >= majority {
                        // TODO: Reconsider. This don't seem to be a good idea? What if some haven't received the packages?
                        tasks.abort_all();
                        break;
                    }
                }
                _ => {} // Handle errors or None responses if needed
            }
        }

        acks
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn broadcast_get_shards(
        &self,
        follower_list: &Vec<String>,
        own_shard: &Option<Vec<u8>>,
        key: &str,
    ) -> Option<Vec<Option<Vec<u8>>>> {
        let ec = self.store.ec.clone();
        let recovery_shards = Arc::new(RwLock::new(vec![
            None;
            ec.data_shard_count
                + ec.parity_shard_count
        ]));
        recovery_shards.write().await[self.cluster_index] = own_shard.clone();

        let size = follower_list.len();
        let response_count = Arc::new(AtomicUsize::new(1));
        let required_count = ec.data_shard_count;
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
                        error!(
                            "Failed to broadcast request to follower at {}",
                            follower_addr
                        );
                        return;
                    }
                };

                match timeout(Duration::from_secs(10), receive_message(stream)).await {
                    Ok(Ok((_stream, ack))) => {
                        if let PaxosMessage::RecoveryReply {
                            index,
                            payload,
                            source: _,
                        } = ack
                        {
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
                        error!(
                            "Error receiving recovery response from {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        error!(
                            "Timeout waiting for recovery response from {}",
                            follower_addr
                        );
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

    #[instrument(level = "debug", skip_all)]
    pub async fn request_replicated_data(
        &self,
        follower_addr: &String,
        key: &str,
    ) -> Option<Vec<u8>> {
        let key = key.to_string();

        let stream = match send_message(
            PaxosMessage::RecoveryRequest {
                key,
                source: self.address.to_string(),
            },
            follower_addr.as_str(),
        )
        .await
        {
            Ok(stream) => stream,
            Err(_e) => {
                error!(
                    "Failed to broadcast request to follower at {}",
                    follower_addr
                );
                return None;
            }
        };

        match timeout(Duration::from_secs(10), receive_message(stream)).await {
            Ok(Ok((_stream, ack))) => {
                if let PaxosMessage::RecoveryReply {
                    index: _,
                    payload,
                    source: _,
                } = ack
                {
                    return Some(payload);
                } else {
                    error!(
                        "Unexpected message type from follower at {}: {:?}",
                        follower_addr, ack
                    );
                    return None;
                }
            }
            Ok(Err(e)) => {
                error!(
                    "Error receiving recovery response from {}: {}",
                    follower_addr, e
                );
                return None;
            }
            Err(_) => {
                warn!(
                    "Timeout waiting for recovery response from {}",
                    follower_addr
                );
                return None;
            }
        }
    }
}
