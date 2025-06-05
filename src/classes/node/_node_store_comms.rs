use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::{
    sync::{Notify, RwLock},
    task::JoinSet,
};
use tracing::{error, info, instrument, warn};

use crate::{
    base_libs::{
        _operation::{BinKV, Operation, OperationType},
        _paxos_types::PaxosMessage,
        network::_messages::{send_message, send_message_and_receive_response},
    },
    classes::node::paxos::_paxos_state::PaxosState,
};

use super::_node::Node;

impl Node {
    #[instrument(level = "debug", skip_all)]
    pub async fn handle_client_request(&mut self, src_addr: &String, operation: Operation) {
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

        let reply = PaxosMessage::ClientReply {
            response: result.clone(),
            source: self.address.to_string(),
        };
        let _ = send_message(reply, src_addr, &self.connection_manager).await;
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn handle_recovery_request(&self, src_addr: &String, key: &str) {
        match self.store.persistent.get(key) {
            Some(value) => {
                let reply = PaxosMessage::RecoveryReply {
                    index: self.cluster_index,
                    payload: value,
                    source: self.address.to_string(),
                };
                let _ = send_message(reply, src_addr, &self.connection_manager).await;
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
        let conn_mgr = Arc::clone(&self.connection_manager);

        for follower_addr in follower_list.iter() {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let source = Arc::clone(&source);
            let follower_addr = follower_addr.clone();
            let request_id = self.request_id.clone();
            let epoch = Arc::clone(&epoch);
            let conn_mgr_clone = Arc::clone(&conn_mgr);

            tasks.spawn(async move {
                match send_message_and_receive_response(
                    PaxosMessage::LearnRequest {
                        epoch: (*epoch).clone(),
                        commit_id: request_id,
                        source: source.to_string(),
                    },
                    &follower_addr,
                    &conn_mgr_clone,
                )
                .await
                {
                    Ok(PaxosMessage::Ack { .. }) => return Some(1),
                    Ok(_) => return Some(0),
                    Err(e) => {
                        error!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
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
        let conn_mgr = Arc::clone(&self.connection_manager);

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
            let conn_mgr_clone = Arc::clone(&conn_mgr);

            tasks.spawn(async move {
                let sent_shard = encoded_shard_arc[index].clone();
                let sent_operation = Operation {
                    op_type: (*op_type).clone(),
                    kv: BinKV {
                        key: (*key).clone(),
                        value: sent_shard,
                    },
                };

                match send_message_and_receive_response(
                    PaxosMessage::AcceptRequest {
                        epoch: (*epoch).clone(),
                        request_id: (*commit_id).clone(),
                        operation: sent_operation,
                        source: (*source).clone(),
                    },
                    follower_addr.as_str(),
                    &conn_mgr_clone,
                )
                .await
                {
                    Ok(PaxosMessage::Ack { .. }) => return Some(1),
                    Ok(_) => return Some(0),
                    Err(e) => {
                        error!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
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
        let conn_mgr = Arc::clone(&self.connection_manager);

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
            let conn_mgr_clone = Arc::clone(&conn_mgr);

            tasks.spawn(async move {
                match send_message_and_receive_response(
                    PaxosMessage::AcceptRequest {
                        epoch: (*epoch).clone(),
                        request_id: (*commit_id).clone(),
                        operation: (*operation).clone(),
                        source: source.to_string(),
                    },
                    follower_addr.as_str(),
                    &conn_mgr_clone,
                )
                .await
                {
                    Ok(PaxosMessage::Ack { .. }) => return Some(1),
                    Ok(_) => return Some(0),
                    Err(e) => {
                        error!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
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
        {
            recovery_shards.write().await[self.cluster_index] = own_shard.clone();
        }

        let size = follower_list.len();
        let response_count = Arc::new(AtomicUsize::new(1));
        let required_count = ec.data_shard_count;
        let notify = Arc::new(Notify::new());
        let source = Arc::new(self.address.clone());
        let conn_mgr = Arc::clone(&self.connection_manager);

        let mut tasks = JoinSet::new();

        for follower_addr in follower_list.iter() {
            let key = key.to_string();
            let follower_addr = follower_addr.clone();
            let notify = Arc::clone(&notify);
            let recovery_shards = Arc::clone(&recovery_shards);
            let response_count = Arc::clone(&response_count);
            let source = Arc::clone(&source);
            let conn_mgr_clone = Arc::clone(&conn_mgr);

            tasks.spawn(async move {
                match send_message_and_receive_response(
                    PaxosMessage::RecoveryRequest {
                        key: key.clone(),
                        source: source.to_string(),
                    },
                    follower_addr.as_str(),
                    &conn_mgr_clone,
                )
                .await
                {
                    Ok(PaxosMessage::RecoveryReply { index, payload, .. }) => {
                        if index < size {
                            let mut shards = recovery_shards.write().await;
                            shards[index] = Some(payload);
                            let count = response_count.fetch_add(1, Ordering::SeqCst);
                            if count >= required_count {
                                notify.notify_one();
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Failed to broadcast request to follower at {}: {}",
                            follower_addr, e
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
        let conn_mgr = Arc::clone(&self.connection_manager);
        match send_message_and_receive_response(
            PaxosMessage::RecoveryRequest {
                key,
                source: self.address.to_string(),
            },
            follower_addr.as_str(),
            &conn_mgr,
        )
        .await
        {
            Ok(PaxosMessage::RecoveryReply { payload, .. }) => Some(payload),
            Ok(other) => {
                error!(
                    "Unexpected message type from follower at {}: {:?}",
                    follower_addr, other
                );
                None
            }
            Err(e) => {
                error!(
                    "Failed to broadcast request to follower at {}: {}",
                    follower_addr, e
                );
                None
            }
        }
    }
}
