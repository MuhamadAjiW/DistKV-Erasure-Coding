use tracing::{info, instrument};

use crate::base_libs::{
    _operation::{BinKV, Operation, OperationType},
    _paxos_types::PaxosMessage,
};

use super::{_node::Node, paxos::_paxos_state::PaxosState};

impl Node {
    #[instrument(level = "debug", skip_all)]
    pub async fn process_request(&self, request: &Operation, request_id: u64) -> Option<String> {
        let mut response: Option<String> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.store.memory.get(&request.kv.key).await;
                if response.is_none() {
                    let recovered = self.store.get_from_cluster(&request.kv.key, self).await;
                    response = recovered.unwrap_or_default();
                }
            }
            OperationType::SET | OperationType::DELETE => match self.state {
                PaxosState::Follower | PaxosState::Candidate => {
                    info!("[FORWARD] Forwarding request to leader");
                    self.forward_to_leader(PaxosMessage::ClientRequest {
                        operation: request.clone(),
                        source: self.address.to_string(),
                        transaction_id: None,
                    })
                    .await;
                    response = Some("FORWARDED".to_string());
                }
                PaxosState::Leader => {
                    // TODO: Separate accept and learn
                    if self.accept_value(&request, request_id).await {
                        response = Some("OK".to_string());
                    } else {
                        response = Some("FAILED".to_string());
                    }
                }
            },
            _ => {}
        }

        response
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn accept_value(&self, operation: &Operation, commit_id: u64) -> bool {
        // Collect follower list under lock
        let follower_list: Vec<String> = {
            let followers_guard = self.cluster_list.read().await;
            followers_guard.iter().cloned().collect()
        };
        let majority = follower_list.len() / 2 + 1;

        // Apply to memory store (write lock if needed, else direct)
        self.store.memory.process_request(operation).await;

        // Prepare persistent op and broadcast tasks
        let acks = if self.store.ec.active {
            let ec = self.store.ec.clone();
            let encoded_shard = ec.encode(&operation.kv.value);
            self.store.persistent.process_request(&Operation {
                op_type: operation.op_type.clone(),
                kv: BinKV {
                    key: operation.kv.key.clone(),
                    value: encoded_shard[self.cluster_index].clone(),
                },
            });
            // Broadcast after all locks released
            self.broadcast_accept_ec(&follower_list, operation, &encoded_shard, commit_id)
                .await
        } else {
            self.store.persistent.process_request(operation);
            self.broadcast_accept_replication(&follower_list, operation, commit_id)
                .await
        };

        if acks < majority {
            return false;
        }
        true
    }

    pub async fn learn_value(&self, node: &Node) -> bool {
        // Collect follower list under lock
        let follower_list: Vec<String> = {
            let followers_guard = node.cluster_list.read().await;
            followers_guard.iter().cloned().collect()
        };
        let majority = follower_list.len() / 2 + 1;
        // Broadcast after lock released
        let acks = node.broadcast_accept(&follower_list).await;
        if acks < majority {
            return false;
        }
        true
    }

    pub async fn synchronize_log(&mut self, new_commit_id: u64) {
        let old_commit_id = self.commit_id;
        self.commit_id = std::cmp::max(self.commit_id, new_commit_id);

        if self.commit_id > old_commit_id {
            info!(
                "[ELECTION] Node updated commit ID from {} to {}",
                old_commit_id, self.commit_id
            );
        }

        // _TODO: Synchronize the transaction log with the new commit ID
        // self.store.synchronize(self.commit_id).await;
    }
}
