use tracing::{info, instrument, error};

use crate::base_libs::{
    _operation::{BinKV, Operation, OperationType},
    _paxos_types::PaxosMessage,
};

use super::{_node::Node, paxos::_paxos_state::PaxosState};

impl Node {
    #[instrument(level = "trace", skip_all)]
    pub async fn process_request(
        &mut self,
        request: &Operation,
        request_id: u64,
    ) -> Option<String> {
        let mut response: Option<String> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.store.memory.get(&request.kv.key);
                if response.is_none() {
                    let recovered = self.store.get_from_cluster(&request.kv.key, self).await;
                    response = recovered.unwrap_or_default();
                }
            }
            OperationType::SET | OperationType::DELETE => match self.state {
                PaxosState::Follower | PaxosState::Candidate => {
                    info!(
                        "[FORWARD] Forwarding request to leader: {}",
                        request.to_string()
                    );
                    self.forward_to_leader(PaxosMessage::ClientRequest {
                        operation: request.clone(),
                        source: self.address.to_string(),
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

        self.store.transaction_log.append(&request).await;

        response
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn accept_value(&mut self, operation: &Operation, commit_id: u64) -> bool {
        let follower_list: Vec<String> = {
            let followers_guard = self.cluster_list.lock().await;
            followers_guard.iter().cloned().collect()
        };

        let majority = follower_list.len() / 2 + 1;
        let acks;

        // _TODO: Delete operation is still broken here
        self.store.memory.process_request(&operation);
        if self.store.ec.active {
            let ec = self.store.ec.clone();
            let encoded_shard = ec.encode(&operation.kv.value);
            self.store.persistent.process_request(&Operation {
                op_type: operation.op_type.clone(),
                kv: BinKV {
                    key: operation.kv.key.clone(),
                    value: encoded_shard[self.cluster_index].clone(),
                },
            });

            acks = self
                .broadcast_accept_ec(&follower_list, operation, &encoded_shard, commit_id)
                .await
        } else {
            self.store.persistent.process_request(operation);
            acks = self
                .broadcast_accept_replication(&follower_list, operation, commit_id)
                .await
        }

        if acks < majority {
            error!("Request failed: Accept broadcast is not accepted by majority");
            return false;
        }

        // _NOTE: Check log synchronization safety, this could block the whole operation
        self.synchronize_log(commit_id - 1).await;
        self.store.transaction_log.append(operation).await;

        info!("Request succeeded: Accept broadcast is accepted by majority");
        return true;
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn learn_value(&self, node: &Node) -> bool {
        let follower_list: Vec<String> = {
            let followers_guard = node.cluster_list.lock().await;
            followers_guard.iter().cloned().collect()
        };

        let majority = follower_list.len() / 2 + 1;
        let acks = node.broadcast_accept(&follower_list).await;

        if acks < majority {
            error!("Request failed: Prepare broadcast is not accepted by majority");
            return false;
        }

        info!("Request succeeded: Accept broadcast is accepted by majority");
        return true;
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
