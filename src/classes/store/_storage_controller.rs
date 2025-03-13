use crate::{
    base_libs::_operation::{BinKV, Operation, OperationType},
    classes::node::{_node::Node, paxos::_paxos::PaxosState},
};

use super::{_memory_store::MemoryStore, _persistent_store::PersistentStore};

// _NOTE: Storage Controller should only be used inside node that owns it
// It may cause memory problems otherwise
pub struct StorageController {
    pub persistent: PersistentStore,
    pub memory: MemoryStore,
}

impl StorageController {
    pub fn new(db_path: &str, memcached_url: &str) -> Self {
        StorageController {
            persistent: PersistentStore::new(db_path),
            memory: MemoryStore::new(memcached_url),
        }
    }

    pub async fn process_request(&self, request: &Operation, node: &Node) -> Option<String> {
        let mut response: Option<String> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.memory.get(&request.kv.key);
                if response.is_none() {
                    let recovered = self.get_from_cluster(&request.kv.key, node).await;
                    response = recovered.unwrap_or_default();
                }
            }
            OperationType::SET | OperationType::DELETE => {
                match node.state {
                    PaxosState::Follower => {
                        println!(
                            "[FORWARD] Forwarding request to leader: {}",
                            request.to_string()
                        );
                        // let serialized_request = bincode::serialize(request).unwrap();
                        let string_payload = request.to_string();
                        node.forward_to_leader(string_payload.into_bytes()).await;
                        response = Some("FORWARDED".to_string());
                    }
                    PaxosState::Leader => {
                        response = self.update_value(&request, node).await;
                    }
                }
            }
            _ => {}
        }

        response
    }
}

// EC logic
impl StorageController {
    pub async fn get_from_cluster(
        &self,
        key: &String,
        node: &Node,
    ) -> Result<Option<String>, reed_solomon_erasure::Error> {
        let mut result: String = String::new();
        let ec = match &node.ec {
            Some(ec) => ec,
            None => {
                println!("EC service is not active");
                return Ok(None);
            }
        };

        match self.persistent.get(key) {
            Some(value) => {
                let follower_list: Vec<String> = {
                    let followers_guard = node.cluster_list.lock().unwrap();
                    followers_guard.iter().cloned().collect()
                };

                let mut recovery = node
                    .broadcast_get_shards(&follower_list, &Some(value), key)
                    .await;

                let mut recovery: Vec<Option<Vec<u8>>> = match recovery {
                    Some(recovery) => recovery,
                    None => {
                        println!("Failed to recover shards");
                        return Ok(None);
                    }
                };

                for ele in recovery.clone() {
                    println!("Shards: {:?}", ele.unwrap_or_default());
                }

                if let Err(e) = ec.reconstruct(&mut recovery) {
                    println!("Failed to reconstruct shards: {:?}", e);
                    return Ok(None);
                }

                result = recovery
                    .iter()
                    .take(ec.shard_count)
                    .filter_map(|opt| opt.as_ref().map(|v| String::from_utf8(v.clone()).unwrap()))
                    .collect::<Vec<String>>()
                    .join("");
            }
            None => {
                // _TODO: Handle partially missing shard
                println!("No value found");
            }
        }
        self.memory.set(key, &result);

        Ok(Some(result))
    }

    pub async fn update_value(&self, operation: &Operation, node: &Node) -> Option<String> {
        let follower_list: Vec<String> = {
            let followers_guard = node.cluster_list.lock().unwrap();
            followers_guard.iter().cloned().collect()
        };

        let majority = follower_list.len() / 2 + 1;
        let mut acks = node.broadcast_prepare(&follower_list).await;

        if acks >= majority {
            self.memory.process_request(&operation);
            if node.ec_active {
                let ec = match &node.ec {
                    Some(ec) => ec,
                    None => {
                        println!("EC service is missing");
                        return None;
                    }
                };
                let encoded_shard = ec.encode(&operation.kv.value);
                self.persistent.process_request(&Operation {
                    op_type: operation.op_type.clone(),
                    kv: BinKV {
                        key: operation.kv.key.clone(),
                        value: encoded_shard[node.cluster_index].clone(),
                    },
                });

                acks = node
                    .broadcast_accept_ec(&follower_list, operation, &encoded_shard)
                    .await
            } else {
                self.persistent.process_request(operation);
                acks = node
                    .broadcast_accept_replication(&follower_list, operation)
                    .await
            }

            if acks >= majority {
                println!("Request succeeded: Accept broadcast is accepted by majority");
                return Some("OK".to_string());
            } else {
                println!("Request failed: Accept broadcast is not accepted by majority");
            }
        } else {
            println!("Request failed: Prepare broadcast is not accepted by majority");
        }

        return Some("FAILED".to_string());
    }
}
