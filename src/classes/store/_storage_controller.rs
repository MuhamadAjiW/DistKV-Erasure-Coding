use crate::{
    base_libs::_operation::{Operation, OperationType},
    classes::node::{_node::Node, paxos::_paxos::PaxosState},
};

use super::{_memory_store::MemoryStore, _persistent_store::PersistentStore};

// _NOTE: Storage Controller should only be used inside node that owns it
// It may cause memory problems otherwise
pub struct StorageController<'a> {
    pub persistent: PersistentStore,
    pub memory: MemoryStore,
    pub node: Option<&'a mut Node>, // Dependency injection 'a
}

impl<'a> StorageController<'a> {
    pub fn new(db_path: &str, memcached_url: &str, node: Option<&'a mut Node>) -> Self {
        StorageController {
            persistent: PersistentStore::new(db_path),
            memory: MemoryStore::new(memcached_url),
            node,
        }
    }

    pub fn assign_node(&mut self, node: &'a mut Node) {
        self.node = Some(node);
    }

    pub async fn process_request(&mut self, request: &Operation) -> Option<String> {
        let mut response: Option<String> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.memory.get(&request.kv.key);
                if response.is_none() {
                    let recovered = self.get_from_cluster(&request.kv.key).await;
                    response = Some(recovered.unwrap_or_default());
                }
            }
            OperationType::SET | OperationType::DELETE => {
                let node = match &self.node {
                    Some(n) => n,
                    None => return None,
                };

                match node.state {
                    PaxosState::Follower => {
                        let serialized_request = bincode::serialize(request).unwrap();
                        node.forward_to_leader(serialized_request).await;
                        response = Some("FORWARDED".to_string());
                    }
                    PaxosState::Leader => {
                        response = self.update_value(&request).await;
                    }
                }
            }
            _ => {}
        }

        response
    }
}

// EC logic
impl<'a> StorageController<'a> {
    pub async fn get_from_cluster(
        &mut self,
        key: &String,
    ) -> Result<String, reed_solomon_erasure::Error> {
        let mut result: String = String::new();
        let node = match self.node {
            Some(ref n) => n,
            None => return Ok(String::new()),
        };

        match self.persistent.get(key) {
            Some(value) => {
                let follower_list: Vec<String> = {
                    let followers_guard = node.cluster_list.lock().unwrap();
                    followers_guard.iter().cloned().collect()
                };

                let mut recovery: Vec<Option<Vec<u8>>> = node
                    .broadcast_get_shards(&follower_list, &Some(value), key)
                    .await;

                for ele in recovery.clone() {
                    println!("Shards: {:?}", ele.unwrap_or_default());
                }

                node.ec.reconstruct(&mut recovery)?;

                result = recovery
                    .iter()
                    .take(node.ec.shard_count)
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

        Ok(result)
    }

    pub async fn update_value(&mut self, operation: &Operation) -> Option<String> {
        let node = match self.node {
            Some(ref mut n) => n,
            None => return None,
        };

        let follower_list: Vec<String> = {
            let followers_guard = node.cluster_list.lock().unwrap();
            followers_guard.iter().cloned().collect()
        };

        let majority = follower_list.len() / 2 + 1;
        let mut acks = node.broadcast_prepare(&follower_list).await;

        if acks >= majority {
            self.memory.process_request(&operation);
            acks = node.broadcast_accept(&follower_list, operation).await;

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
