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

use crate::{
    base_libs::{
        _operation::{BinKV, Operation, OperationType},
        _paxos_types::PaxosMessage,
        network::_messages::{receive_message, send_message},
    },
    classes::node::{_node::Node, paxos::_paxos::PaxosState},
};

use super::{_memory_store::MemoryStore, _persistent_store::PersistentStore};

// _NOTE: Storage Controller should only be used inside node that owns it
// It may cause memory problems otherwise
pub struct StorageController<'a> {
    pub persistent: PersistentStore,
    pub memory: MemoryStore,
    pub node: Option<&'a Node>, // Dependency injection 'a
}

impl<'a> StorageController<'a> {
    pub fn new(db_path: &str, memcached_url: &str, node: Option<&'a Node>) -> Self {
        StorageController {
            persistent: PersistentStore::new(db_path),
            memory: MemoryStore::new(memcached_url),
            node,
        }
    }

    pub fn assign_node(&mut self, node: &'a Node) {
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
                let node = match self.node {
                    Some(n) => n,
                    None => return None,
                };

                match node.state {
                    PaxosState::Follower => {
                        let serialized_request = bincode::serialize(request).unwrap();
                        node.forward_to_leader(serialized_request).await;
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

                let mut recovery: Vec<Option<Vec<u8>>> = self
                    .broadcast_get_value(&follower_list, &Some(value), key)
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
                println!("No value found");
            }
        }
        self.memory.set(key, &result);

        Ok(result)
    }

    async fn broadcast_get_value(
        &self,
        follower_list: &Vec<String>,
        own_shard: &Option<Vec<u8>>,
        key: &str,
    ) -> Vec<Option<Vec<u8>>> {
        let node = match self.node {
            Some(ref n) => n,
            None => return vec![],
        };

        let recovery_shards = Arc::new(RwLock::new(vec![
            None;
            node.ec.shard_count + node.ec.parity_count
        ]));
        recovery_shards.write().await[node.cluster_index] = own_shard.clone();

        let size = follower_list.len();
        let response_count = Arc::new(AtomicUsize::new(1));
        let required_count = node.ec.shard_count;
        let notify = Arc::new(Notify::new());

        let mut tasks = JoinSet::new();

        for follower_addr in follower_list.iter() {
            let socket = Arc::clone(&node.socket);
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

    pub async fn update_value(&mut self, operation: &Operation) -> Option<String> {
        let node = match self.node {
            Some(ref n) => n,
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

            if node.ec_active {
                let encoded_shard = node.ec.encode(&operation.kv.value);
                self.persistent.process_request(&Operation {
                    op_type: operation.op_type.clone(),
                    kv: BinKV {
                        key: operation.kv.key.clone(),
                        value: encoded_shard[node.cluster_index].clone(),
                    },
                });

                acks = node
                    .broadcast_accept_ec(&follower_list, &operation, &encoded_shard)
                    .await;
            } else {
                acks = node
                    .broadcast_accept_replication(&follower_list, &operation)
                    .await;
            }

            if acks >= majority {
                println!("Request succeeded: Accept broadcast is accepted by majority");
            } else {
                println!("Request failed: Accept broadcast is not accepted by majority");
            }
        } else {
            println!("Request failed: Prepare broadcast is not accepted by majority");
        }

        return Some("".to_string());
    }
}
