use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::Duration};

use tokio::{sync::{Notify, RwLock}, task::JoinSet, time::timeout};

use crate::base_libs::{_paxos_types::PaxosMessage, network::_messages::{receive_message, send_message}};

use super::_node::Node;

impl Node {
    pub async fn get_from_cluster(
        &mut self,
        wal_path: &String,
        key: &String,
    ) -> Result<String, reed_solomon_erasure::Error> {
        let mut result: String = String::new();
        match self.store.get_from_wal(wal_path, key).await {
            Ok(Some(value)) => {
                let follower_list: Vec<String> = {
                    let followers_guard = self.cluster_list.lock().unwrap();
                    followers_guard.iter().cloned().collect()
                };

                let mut recovery: Vec<Option<Vec<u8>>> = self
                    .broadcast_get_value(&follower_list, &Some(value), key)
                    .await;

                for ele in recovery.clone() {
                    println!("Shards: {:?}", ele.unwrap_or_default());
                }

                self.ec.reconstruct(&mut recovery)?;

                result = recovery
                    .iter()
                    .take(self.ec.shard_count)
                    .filter_map(|opt| opt.as_ref().map(|v| String::from_utf8(v.clone()).unwrap()))
                    .collect::<Vec<String>>()
                    .join("");
            }
            Ok(None) => {
                println!("No value found");
            }
            Err(e) => {
                eprintln!("Error while reading from WAL: {}", e);
            }
        }

        self.store.set(key, &result);

        Ok(result)
    }

    pub async fn handle_recovery_request(&self, src_addr: &String, wal_path: &String, key: &str) {
        match self.store.get_from_wal(wal_path, key).await {
            Ok(Some(value)) => {
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
            Ok(None) => {
                println!("No value found");
            }
            Err(e) => {
                eprintln!("Error while reading from WAL: {}", e);
            }
        }
    }

    async fn broadcast_get_value(
        &self,
        follower_list: &Vec<String>,
        own_shard: &Option<Vec<u8>>,
        key: &str,
    ) -> Vec<Option<Vec<u8>>> {
        let recovery_shards = Arc::new(RwLock::new(vec![
            None;
            self.ec.shard_count + self.ec.parity_count
        ]));
        recovery_shards.write().await[self.cluster_index] = own_shard.clone();

        let size = follower_list.len();
        let response_count = Arc::new(AtomicUsize::new(1));
        let required_count = self.ec.shard_count;
        let notify = Arc::new(Notify::new());

        let mut tasks = JoinSet::new();

        for follower_addr in follower_list.iter() {
            let socket = Arc::clone(&self.socket);
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
}
