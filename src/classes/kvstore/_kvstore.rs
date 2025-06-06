use std::sync::Arc;

use tracing::{error, info, instrument};

use crate::classes::{ec::_ec::ECService, node::_node::Node};

use super::{
    _kvstore_memory::KvMemory, _kvstore_persistent::KvPersistent,
    _kvstore_transaction_log::KvTransactionLog,
};

pub struct KvStoreModule {
    pub persistent: KvPersistent,
    pub memory: KvMemory,
    pub transaction_log: KvTransactionLog,
    pub ec: Arc<ECService>,
}

impl KvStoreModule {
    pub async fn new(db_path: &str, tlog_path: &str, ec: Arc<ECService>) -> Self {
        KvStoreModule {
            persistent: KvPersistent::new(db_path),
            memory: KvMemory::new().await,
            transaction_log: KvTransactionLog::new(tlog_path),
            ec,
        }
    }

    pub async fn initialize(&mut self) {
        self.transaction_log.initialize().await;
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn get_from_cluster(
        &self,
        key: &String,
        node: &Node,
    ) -> Result<Option<String>, reed_solomon_erasure::Error> {
        let result: String;
        let ec = self.ec.clone();
        if !ec.active {
            match self.persistent.get(key) {
                Some(value) => {
                    result = String::from_utf8(value)
                        .map_err(|_e| reed_solomon_erasure::Error::InvalidIndex)?;

                    return Ok(Some(result));
                }
                None => {
                    let follower_list: Vec<String> = {
                        let followers_guard = node.cluster_list.read().await;
                        followers_guard.iter().cloned().collect()
                    };

                    for follower_address in follower_list {
                        match node.request_replicated_data(&follower_address, key).await {
                            Some(value) => {
                                let str_value = String::from_utf8(value.clone())
                                    .map_err(|_e| reed_solomon_erasure::Error::InvalidIndex)?;

                                self.memory.set(key, &value).await;
                                self.persistent.set(key, &value);
                                return Ok(Some(str_value));
                            }
                            None => {
                                info!(
                                    "[INFO] Follower {} did not have key {}",
                                    follower_address, key
                                );
                                continue; // Try the next follower
                            }
                        }
                    }

                    error!("[ERROR] Key {} not found on any replica.", key);
                    return Ok(None);
                }
            }
        }

        let self_shard: Option<Vec<u8>> = self.persistent.get(key);

        let follower_list: Vec<String> = {
            let followers_guard = node.cluster_list.read().await;
            followers_guard.iter().cloned().collect()
        };

        let recovery = node
            .broadcast_get_shards(&follower_list, &self_shard, key)
            .await;

        let mut recovery: Vec<Option<Vec<u8>>> = match recovery {
            Some(recovery) => recovery,
            None => {
                error!("[ERROR] Failed to recover shards");
                return Ok(None);
            }
        };

        let reconstructed_data = match ec.reconstruct(&mut recovery) {
            Ok(data) => data,
            Err(e) => {
                error!("[ERROR] Failed to reconstruct shards: {:?}", e);
                return Ok(None);
            }
        };

        self.memory.set(key, &reconstructed_data).await;

        result = String::from_utf8(reconstructed_data)
            .map_err(|_e| reed_solomon_erasure::Error::InvalidIndex)?;

        Ok(Some(result))
    }
}
