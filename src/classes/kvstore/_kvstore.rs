use std::sync::Arc;

use tracing::instrument;

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
    pub fn new(db_path: &str, memcached_url: &str, tlog_path: &str, ec: Arc<ECService>) -> Self {
        KvStoreModule {
            persistent: KvPersistent::new(db_path),
            memory: KvMemory::new(memcached_url),
            transaction_log: KvTransactionLog::new(tlog_path),
            ec,
        }
    }

    pub async fn initialize(&mut self) {
        self.transaction_log.initialize().await;
    }

    #[instrument(skip_all)]
    pub async fn get_from_cluster(
        &self,
        key: &String,
        node: &Node,
    ) -> Result<Option<String>, reed_solomon_erasure::Error> {
        let result: String;
        let ec = self.ec.clone();
        if !ec.active {
            // _TODO: Inactive EC service
            println!("[ERROR] EC service is inactive");
            return Ok(None);
        }

        match self.persistent.get(key) {
            Some(value) => {
                let follower_list: Vec<String> = {
                    let followers_guard = node.cluster_list.lock().await;
                    followers_guard.iter().cloned().collect()
                };

                let recovery = node
                    .broadcast_get_shards(&follower_list, &Some(value), key)
                    .await;

                let mut recovery: Vec<Option<Vec<u8>>> = match recovery {
                    Some(recovery) => recovery,
                    None => {
                        println!("[ERROR] Failed to recover shards");
                        return Ok(None);
                    }
                };

                // for ele in recovery.clone() {
                //     println!("Shards: {:?}", ele.unwrap_or_default());
                // }

                let reconstructed_data = match ec.reconstruct(&mut recovery) {
                    Ok(data) => data,
                    Err(e) => {
                        println!("[ERROR] Failed to reconstruct shards: {:?}", e);
                        return Ok(None);
                    }
                };

                result = String::from_utf8(reconstructed_data)
                    .map_err(|_e| reed_solomon_erasure::Error::InvalidIndex)?;
            }
            None => {
                // _TODO: Handle partially missing shard
                println!("No value found");
                return Ok(None);
            }
        }
        self.memory.set(key, &result);

        Ok(Some(result))
    }
}
