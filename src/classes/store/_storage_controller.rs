use tracing::instrument;

use crate::classes::node::_node::Node;

use super::{
    _memory_store::MemoryStore, _persistent_store::PersistentStore,
    _transaction_log::TransactionLog,
};

pub struct StorageController {
    pub persistent: PersistentStore,
    pub memory: MemoryStore,
    pub transaction_log: TransactionLog,
}

impl StorageController {
    pub fn new(db_path: &str, memcached_url: &str, tlog_path: &str) -> Self {
        StorageController {
            persistent: PersistentStore::new(db_path),
            memory: MemoryStore::new(memcached_url),
            transaction_log: TransactionLog::new(tlog_path),
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
        let ec = match &node.ec {
            Some(ec) => ec,
            None => {
                // TODO: replication?
                println!("[ERROR] EC service is not active");
                return Ok(None);
            }
        };

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

                for ele in recovery.clone() {
                    println!("Shards: {:?}", ele.unwrap_or_default());
                }

                if let Err(e) = ec.reconstruct(&mut recovery) {
                    println!("[ERROR] Failed to reconstruct shards: {:?}", e);
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
                return Ok(None);
            }
        }
        self.memory.set(key, &result);

        Ok(Some(result))
    }
}
