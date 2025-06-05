use tracing::instrument;

use crate::base_libs::_operation::{Operation, OperationType};
use moka::future::Cache;

pub struct KvMemory {
    store: Cache<String, Vec<u8>>,
}

impl KvMemory {
    pub async fn new() -> Self {
        KvMemory {
            store: Cache::builder().max_capacity(100_000).build(),
        }
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn set(&self, key: &String, value: &[u8]) {
        self.store.insert(key.clone(), value.to_vec()).await;
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn get(&self, key: &String) -> Option<String> {
        self.store
            .get(key)
            .await
            .map(|v| String::from_utf8_lossy(&v).to_string())
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn remove(&self, key: &String) -> () {
        self.store.invalidate(key).await;
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn process_request(&self, request: &Operation) -> Option<String> {
        let mut response: Option<String> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.get(&request.kv.key).await;
            }
            OperationType::SET => {
                self.set(&request.kv.key, &request.kv.value).await;
            }
            OperationType::DELETE => {
                self.remove(&request.kv.key).await;
            }
            _ => {}
        }

        response
    }
}
