use bb8_memcached::{bb8::Pool, MemcacheConnectionManager};
use tracing::instrument;

use crate::base_libs::_operation::{Operation, OperationType};

pub struct KvMemory {
    pool: Pool<MemcacheConnectionManager>,
}

impl KvMemory {
    pub async fn new(memcached_url: &str) -> Self {
        let manager = MemcacheConnectionManager::new(memcached_url).unwrap();
        let pool = Pool::builder()
            // .max_size(1000) // Default maximum number of memcached connections is 1024, book 1000 of them
            // .min_idle(1000) // All connections should be taken by the server
            .build(manager)
            .await
            .unwrap();

        return KvMemory { pool };
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn set(&self, key: &String, value: &[u8]) {
        let mut conn = self.pool.get().await.unwrap();

        conn.set(key, value, 0).await.unwrap();
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn get(&self, key: &String) -> Option<String> {
        let mut conn = self.pool.get().await.unwrap();

        let result = conn.get(key).await;

        match result {
            Ok(value) => Some(String::from_utf8(value).unwrap()),
            Err(err) => {
                tracing::error!("Failed to get from memcached: {:?}", err);
                None
            }
        }
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn remove(&self, key: &String) -> () {
        let mut conn = self.pool.get().await.unwrap();

        conn.delete(key).await.unwrap();
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
