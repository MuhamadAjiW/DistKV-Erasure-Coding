use moka::future::Cache;
use std::sync::Arc;

use crate::store::_key_index_map::{KeyIndexMap, KeyIndexMapResult};

pub struct MemoryKeyIndexMap {
    cache: Arc<Cache<String, u64>>,
}

impl MemoryKeyIndexMap {
    pub fn new(capacity: u64) -> Self {
        let cache = Cache::new(capacity);
        Self {
            cache: Arc::new(cache),
        }
    }
}

impl KeyIndexMap for MemoryKeyIndexMap {
    fn put(&self, key: &str, log_idx: u64) -> KeyIndexMapResult<()> {
        let key = key.to_string();
        let cache = self.cache.clone();
        tokio::spawn(async move {
            cache.insert(key, log_idx).await;
        });
        Ok(())
    }
    fn get(&self, key: &str) -> KeyIndexMapResult<Option<u64>> {
        let cache = self.cache.clone();
        let key = key.to_string();
        let fut = async move { cache.get(&key).await };
        Ok(tokio::runtime::Handle::current().block_on(fut))
    }
    fn remove(&self, key: &str) -> KeyIndexMapResult<()> {
        let cache = self.cache.clone();
        let key = key.to_string();
        tokio::spawn(async move {
            cache.invalidate(&key).await;
        });
        Ok(())
    }
    fn clear(&self) -> KeyIndexMapResult<()> {
        let cache = self.cache.clone();
        tokio::spawn(async move {
            cache.invalidate_all();
        });
        Ok(())
    }
}
