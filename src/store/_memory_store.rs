use tracing::instrument;

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

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub async fn set(&self, key: &str, value: &[u8]) {
        self.store.insert(key.to_string(), value.to_vec()).await;
    }

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.store.get(key).await
    }

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub async fn remove(&self, key: &str) {
        self.store.invalidate(key).await;
    }
}
