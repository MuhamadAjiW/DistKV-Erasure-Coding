pub type KeyIndexMapResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub trait KeyIndexMap: Send + Sync {
    fn put(&self, key: &str, log_idx: u64) -> KeyIndexMapResult<()>;
    fn get(&self, key: &str) -> KeyIndexMapResult<Option<u64>>;
    fn remove(&self, key: &str) -> KeyIndexMapResult<()>;
    fn clear(&self) -> KeyIndexMapResult<()>;
}
