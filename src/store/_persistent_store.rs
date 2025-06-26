use rocksdb::DB;
use std::sync::Arc;

use crate::store::_key_index_map::{KeyIndexMap, KeyIndexMapResult};

pub struct RocksDBKeyIndexMap {
    db: Arc<DB>,
}

impl RocksDBKeyIndexMap {
    pub fn open(path: &str) -> KeyIndexMapResult<Self> {
        let db = DB::open_default(path)?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl KeyIndexMap for RocksDBKeyIndexMap {
    fn put(&self, key: &str, log_idx: u64) -> KeyIndexMapResult<()> {
        self.db.put(key.as_bytes(), &log_idx.to_be_bytes())?;
        Ok(())
    }
    fn get(&self, key: &str) -> KeyIndexMapResult<Option<u64>> {
        if let Some(bytes) = self.db.get(key.as_bytes())? {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes);
                Ok(Some(u64::from_be_bytes(arr)))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
    fn remove(&self, key: &str) -> KeyIndexMapResult<()> {
        self.db.delete(key.as_bytes())?;
        Ok(())
    }
    fn clear(&self) -> KeyIndexMapResult<()> {
        self.db.flush()?;
        Ok(())
    }
}
