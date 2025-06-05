use rust_rocksdb::DB;
use tracing::instrument;

use crate::base_libs::_operation::{Operation, OperationType};

pub struct KvPersistent {
    rocks_db: DB,
}

impl KvPersistent {
    pub fn new(db_path: &str) -> Self {
        return KvPersistent {
            rocks_db: DB::open_default(db_path).expect("Failed to open RocksDB"),
        };
    }

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub fn set(&self, key: &str, value: &[u8]) {
        self.rocks_db
            .put(key, value)
            .expect("Failed to set RocksDB");
    }

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Ok(Some(value)) = self.rocks_db.get(key) {
            return Some(value);
        } else {
            return None;
        }
    }

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub fn remove(&self, key: &str) {
        self.rocks_db
            .delete(key)
            .expect("Failed to delete from RocksDB");
    }

    #[instrument(level = "debug", skip_all)]
    pub fn process_request(&self, request: &Operation) -> Option<Vec<u8>> {
        let mut response: Option<Vec<u8>> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.get(&request.kv.key);
            }
            OperationType::SET => {
                self.set(&request.kv.key, &request.kv.value);
            }
            OperationType::DELETE => {
                self.remove(&request.kv.key);
            }
            _ => {}
        }

        response
    }
}
