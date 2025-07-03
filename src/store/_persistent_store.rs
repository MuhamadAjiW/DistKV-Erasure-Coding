use std::marker::PhantomData;

use rocksdb::DB;
use serde::{Deserialize, Serialize};
use tracing::instrument;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionedEntry<T> {
    pub entry: T,
    pub version: u64,
}

pub struct KvPersistent<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Clone,
{
    rocks_db: DB,
    _phantom: PhantomData<T>,
}

impl<T> KvPersistent<T>
where
    T: Serialize + for<'a> Deserialize<'a> + Clone,
{
    pub fn new(db_path: &str) -> Self {
        return KvPersistent {
            rocks_db: DB::open_default(db_path).expect("Failed to open RocksDB"),
            _phantom: PhantomData,
        };
    }

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub fn set(&self, key: &str, data: &T) {
        let previous_value = self.get(key);
        let value = if let Some(previous_value) = previous_value {
            VersionedEntry {
                entry: data,
                version: previous_value.version + 1,
            }
        } else {
            VersionedEntry {
                entry: data,
                version: 1,
            }
        };

        let serialized_value =
            bincode::serialize(&value).expect("Failed to serialize VersionedEntry for RocksDB");

        self.rocks_db
            .put(key, serialized_value)
            .expect("Failed to set RocksDB");
    }

    #[inline(always)]
    #[instrument(level = "debug", skip_all)]
    pub fn get(&self, key: &str) -> Option<VersionedEntry<T>> {
        if let Ok(Some(value)) = self.rocks_db.get(key) {
            return Some(
                bincode::deserialize(&value)
                    .expect("Failed to deserialize VersionedEntry from RocksDB"),
            );
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
}
