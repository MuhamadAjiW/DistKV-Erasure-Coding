use std::collections::HashMap;

use omnipaxos::erasure::log_entry::OperationType;
use omnipaxos::macros::Entry;
use omnipaxos::storage::Snapshot;
use serde::{Deserialize, Serialize};

#[derive(Entry, Clone, Debug, Serialize, Deserialize)]
#[snapshot(KVSnapshot)]
pub struct KeyValue {
    pub key: String,
    pub value: Vec<u8>,
    pub op: OperationType,
    pub version: u64,
}

impl KeyValue {
    /// Creates a new KeyValue with the specified version
    pub fn with_version(key: String, value: Vec<u8>, op: OperationType, version: u64) -> Self {
        Self { key, value, op, version }
    }
    
    /// Sets the version of this entry
    pub fn set_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }
}

// The snapshot needs to track both the value and version for consistency
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, (Vec<u8>, u64)>, // (value, version)
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            snapshotted.insert(e.key.clone(), (e.value.clone(), e.version));
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, (value, version)) in delta.snapshotted {
            // Only update if the incoming version is newer or key doesn't exist
            match self.snapshotted.get(&k) {
                Some((_, current_version)) if *current_version >= version => {
                    // Keep the existing entry as it's newer or same version
                }
                _ => {
                    // Update with the new entry
                    self.snapshotted.insert(k, (value, version));
                }
            }
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}
