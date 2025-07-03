use std::collections::HashMap;

use omnipaxos::erasure::ec_service::EntryFragment;
use omnipaxos::erasure::log_entry::{ECEntry, OperationType};
use omnipaxos::macros::Entry;
use omnipaxos::storage::Snapshot;
use serde::{Deserialize, Serialize};

#[derive(Entry, Clone, Debug, Serialize, Deserialize)]
#[snapshot(ECKVSnapshot)]
pub struct ECKeyValue {
    pub key: String,
    pub fragment: EntryFragment,
    pub op: OperationType,
    pub version: u64,
}

impl ECEntry for ECKeyValue {
    fn operation(&self) -> &OperationType {
        &self.op
    }
    fn key(&self) -> &str {
        &self.key
    }
    fn value(&self) -> &EntryFragment {
        &self.fragment
    }
    fn from_parts(key: String, fragment: EntryFragment, op: OperationType) -> Self
    where
        Self: Sized,
    {
        Self { key, fragment, op, version: 1 }
    }
}

impl ECKeyValue {
    /// Creates a new ECKeyValue with the specified version
    pub fn with_version(key: String, fragment: EntryFragment, op: OperationType, version: u64) -> Self {
        Self { key, fragment, op, version }
    }
    
    /// Sets the version of this entry
    pub fn set_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }
}

// The snapshot needs to track both the fragment and version for consistency
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ECKVSnapshot {
    snapshotted: HashMap<String, (EntryFragment, u64)>, // (fragment, version)
}

impl Snapshot<ECKeyValue> for ECKVSnapshot {
    fn create(entries: &[ECKeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            snapshotted.insert(e.key.clone(), (e.fragment.clone(), e.version));
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, (fragment, version)) in delta.snapshotted {
            // Only update if the incoming version is newer or key doesn't exist
            match self.snapshotted.get(&k) {
                Some((_, current_version)) if *current_version >= version => {
                    // Keep the existing entry as it's newer or same version
                }
                _ => {
                    // Update with the new entry
                    self.snapshotted.insert(k, (fragment, version));
                }
            }
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}
