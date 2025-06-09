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
}

// The snapshot does not need to keep track of the operation type,
// as it is not needed for the snapshot itself. Operation type is only needed during the consensus.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KVSnapshot {
    snapshotted: HashMap<String, Vec<u8>>,
}

impl Snapshot<KeyValue> for KVSnapshot {
    fn create(entries: &[KeyValue]) -> Self {
        let mut snapshotted = HashMap::new();
        for e in entries {
            snapshotted.insert(e.key.clone(), e.value.clone());
        }
        Self { snapshotted }
    }

    fn merge(&mut self, delta: Self) {
        for (k, v) in delta.snapshotted {
            self.snapshotted.insert(k, v);
        }
    }

    fn use_snapshots() -> bool {
        true
    }
}
