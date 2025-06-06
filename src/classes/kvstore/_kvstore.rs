use std::sync::Arc;

use tracing::instrument;

use crate::classes::{ec::_ec::ECService, node::_node::Node};

pub struct KvStoreModule {
    pub ec: Arc<ECService>,
}

impl KvStoreModule {
    pub async fn new(_db_path: &str, _tlog_path: &str, ec: Arc<ECService>) -> Self {
        KvStoreModule { ec }
    }

    pub async fn initialize(&mut self) {
        // No-op: transaction log removed
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn get_from_cluster(
        &self,
        key: &String,
        _node: &Node,
    ) -> Result<Option<String>, reed_solomon_erasure::Error> {
        // All reads are now from Omnipaxos log/state machine
        let omnipaxos = _node.omnipaxos.read().await;
        if let Some(entries) = omnipaxos.read_decided_suffix(0) {
            for entry in entries {
                if let omnipaxos::util::LogEntry::Decided(op) = entry {
                    if op.kv.key == *key
                        && op.op_type == crate::base_libs::_operation::OperationType::SET
                    {
                        let result = String::from_utf8(op.kv.value.clone())
                            .map_err(|_e| reed_solomon_erasure::Error::InvalidIndex)?;
                        return Ok(Some(result));
                    }
                }
            }
        }
        Ok(None)
    }
}
