use crate::base_libs::_operation::{Operation, OperationType};
use omnipaxos::util::LogEntry;

use super::_node::Node;

impl Node {
    pub async fn process_request(&self, request: &Operation) -> Option<String> {
        match request.op_type {
            OperationType::GET => {
                // Read from decided log entries (state machine)
                let omnipaxos = self.omnipaxos.read().await;
                if let Some(entries) = omnipaxos.read_decided_suffix(0) {
                    for entry in entries {
                        if let LogEntry::Decided(op) = entry {
                            if op.kv.key == request.kv.key && op.op_type == OperationType::SET {
                                return Some(String::from_utf8_lossy(&op.kv.value).to_string());
                            }
                        }
                    }
                }
                None
            }
            OperationType::SET | OperationType::DELETE => {
                // Propose to Omnipaxos
                let mut omnipaxos = self.omnipaxos.write().await;
                let _ = omnipaxos.append(request.clone());
                // Wait for decision (polling for simplicity)
                loop {
                    if let Some(entries) = omnipaxos.read_decided_suffix(0) {
                        for entry in entries {
                            if let LogEntry::Decided(op) = entry {
                                if &op == request {
                                    return Some("OK".to_string());
                                }
                            }
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
            _ => None,
        }
    }
}
