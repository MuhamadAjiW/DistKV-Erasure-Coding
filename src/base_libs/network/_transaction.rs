use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::base_libs::_paxos_types::PaxosMessage;

/// Unique transaction ID for correlating request/response pairs
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId(pub String);

impl TransactionId {
    pub fn new() -> Self {
        TransactionId(Uuid::now_v7().to_string())
    }
}

/// A pending transaction, including a channel to send the response back to the requester
pub struct PendingTransaction {
    pub sender: oneshot::Sender<PaxosMessage>,
}

/// Manages transaction state for correlating request/response pairs
/// This allows us to handle responses either in the event loop or in the direct response path
pub struct TransactionManager {
    pub pending: DashMap<TransactionId, PendingTransaction>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            pending: DashMap::new(),
        }
    }

    /// Register a new transaction and get a channel for receiving the response
    pub async fn register_transaction(&self, id: TransactionId) -> oneshot::Receiver<PaxosMessage> {
        let (sender, receiver) = oneshot::channel();
        self.pending.insert(id, PendingTransaction { sender });
        receiver
    }

    /// Handle a response to a transaction, either directly or from the event loop
    /// Returns true if the response was for a pending transaction, false otherwise
    pub async fn handle_response(&self, id: &TransactionId, response: PaxosMessage) -> bool {
        if let Some((_, transaction)) = self.pending.remove(id) {
            // Send the response to the waiting task
            let _ = transaction.sender.send(response);
            true
        } else {
            false
        }
    }

    /// Cancel a transaction, typically used when a timeout occurs
    pub async fn cancel_transaction(&self, id: &TransactionId) {
        self.pending.remove(id);
    }
}
