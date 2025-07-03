use std::sync::Arc;

use tokio::sync::{oneshot, Mutex};

/// Buffer type for pending SET operations, awaiting durability
pub struct PendingSet {
    pub key: String,
    pub fragment: Vec<u8>,
    // Wrap sender in Option to allow moving it out exactly once
    pub response: Arc<Mutex<Option<oneshot::Sender<String>>>>,
}
