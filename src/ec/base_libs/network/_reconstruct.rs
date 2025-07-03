use serde::{Deserialize, Serialize};

use crate::ec::classes::_entry::ECKeyValue;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReconstructMessage {
    Request { key: String },
    Response { fragment: Option<ECKeyValue> },
}

// Helper to serialize and deserialize ReconstructMessage
pub fn serialize_reconstruct_message(msg: &ReconstructMessage) -> Vec<u8> {
    bincode::serialize(msg).unwrap()
}

pub fn deserialize_reconstruct_message(bytes: &[u8]) -> Option<ReconstructMessage> {
    bincode::deserialize(bytes).ok()
}
