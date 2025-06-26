use omnipaxos::erasure::ec_service::EntryFragment;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReconstructMessage {
    Request { key: String },
    Response { fragment: Option<EntryFragment> },
}

// Helper to serialize and deserialize ReconstructMessage
pub fn serialize_reconstruct_message(msg: &ReconstructMessage) -> Vec<u8> {
    bincode::serialize(msg).unwrap()
}

pub fn deserialize_reconstruct_message(bytes: &[u8]) -> Option<ReconstructMessage> {
    bincode::deserialize(bytes).ok()
}
