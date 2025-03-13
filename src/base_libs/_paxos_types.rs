use serde::{Deserialize, Serialize};

use super::_operation::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaxosMessage {
    // Paxos
    LeaderRequest {
        request_id: u64,
    },
    LeaderAccepted {
        request_id: u64,
        operation: Operation,
    },
    FollowerAck {
        request_id: u64,
    },

    // Client requests
    ClientRequest {
        operation: Operation,
    },
    RecoveryRequest {
        key: String,
    },
    RecoveryReply {
        index: usize,
        payload: Vec<u8>,
    },
}
