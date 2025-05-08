use serde::{Deserialize, Serialize};

use super::_operation::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaxosMessage {
    // Paxos
    LeaderRequest {
        request_id: u64,
        source: String,
    },
    LeaderAccepted {
        request_id: u64,
        operation: Operation,
        source: String,
    },
    LeaderLearn {
        request_id: u64,
        source: String,
    },
    FollowerAck {
        request_id: u64,
        source: String,
    },

    // Leader Election
    Heartbeat {
        request_id: u64,
        source: String,
    },
    LeaderVote {
        request_id: u64,
        source: String,
    },
    LeaderDeclaration {
        request_id: u64,
        source: String,
    },

    // Client requests
    ClientRequest {
        operation: Operation,
        source: String,
    },
    RecoveryRequest {
        key: String,
        source: String,
    },
    RecoveryReply {
        index: usize,
        payload: Vec<u8>,
        source: String,
    },
}
