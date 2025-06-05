use serde::{Deserialize, Serialize};

use super::_operation::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaxosMessage {
    // Paxos
    LearnRequest {
        epoch: u64,
        commit_id: u64,
        source: String,
    },
    AcceptRequest {
        epoch: u64,
        request_id: u64,
        operation: Operation,
        source: String,
    },
    Ack {
        epoch: u64,
        request_id: u64,
        source: String,
    },

    // Leader Election
    ElectionRequest {
        epoch: u64,
        request_id: u64,
        source: String,
    },
    LeaderVote {
        epoch: u64,
        source: String,
    },
    LeaderDeclaration {
        epoch: u64,
        commit_id: u64,
        source: String,
    },
    Heartbeat {
        epoch: u64,
        commit_id: u64,
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
