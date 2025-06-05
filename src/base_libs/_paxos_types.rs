use serde::{Deserialize, Serialize};

use crate::base_libs::network::_transaction::TransactionId;

use super::_operation::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaxosMessage {
    // Paxos
    LearnRequest {
        epoch: u64,
        commit_id: u64,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    AcceptRequest {
        epoch: u64,
        request_id: u64,
        operation: Operation,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    Ack {
        epoch: u64,
        request_id: u64,
        source: String,
        transaction_id: Option<TransactionId>,
    },

    // Leader Election
    ElectionRequest {
        epoch: u64,
        request_id: u64,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    LeaderVote {
        epoch: u64,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    LeaderDeclaration {
        epoch: u64,
        commit_id: u64,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    Heartbeat {
        epoch: u64,
        commit_id: u64,
        source: String,
        transaction_id: Option<TransactionId>,
    },

    // Client requests
    ClientRequest {
        operation: Operation,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    ClientReply {
        response: String,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    RecoveryRequest {
        key: String,
        source: String,
        transaction_id: Option<TransactionId>,
    },
    RecoveryReply {
        index: usize,
        payload: Vec<u8>,
        source: String,
        transaction_id: Option<TransactionId>,
    },
}
