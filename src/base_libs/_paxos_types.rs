use serde::{Deserialize, Serialize};

use super::_operation::Operation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowerRegistrationRequest {
    pub follower_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowerRegistrationReply {
    pub follower_list: Vec<String>,
    pub index: usize,
}

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
    FollowerRegisterRequest(FollowerRegistrationRequest),
    FollowerRegisterReply(FollowerRegistrationReply),

    // Client requests
    ClientRequest {
        request_id: u64,
        payload: Vec<u8>,
    },
    RecoveryRequest {
        key: String,
    },
    RecoveryReply {
        index: usize,
        payload: Vec<u8>,
    },
}
