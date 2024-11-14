use crate::{classes::node::Node, types::FollowerRegistration};
use std::{fmt, u64};

use super::operation::Operation;

// ---PaxosState---
pub enum PaxosState {
    Follower,
    Leader,
}
impl fmt::Display for PaxosState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaxosState::Follower => write!(f, "Follower"),
            PaxosState::Leader => write!(f, "Leader"),
        }
    }
}

// _TODO: Implement handlers
// ---Node Commands---
impl Node {
    pub async fn handle_leader_request(&self, src_addr: &String, request_id: u64) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_request(src_addr, request_id)
                    .await
            }
            PaxosState::Leader => {
                self.leader_handle_leader_request(src_addr, request_id)
                    .await
            }
        }
    }
    pub async fn handle_leader_accepted(
        &self,
        src_addr: &String,
        request_id: u64,
        operation: &Operation,
    ) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_accepted(src_addr, request_id, operation)
                    .await
            }
            PaxosState::Leader => {
                self.leader_handle_leader_accepted(src_addr, request_id, operation)
                    .await
            }
        }
    }
    pub async fn handle_client_request(
        &mut self,
        src_addr: &String,
        request_id: u64,
        payload: &Vec<u8>,
    ) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_client_request(src_addr, request_id, payload)
                    .await
            }
            PaxosState::Leader => {
                self.leader_handle_client_request(src_addr, request_id, payload)
                    .await
            }
        }
    }
    pub async fn handle_follower_ack(&self, src_addr: &String, request_id: u64) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_follower_ack(src_addr, request_id)
                    .await
            }
            PaxosState::Leader => self.leader_handle_follower_ack(src_addr, request_id).await,
        }
    }
    pub async fn handle_follower_register(
        &self,
        src_addr: &String,
        follower: &FollowerRegistration,
    ) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_follower_register(src_addr, follower)
                    .await
            }
            PaxosState::Leader => self.leader_handle_follower_register(follower).await,
        }
    }
}
