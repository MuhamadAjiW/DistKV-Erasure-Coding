use std::{fmt, io, u64};

use crate::{
    base_libs::{
        _operation::Operation,
        _paxos_types::{FollowerRegistrationReply, FollowerRegistrationRequest},
    },
    classes::node::_node::Node,
};

// ---PaxosState---
#[derive(PartialEq)]
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
        &mut self,
        src_addr: &String,
        request_id: u64,
        operation: &Operation,
    ) -> Result<(), io::Error> {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_accepted(src_addr, request_id, operation)
                    .await?
            }
            PaxosState::Leader => {
                self.leader_handle_leader_accepted(src_addr, request_id, operation)
                    .await
            }
        }

        Ok(())
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
    pub async fn handle_follower_register_request(
        &self,
        src_addr: &String,
        follower: &FollowerRegistrationRequest,
    ) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_follower_register_request(&src_addr, &follower)
                    .await
            }
            PaxosState::Leader => {
                self.leader_handle_follower_register_request(&follower)
                    .await
            }
        }
    }
    pub async fn handle_follower_register_reply(
        &mut self,
        src_addr: &String,
        follower: &FollowerRegistrationReply,
    ) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_follower_register_reply(&src_addr, &follower)
                    .await
            }
            PaxosState::Leader => {
                self.leader_handle_follower_register_reply(&src_addr, &follower)
                    .await
            }
        }
    }
}
