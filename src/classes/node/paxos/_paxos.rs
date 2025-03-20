use std::{fmt, io, u64};

use tokio::net::TcpStream;

use crate::{base_libs::_operation::Operation, classes::node::_node::Node};

// ---PaxosState---
#[derive(PartialEq, Clone, Copy)]
pub enum PaxosState {
    Follower,
    Candidate,
    Leader,
}
impl fmt::Display for PaxosState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PaxosState::Follower => write!(f, "Follower"),
            PaxosState::Candidate => write!(f, "Candidate"),
            PaxosState::Leader => write!(f, "Leader"),
        }
    }
}

// ---Node Commands---
impl Node {
    pub async fn handle_leader_request(&self, source: &String, stream: TcpStream, request_id: u64) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_request(source, stream, request_id)
                    .await
            }
            PaxosState::Candidate => {
                self.candidate_handle_leader_request(source, stream, request_id)
                    .await
            }
            PaxosState::Leader => {
                self.leader_handle_leader_request(source, stream, request_id)
                    .await
            }
        }
    }
    pub async fn handle_leader_accepted(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        request_id: u64,
        operation: &Operation,
    ) -> Result<(), io::Error> {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_accepted(src_addr, stream, request_id, operation)
                    .await?
            }
            PaxosState::Candidate => {
                self.candidate_handle_leader_accepted(src_addr, stream, request_id, operation)
                    .await?
            }
            PaxosState::Leader => {
                self.leader_handle_leader_accepted(src_addr, stream, request_id, operation)
                    .await
            }
        }

        Ok(())
    }

    pub async fn handle_follower_ack(&self, src_addr: &String, stream: TcpStream, request_id: u64) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_follower_ack(src_addr, request_id)
                    .await
            }
            PaxosState::Candidate => {
                self.candidate_handle_follower_ack(src_addr, request_id)
                    .await
            }
            PaxosState::Leader => {
                self.leader_handle_follower_ack(src_addr, stream, request_id)
                    .await
            }
        }
    }
}
