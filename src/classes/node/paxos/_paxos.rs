use std::{fmt, io, u64};

use tokio::net::TcpStream;

use crate::{
    base_libs::{_operation::Operation, network::_address::Address},
    classes::node::_node::Node,
};

// ---PaxosState---
#[derive(PartialEq, Clone, Copy, Debug)]
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
    pub async fn handle_leader_request(
        &mut self,
        source: &String,
        stream: TcpStream,
        request_id: u64,
    ) {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_request(source, stream, request_id)
                    .await
            }
            PaxosState::Leader | PaxosState::Candidate => {
                if request_id > self.request_id {
                    println!(
                        "[ELECTION] {:?} Node received a leader request with a higher request id",
                        self.state
                    );
                    self.request_id = request_id;
                    self.leader_address = Address::from_string(&source);
                    self.state = PaxosState::Follower;
                    println!("[ELECTION] leader is now {:?}", self.leader_address);

                    self.follower_handle_leader_request(source, stream, request_id)
                        .await
                } else {
                    println!(
                        "[ELECTION] {:?} Node received a leader request with a lower request id, reasserting leadership",
                        self.state
                    );
                    self.declare_leader().await;
                }
            }
        }
    }
    pub async fn handle_leader_accepted(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        request_id: u64,
    ) -> Result<(), io::Error> {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_accepted(src_addr, stream, request_id)
                    .await?
            }
            _ => {
                println!(
                    "[ERROR] Node received LeaderAccepted message in state {:?}",
                    self.state
                );
            }
        }

        Ok(())
    }

    pub async fn handle_leader_learn(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        request_id: u64,
        operation: &Operation,
    ) -> Result<(), io::Error> {
        match self.state {
            PaxosState::Follower => {
                self.follower_handle_leader_learn(src_addr, stream, request_id, operation)
                    .await?
            }
            _ => {
                println!(
                    "[ERROR] Node received LeaderLearn message in state {:?}",
                    self.state
                );
            }
        }

        Ok(())
    }

    pub async fn handle_follower_ack(
        &self,
        _src_addr: &String,
        _stream: TcpStream,
        _request_id: u64,
    ) {
        match self.state {
            _ => {
                println!(
                    "[ERROR] Node received unexpected FollowerAck message in state {:?}",
                    self.state
                );
            }
        }
    }

    pub async fn handle_leader_vote(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        request_id: u64,
    ) {
        match self.state {
            PaxosState::Candidate => {
                self.candidate_handle_leader_vote(src_addr, stream, request_id)
                    .await;
            }
            _ => {
                println!(
                    "[ERROR] Node received LeaderVote message in state {:?}",
                    self.state
                );
            }
        }
    }

    pub async fn handle_leader_declaration(
        &mut self,
        src_addr: &String,
        _stream: TcpStream,
        request_id: u64,
    ) {
        match self.state {
            _ => {
                if self.request_id <= request_id {
                    self.request_id = request_id;
                    self.leader_address = Address::from_string(&src_addr);
                    self.state = PaxosState::Follower;
                    println!("[ELECTION] leader is now {:?}", self.leader_address);
                }
            }
        }
    }

    pub async fn handle_heartbeat(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        request_id: u64,
    ) {
        match self.state {
            PaxosState::Leader => {
                if self.request_id <= request_id {
                    println!(
                        "[ELECTION] {:?} Node received a heartbeat with a higher request id",
                        self.state
                    );
                    self.handle_leader_declaration(src_addr, stream, request_id)
                        .await;
                } else {
                    println!(
                        "[ELECTION] {:?} Node received a heartbeat with a lower request id, reasserting leadership",
                        self.state
                    );
                    self.declare_leader().await;
                }
            }
            _ => {
                if self.request_id <= request_id {
                    let leader_addr = match &self.leader_address {
                        Some(addr) => addr.to_string(),
                        None => {
                            println!("[ELECTION] Received heartbeat from an elected leader");
                            self.handle_leader_declaration(src_addr, stream, request_id)
                                .await;
                            return;
                        }
                    };
                    let leader_addr = &leader_addr as &str;
                    if src_addr != leader_addr {
                        println!("[ELECTION] Received heartbeat from a different leader");
                        self.handle_leader_declaration(src_addr, stream, request_id)
                            .await;
                    }
                }
            }
        }
    }
}
