use std::{io, sync::atomic::Ordering, u64};

use tokio::{net::TcpStream, time::Instant};
use tracing::{error, info, instrument, warn};

use crate::{
    base_libs::{
        _operation::Operation,
        _paxos_types::PaxosMessage,
        network::{
            _address::Address,
            _messages::{reply_message, send_message},
        },
    },
    classes::node::{_node::Node, paxos::_paxos_state::PaxosState},
};

// ---Node Commands---
impl Node {
    #[instrument(level = "debug", skip_all)]
    pub async fn handle_leader_request(
        &mut self,
        source: &String,
        _stream: TcpStream,
        epoch: u64,
        request_id: u64,
    ) {
        if epoch < self.epoch {
            info!(
                "[ELECTION] Node received a leader request with a lower epoch: {}",
                epoch
            );
            return;
        }

        if epoch > self.epoch {
            info!(
                "[ELECTION] Node received a leader request with a higher epoch: {}",
                epoch
            );
            self.epoch = epoch;

            if self.state == PaxosState::Leader || self.state == PaxosState::Candidate {
                self.state = PaxosState::Follower;
                self.vote_count.store(0, Ordering::SeqCst);
                self.leader_address = None;
            }
        }

        match self.state {
            PaxosState::Follower => {
                if request_id > self.request_id {
                    self.request_id = request_id
                }

                let _ = send_message(
                    PaxosMessage::LeaderVote {
                        epoch: self.epoch,
                        source: self.address.to_string(),
                    },
                    &source,
                )
                .await;
            }
            PaxosState::Leader | PaxosState::Candidate => {
                info!(
                    "[ELECTION] {:?} Node received a leader request with the same request id",
                    self.state
                );
            }
        }
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn handle_leader_learn(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        epoch: u64,
        commit_id: u64,
    ) -> Result<(), io::Error> {
        if self.state != PaxosState::Follower {
            warn!(
                "[ERROR] Node received learn request in state {:?}",
                self.state
            );
            return Ok(());
        }
        let leader_addr = match &self.leader_address {
            Some(addr) => addr.to_string(),
            None => {
                error!("[ERROR] Leader address is not set");
                return Ok(());
            }
        };
        let leader_addr = &leader_addr as &str;
        if src_addr != leader_addr {
            warn!("[ERROR] Follower received request message from not a leader");
            return Ok(());
        }

        info!("Follower received learn request message from leader");
        let ack = PaxosMessage::Ack {
            request_id: commit_id,
            epoch,
            source: self.address.to_string(),
        };
        _ = reply_message(ack, stream).await;
        info!("Follower acknowledged commit ID: {}", commit_id);

        Ok(())
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn handle_leader_accept(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        epoch: u64,
        request_id: u64,
        operation: &Operation,
    ) -> Result<(), io::Error> {
        if self.state != PaxosState::Follower {
            warn!(
                "[ERROR] Node received leader accept message in state {:?}",
                self.state
            );
            return Ok(());
        }

        let leader_addr = match &self.leader_address {
            Some(addr) => addr.to_string(),
            None => {
                error!("[ERROR] Leader address is not set");
                return Ok(());
            }
        };
        let leader_addr = &leader_addr as &str;

        if src_addr != leader_addr {
            warn!("[ERROR] Follower received leader accept message from not a leader");
            return Ok(());
        }

        // _NOTE: Check log synchronization safety, this could block the whole operation
        self.synchronize_log(request_id - 1).await;
        self.store.transaction_log.append(operation).await;
        self.store.persistent.process_request(operation);

        if !self.store.memory.get(&operation.kv.key).await.is_none() {
            self.store.memory.remove(&operation.kv.key).await;
        }

        info!("Follower received leader accept message from leader",);
        let ack = PaxosMessage::Ack {
            epoch,
            request_id,
            source: self.address.to_string(),
        };
        _ = reply_message(ack, stream).await;
        info!("Follower acknowledged request ID: {}", request_id);

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
                warn!(
                    "[ERROR] Node received unexpected FollowerAck message in state {:?}",
                    self.state
                );
            }
        }
    }

    pub async fn handle_leader_vote(&mut self, src_addr: &String, _stream: TcpStream, epoch: u64) {
        if self.state != PaxosState::Candidate {
            warn!(
                "[ERROR] Node received LeaderVote message in state {:?}",
                self.state
            );
            return;
        }

        self.vote_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        info!(
            "[ELECTION] Received vote from {} for epoch: {}",
            src_addr, epoch
        );

        let quorum = self.cluster_list.read().await.len() / 2;

        if self.vote_count.load(std::sync::atomic::Ordering::SeqCst) > quorum {
            info!("[ELECTION] Received quorum votes, declaring leader");
            self.declare_leader().await;
        }
    }

    pub async fn handle_leader_declaration(
        &mut self,
        src_addr: &String,
        _stream: TcpStream,
        epoch: u64,
        commit_id: u64,
    ) {
        if epoch < self.epoch {
            info!(
                "[ELECTION] Node received a leader declaration with a lower epoch: {}",
                epoch
            );
            return;
        }

        if epoch > self.epoch {
            info!(
                "[ELECTION] Node received a leader declaration with a higher epoch: {}",
                epoch
            );
            self.epoch = epoch;
            self.vote_count.store(0, Ordering::SeqCst);
        }

        self.leader_address = Address::from_string(&src_addr);
        self.state = PaxosState::Follower;
        info!("[ELECTION] leader is now {:?}", self.leader_address);

        self.synchronize_log(commit_id).await;
    }

    pub async fn handle_heartbeat(
        &mut self,
        src_addr: &String,
        _stream: TcpStream,
        epoch: u64,
        commit_id: u64,
    ) {
        if epoch < self.epoch {
            info!(
                "[HEARTBEAT] Node received a heartbeat with a lower epoch: {}",
                epoch
            );
            return;
        }

        if epoch > self.epoch {
            info!(
                "[HEARTBEAT] Node received a heartbeat with a higher epoch: {}",
                epoch
            );
            self.epoch = epoch;
            self.vote_count.store(0, Ordering::SeqCst);
        }

        self.leader_address = Address::from_string(&src_addr);
        self.state = PaxosState::Follower;

        {
            let mut last_heartbeat_mut = self.last_heartbeat.write().await;
            *last_heartbeat_mut = Instant::now();
        }

        self.synchronize_log(commit_id).await;
    }
}
