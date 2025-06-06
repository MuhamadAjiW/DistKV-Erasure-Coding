use std::sync::atomic::Ordering;

use tokio::io;
use tracing::info;

use crate::{
    base_libs::_paxos_types::PaxosMessage,
    classes::node::{_node::Node, paxos::_paxos_state::PaxosState},
};

impl Node {
    pub async fn start_leader_election(&mut self) -> Result<(), io::Error> {
        info!("[ELECTION] Starting leader election");

        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.vote_count.store(1, Ordering::Relaxed);

        let new_request_id = self.request_id.fetch_add(1, Ordering::SeqCst) + 1;
        let leader_request = PaxosMessage::ElectionRequest {
            epoch: self.epoch.load(Ordering::SeqCst),
            request_id: new_request_id,
            source: self.address.to_string(),
        };
        self.broadcast_message(leader_request).await;

        Ok(())
    }

    pub async fn declare_leader(&mut self) {
        info!("[ELECTION] Declaring self as leader");
        self.state = PaxosState::Leader;
        self.leader_address = Some(self.address.clone());

        let leader_declaration = PaxosMessage::LeaderDeclaration {
            epoch: self.epoch.load(Ordering::SeqCst),
            commit_id: self.commit_id.load(Ordering::SeqCst),
            source: self.address.to_string(),
        };
        self.broadcast_message(leader_declaration).await;
    }
}
