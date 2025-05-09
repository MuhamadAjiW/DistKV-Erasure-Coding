use tokio::io;

use crate::{
    base_libs::_paxos_types::PaxosMessage,
    classes::node::{_node::Node, paxos::_paxos::PaxosState},
};

// TODO: Implement
impl Node {
    // Active command
    pub async fn start_leader_election(&mut self) -> Result<(), io::Error> {
        println!("[ELECTION] Starting leader election");

        let leader_request = PaxosMessage::ElectionRequest {
            epoch: self.epoch,
            request_id: self.request_id,
            source: self.address.to_string(),
        };
        self.broadcast_message(leader_request).await;

        Ok(())
    }

    pub async fn declare_leader(&mut self) {
        println!("[ELECTION] Declaring self as leader");
        self.state = PaxosState::Leader;
        self.leader_address = Some(self.address.clone());

        let leader_declaration = PaxosMessage::LeaderDeclaration {
            epoch: self.epoch,
            commit_id: self.request_id,
            source: self.address.to_string(),
        };
        self.broadcast_message(leader_declaration).await;
    }
}
