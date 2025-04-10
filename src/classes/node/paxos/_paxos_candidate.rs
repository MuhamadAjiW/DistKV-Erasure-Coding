use tokio::{io, net::TcpStream};

use crate::{
    base_libs::_paxos_types::PaxosMessage,
    classes::node::{_node::Node, paxos::_paxos::PaxosState},
};

// TODO: Implement
impl Node {
    // Active command
    pub async fn start_leader_election(&mut self) -> Result<(), io::Error> {
        println!("[ELECTION] Starting leader election");

        let leader_request = PaxosMessage::LeaderRequest {
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
            request_id: self.request_id,
            source: self.address.to_string(),
        };
        self.broadcast_message(leader_declaration).await;
    }

    // ---Handlers---
    pub async fn candidate_handle_leader_vote(
        &mut self,
        src_addr: &String,
        _stream: TcpStream,
        request_id: u64,
    ) {
        self.vote_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        println!(
            "[ELECTION] Received vote from {} for request ID: {}",
            src_addr, request_id
        );

        let quorum = self.cluster_list.lock().await.len() / 2;

        if self.vote_count.load(std::sync::atomic::Ordering::SeqCst) > quorum {
            println!("[ELECTION] Received quorum votes, declaring leader");
            self.declare_leader().await;
        }
    }
}
