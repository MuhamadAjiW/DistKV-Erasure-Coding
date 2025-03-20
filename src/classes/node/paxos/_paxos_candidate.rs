use std::sync::Arc;

use tokio::{io, net::TcpStream, sync::Mutex};

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

    pub async fn declare_leader(node_arc: Arc<Mutex<Node>>) {
        {
            let mut node = node_arc.lock().await;
            println!("[ELECTION] Declaring self as leader");
            node.state = PaxosState::Leader;
            node.leader_address = Some(node.address.clone());

            let leader_declaration = PaxosMessage::LeaderDeclaration {
                request_id: node.request_id,
                source: node.address.to_string(),
            };
            node.broadcast_message(leader_declaration).await;
        }

        let heartbeat_duration = {
            let node = node_arc.lock().await;
            let timeout_duration = *node.timeout_duration.read().await;
            timeout_duration / 2
        };

        let cloned_self = node_arc.clone();
        tokio::spawn(async move {
            println!("[TIMEOUT] Spawning task to check for timeout");

            loop {
                tokio::time::sleep(heartbeat_duration).await;

                let node = cloned_self.lock().await;
                let heartbeat = PaxosMessage::Heartbeat {
                    request_id: node.request_id,
                    source: node.address.to_string(),
                };
                node.broadcast_message(heartbeat).await;

                if !node.running {
                    break;
                }
            }
        });
    }

    // ---Handlers---
    pub async fn candidate_handle_leader_vote(
        node_arc: Arc<Mutex<Node>>,
        src_addr: &String,
        _stream: TcpStream,
        request_id: u64,
    ) {
        let node = node_arc.lock().await;
        node.vote_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        println!(
            "[ELECTION] Received vote from {} for request ID: {}",
            src_addr, request_id
        );

        let quorum = node.cluster_list.lock().await.len() / 2;

        if node.vote_count.load(std::sync::atomic::Ordering::SeqCst) > quorum {
            println!("[ELECTION] Received quorum votes, declaring leader");
            drop(node); // Release lock before await
            Node::declare_leader(node_arc.clone()).await;
        }
    }

    // _TODO: handle false message
    pub async fn candidate_handle_follower_ack(&self, _src_addr: &String, _request_id: u64) {}
}
