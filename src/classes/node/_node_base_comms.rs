use tracing::instrument;

use crate::base_libs::{_paxos_types::PaxosMessage, network::_messages::send_message};

use super::_node::Node;

impl Node {
    #[instrument(skip_all)]
    pub async fn forward_to_leader(&self, message: PaxosMessage) {
        let leader_addr = match &self.leader_address {
            Some(addr) => addr.to_string(),
            None => {
                println!("[ERROR] Leader address is not set");
                return;
            }
        };
        let leader_addr = &leader_addr as &str;

        println!("[MESSAGE] Forwarding request to leader at {}", leader_addr);
        _ = send_message(message, leader_addr).await;
    }

    #[instrument(skip_all)]
    pub async fn broadcast_message(&self, message: PaxosMessage) {
        println!("[MESSAGE] Broadcasting message: {:?}", message);
        let cluster_list = self.cluster_list.lock().await;
        for addr in cluster_list.iter() {
            if addr == &self.address.to_string() {
                continue;
            }
            _ = send_message(message.clone(), addr).await;
        }
    }
}
