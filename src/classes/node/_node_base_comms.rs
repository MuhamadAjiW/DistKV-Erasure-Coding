use std::sync::Arc;

use tracing::{error, instrument};

use crate::base_libs::{_paxos_types::PaxosMessage, network::_messages::send_message};

use super::_node::Node;

impl Node {
    #[instrument(level = "debug", skip_all)]
    pub async fn forward_to_leader(&self, message: PaxosMessage) {
        let leader_addr = match &self.leader_address {
            Some(addr) => addr.to_string(),
            None => {
                error!("[ERROR] Leader address is not set");
                return;
            }
        };
        let leader_addr = &leader_addr as &str;

        _ = send_message(message, leader_addr).await;
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn broadcast_message(&self, message: PaxosMessage) {
        let cluster_list = self.cluster_list.read().await;
        let addresses: Vec<String> = cluster_list.iter().cloned().collect();
        drop(cluster_list);

        let self_addr = self.address.to_string();
        let message_arc = Arc::new(message);

        let mut join_handles = Vec::new();

        for addr in addresses {
            if addr == self_addr {
                continue;
            }
            let message_clone = Arc::clone(&message_arc);
            let addr_clone = addr.clone();

            let handle = tokio::spawn(async move {
                _ = send_message((*message_clone).clone(), &addr_clone).await;
            });
            join_handles.push(handle);
        }
    }
}
