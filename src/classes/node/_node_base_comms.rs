use crate::base_libs::network::_messages::send_omnipaxos_message;

use super::_node::Node;

impl Node {
    /// Send all outgoing Omnipaxos messages to peers.
    pub async fn send_omnipaxos_messages(&self) {
        let mut omnipaxos = self.omnipaxos.write().await;
        let mut buffer = Vec::new();
        omnipaxos.take_outgoing_messages(&mut buffer);

        let cluster_list = self.cluster_list.read().await;

        for msg in buffer {
            let receiver_id = msg.get_receiver();
            if receiver_id as usize == self.cluster_index {
                continue;
            }
            if let Some(addr_str) = cluster_list.get(receiver_id as usize) {
                let _ = send_omnipaxos_message(msg.clone(), addr_str).await;
            }
        }
    }
}
