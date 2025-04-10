use tokio::io;

use crate::{base_libs::_paxos_types::PaxosMessage, classes::node::_node::Node};

impl Node {
    pub async fn send_heartbeat(&mut self) -> Result<(), io::Error> {
        let heartbeat = PaxosMessage::Heartbeat {
            request_id: self.request_id,
            source: self.address.to_string(),
        };
        self.broadcast_message(heartbeat).await;

        Ok(())
    }
}
