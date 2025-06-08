use omnipaxos::util::NodeId;
use std::collections::HashMap;
use tokio::sync::mpsc;

use crate::base_libs::_types::OmniPaxosECMessage;

pub struct OmniPaxosServerEC {
    pub incoming: mpsc::Receiver<OmniPaxosECMessage>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<OmniPaxosECMessage>>,
    pub message_buffer: Vec<OmniPaxosECMessage>,
}

impl OmniPaxosServerEC {
    pub async fn run(&mut self) {
        // This is now a pure message relay/queue handler, not owning the OmniPaxos instance directly.
        while let Some(in_msg) = self.incoming.recv().await {
            // Forward or process messages as needed (e.g., send to TCP peer, etc.)
            let receiver = in_msg.get_receiver();
            if let Some(channel) = self.outgoing.get_mut(&receiver) {
                let _ = channel.send(in_msg).await;
            }
        }
    }
}
