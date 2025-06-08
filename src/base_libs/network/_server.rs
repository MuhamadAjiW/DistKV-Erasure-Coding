use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error};

use crate::base_libs::_types::OmniPaxosECKV;
use crate::base_libs::_types::OmniPaxosECMessage;
use crate::base_libs::network::_messages::send_omnipaxos_message;
use crate::config::_constants::{OUTGOING_MESSAGE_PERIOD, TICK_PERIOD};

pub struct OmniPaxosServerEC {
    pub omni_paxos: Arc<Mutex<OmniPaxosECKV>>,
    pub incoming: mpsc::Receiver<OmniPaxosECMessage>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<OmniPaxosECMessage>>, // for local delivery only
    pub peer_addresses: HashMap<NodeId, String>, // new: map NodeId to network address ("ip:port")
    pub message_buffer: Vec<OmniPaxosECMessage>,
}

impl OmniPaxosServerEC {
    async fn send_outgoing_msgs(&mut self) {
        self.omni_paxos
            .lock()
            .unwrap()
            .take_outgoing_messages(&mut self.message_buffer);
        for msg in self.message_buffer.drain(..) {
            let receiver = msg.get_receiver();
            if let Some(local_channel) = self.outgoing.get_mut(&receiver) {
                debug!("[SERVER] Sending message to local receiver {}", receiver);
                // Local delivery (same process)
                let _ = local_channel.send(msg).await;
            } else if let Some(addr) = self.peer_addresses.get(&receiver) {
                debug!(
                    "[SERVER] Sending message to network peer {} at {}",
                    receiver, addr
                );
                // Network delivery
                let send_result = send_omnipaxos_message(msg, addr, None).await;
                if let Err(e) = send_result {
                    error!("[SERVER] Failed to send message to {}: {}", addr, e);
                }
            } else {
                error!("No channel or address for receiver {}", receiver);
            }
        }
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                _ = tick_interval.tick() => {
                    debug!("[SERVER] Tick");
                    self.omni_paxos.lock().unwrap().tick();
                },
                _ = outgoing_interval.tick() => {
                    debug!("[SERVER] Outgoing interval");
                    self.send_outgoing_msgs().await;
                },
                Some(in_msg) = self.incoming.recv() => {
                    debug!("[SERVER] Received incoming message");
                    self.omni_paxos.lock().unwrap().handle_incoming(in_msg);
                },
                else => { break; }
            }
        }
    }
}
