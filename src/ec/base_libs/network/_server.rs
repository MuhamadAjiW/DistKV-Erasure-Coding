use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::time;
use tracing::{debug, error};

use crate::config::_constants::{OUTGOING_MESSAGE_PERIOD, TICK_PERIOD};
use crate::ec::base_libs::_types::OmniPaxosECKV;
use crate::ec::base_libs::_types::OmniPaxosECMessage;
use crate::ec::base_libs::network::_messages::send_omnipaxos_message;

pub struct OmniPaxosServerEC {
    pub omni_paxos: Arc<Mutex<OmniPaxosECKV>>,
    pub incoming: mpsc::Receiver<OmniPaxosECMessage>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<OmniPaxosECMessage>>, // for local delivery only
    pub peer_addresses: HashMap<NodeId, String>, // new: map NodeId to network address ("ip:port")
    pub message_buffer: Vec<OmniPaxosECMessage>,
}

impl OmniPaxosServerEC {
    async fn send_outgoing_msgs(&mut self) {
        let mut buffer = Vec::new();
        {
            let mut omni = self.omni_paxos.lock().await;
            omni.take_outgoing_messages(&mut buffer);
        }
        let mut peer_batches: HashMap<NodeId, Vec<OmniPaxosECMessage>> = HashMap::new();
        for msg in buffer.drain(..) {
            let receiver = msg.get_receiver();
            if let Some(local_channel) = self.outgoing.get_mut(&receiver) {
                // Fast-path: local delivery
                let _ = local_channel.send(msg).await;
            } else if self.peer_addresses.contains_key(&receiver) {
                peer_batches.entry(receiver).or_default().push(msg);
            } else {
                error!("No channel or address for receiver {}", receiver);
            }
        }
        for (receiver, batch) in peer_batches {
            if let Some(addr) = self.peer_addresses.get(&receiver) {
                let send_result = send_omnipaxos_message(batch, addr, None).await;
                if let Err(e) = send_result {
                    error!("[SERVER] Failed to send batch to {}: {}", addr, e);
                }
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
                    {
                        let mut omni = self.omni_paxos.lock().await;
                        omni.tick();
                    }
                },
                _ = outgoing_interval.tick() => {
                    debug!("[SERVER] Outgoing interval");
                    self.send_outgoing_msgs().await;
                },
                Some(in_msg) = self.incoming.recv() => {
                    debug!("[SERVER] Received incoming message");
                    {
                        let mut omni = self.omni_paxos.lock().await;
                        omni.handle_incoming(in_msg);
                    }
                },
                else => { break; }
            }
        }
    }
}
