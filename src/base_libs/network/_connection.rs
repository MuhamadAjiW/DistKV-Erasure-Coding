use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::base_libs::_paxos_types::PaxosMessage;
use crate::base_libs::network::_transaction::{TransactionId, TransactionManager};
use bincode;

pub struct ConnectionManager {
    pool: Arc<DashMap<String, PeerConnection>>,
    pub transaction_manager: Arc<TransactionManager>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(DashMap::new()),
            transaction_manager: Arc::new(TransactionManager::new()),
        }
    }

    /// Get or connect to a peer, spawning a background task for message dispatch
    pub async fn get_or_connect(&self, addr: &str) -> std::io::Result<PeerConnection> {
        if let Some(conn) = self.pool.get(addr) {
            return Ok(conn.value().clone());
        }
        let stream = TcpStream::connect(addr).await?;
        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(16 * 1024 * 1024); // 16MB max frame size
        let framed = Framed::new(stream, codec);
        let (mut writer, mut reader) = framed.split();
        let (tx, mut rx) =
            mpsc::channel::<(PaxosMessage, Option<oneshot::Sender<PaxosMessage>>)>(1024); // Larger buffer
        let transaction_manager = self.transaction_manager.clone();
        let addr_string = addr.to_string();

        // Spawn writer task with batching
        tokio::spawn(async move {
            loop {
                // Batch up to 32 messages or wait up to 200us
                let mut batch = Vec::with_capacity(32);
                let first_resp;
                // Always wait for at least one message
                match rx.recv().await {
                    Some(item) => {
                        first_resp = Some(item);
                    }
                    None => break, // channel closed
                }
                if let Some(item) = first_resp {
                    batch.push(item);
                }
                let start = Instant::now();
                while batch.len() < 32 {
                    let elapsed = start.elapsed();
                    let wait = if elapsed >= Duration::from_micros(200) {
                        // Already waited enough
                        break;
                    } else {
                        Duration::from_micros(200) - elapsed
                    };
                    match timeout(wait, rx.recv()).await {
                        Ok(Some(item)) => batch.push(item),
                        _ => break, // timeout or channel closed
                    }
                }
                // Send all batched messages
                for (msg, resp_sender) in batch {
                    let data = match bincode::serialize(&msg) {
                        Ok(d) => d,
                        Err(_) => continue,
                    };
                    if let Err(_) = writer.send(data.into()).await {
                        // Connection broken
                        return;
                    }
                    // If expecting a response, register the transaction
                    if let Some(sender) = resp_sender {
                        if let Some(tid) = extract_transaction_id(&msg) {
                            transaction_manager.pending.insert(
                                tid,
                                crate::base_libs::network::_transaction::PendingTransaction {
                                    sender,
                                },
                            );
                        }
                    }
                }
            }
        });

        // Spawn reader task
        let transaction_manager2 = self.transaction_manager.clone();
        tokio::spawn(async move {
            while let Some(Ok(bytes)) = reader.next().await {
                if let Ok(msg) = bincode::deserialize::<PaxosMessage>(&bytes) {
                    if let Some(tid) = extract_transaction_id(&msg) {
                        let _ = transaction_manager2.handle_response(&tid, msg).await;
                    }
                }
            }
        });

        let peer_conn = PeerConnection::new(tx);
        self.pool.insert(addr_string, peer_conn.clone());
        Ok(peer_conn)
    }

    pub async fn remove(&self, addr: &str) {
        self.pool.remove(addr);
    }

    pub async fn close_all(&self) {
        self.pool.clear();
    }
}

#[derive(Clone)]
pub struct PeerConnection {
    sender: mpsc::Sender<(PaxosMessage, Option<oneshot::Sender<PaxosMessage>>)>,
}

impl PeerConnection {
    pub fn new(
        sender: mpsc::Sender<(PaxosMessage, Option<oneshot::Sender<PaxosMessage>>)>,
    ) -> Self {
        Self { sender }
    }

    pub async fn send(
        &self,
        msg: PaxosMessage,
        resp: Option<oneshot::Sender<PaxosMessage>>,
    ) -> Result<(), ()> {
        self.sender.send((msg, resp)).await.map_err(|_| ())
    }
}

// Helper to extract transaction id from PaxosMessage
fn extract_transaction_id(msg: &PaxosMessage) -> Option<TransactionId> {
    match msg {
        PaxosMessage::AcceptRequest { transaction_id, .. }
        | PaxosMessage::LearnRequest { transaction_id, .. }
        | PaxosMessage::ElectionRequest { transaction_id, .. }
        | PaxosMessage::LeaderVote { transaction_id, .. }
        | PaxosMessage::LeaderDeclaration { transaction_id, .. }
        | PaxosMessage::Heartbeat { transaction_id, .. }
        | PaxosMessage::ClientRequest { transaction_id, .. }
        | PaxosMessage::RecoveryRequest { transaction_id, .. } => transaction_id.clone(),
        _ => None,
    }
}
