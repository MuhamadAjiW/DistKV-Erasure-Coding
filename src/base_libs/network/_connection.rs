use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::base_libs::_paxos_types::PaxosMessage;
use crate::base_libs::network::_transaction::{TransactionId, TransactionManager};
use bincode;

pub struct ConnectionManager {
    pub transaction_manager: Arc<TransactionManager>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            transaction_manager: Arc::new(TransactionManager::new()),
        }
    }

    /// Always create a new connection for each call
    pub async fn connect(&self, addr: &str) -> std::io::Result<PeerConnection> {
        let stream = TcpStream::connect(addr).await?;
        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(16 * 1024 * 1024); // 16MB max frame size
        let framed = Framed::new(stream, codec);
        let (mut writer, mut reader) = framed.split();
        let (tx, mut rx) =
            mpsc::channel::<(PaxosMessage, Option<oneshot::Sender<PaxosMessage>>)>(1024);
        let transaction_manager = self.transaction_manager.clone();

        // Writer task
        tokio::spawn(async move {
            while let Some((msg, resp_sender)) = rx.recv().await {
                let data = match bincode::serialize(&msg) {
                    Ok(d) => d,
                    Err(_) => continue,
                };
                if let Err(_) = writer.send(data.into()).await {
                    return;
                }
                if let Some(sender) = resp_sender {
                    if let Some(tid) = extract_transaction_id(&msg) {
                        transaction_manager.pending.insert(
                            tid,
                            crate::base_libs::network::_transaction::PendingTransaction { sender },
                        );
                    }
                }
            }
        });

        // Reader task
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

        Ok(PeerConnection::new(tx))
    }

    pub async fn close_all(&self) {
        // No pooling, nothing to close
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
