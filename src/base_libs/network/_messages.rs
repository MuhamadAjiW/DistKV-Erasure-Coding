use bincode;
use tokio::io::{self, AsyncReadExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::error;

use crate::base_libs::_paxos_types::PaxosMessage;
use crate::base_libs::network::_connection::ConnectionManager;
use crate::base_libs::network::_transaction::TransactionId;
use crate::config::_constants::ACK_TIMEOUT;

pub async fn receive_message_from_stream(stream: &mut TcpStream) -> io::Result<PaxosMessage> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;
    let message: PaxosMessage = match bincode::deserialize(&buffer) {
        Ok(msg) => msg,
        Err(e) => {
            error!("Failed to deserialize PaxosMessage: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };
    Ok(message)
}

/// Send a message using the connection pool (Framed) without expecting a response
pub async fn send_message(
    message: PaxosMessage,
    addr: &str,
    conn_mgr: &ConnectionManager,
) -> io::Result<()> {
    let peer_conn = conn_mgr
        .get_or_connect(addr)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    peer_conn
        .send(message, None)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Send failed"))
}

/// Send a message and receive a response using the connection pool (Framed)
pub async fn send_message_and_receive_response(
    mut message: PaxosMessage,
    addr: &str,
    conn_mgr: &ConnectionManager,
) -> io::Result<PaxosMessage> {
    let transaction_id = TransactionId::new();
    match &mut message {
        PaxosMessage::AcceptRequest {
            transaction_id: tid,
            ..
        }
        | PaxosMessage::LearnRequest {
            transaction_id: tid,
            ..
        }
        | PaxosMessage::ElectionRequest {
            transaction_id: tid,
            ..
        }
        | PaxosMessage::LeaderVote {
            transaction_id: tid,
            ..
        }
        | PaxosMessage::LeaderDeclaration {
            transaction_id: tid,
            ..
        }
        | PaxosMessage::Heartbeat {
            transaction_id: tid,
            ..
        }
        | PaxosMessage::ClientRequest {
            transaction_id: tid,
            ..
        }
        | PaxosMessage::RecoveryRequest {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        _ => {}
    }
    let (tx, rx) = oneshot::channel();
    let peer_conn = conn_mgr
        .get_or_connect(addr)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    peer_conn
        .send(message, Some(tx))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Send failed"))?;
    match timeout(ACK_TIMEOUT, rx).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => {
            conn_mgr.remove(addr).await;
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Transaction channel closed",
            ))
        }
        Err(_) => {
            conn_mgr.remove(addr).await;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Timeout waiting for response",
            ))
        }
    }
}
