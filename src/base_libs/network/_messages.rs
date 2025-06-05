use bincode;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
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

pub async fn send_message(
    message: PaxosMessage,
    addr: &str,
    conn_mgr: &ConnectionManager,
) -> io::Result<()> {
    let stream_arc = conn_mgr.get_or_connect(addr).await?;
    let mut stream = stream_arc.lock().await;
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&serialized).await?;
    Ok(())
}

use tokio::time::timeout;

/// Send a message using the connection pool and receive a response (request/response pattern).
/// Uses a transaction ID to correlate requests with responses, which can come through the event loop.
pub async fn send_message_and_receive_response(
    mut message: PaxosMessage,
    addr: &str,
    conn_mgr: &ConnectionManager,
) -> io::Result<PaxosMessage> {
    // Create a transaction ID for this request-response pair
    let transaction_id = TransactionId::new();

    match &mut message {
        PaxosMessage::AcceptRequest {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        PaxosMessage::LearnRequest {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        PaxosMessage::ElectionRequest {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        PaxosMessage::LeaderVote {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        PaxosMessage::LeaderDeclaration {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        PaxosMessage::Heartbeat {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        PaxosMessage::ClientRequest {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        PaxosMessage::RecoveryRequest {
            transaction_id: tid,
            ..
        } => *tid = Some(transaction_id.clone()),
        _ => {} // Other message types don't need a transaction ID
    }

    let response_receiver = conn_mgr
        .transaction_manager
        .register_transaction(transaction_id.clone())
        .await;

    send_message(message, addr, conn_mgr).await?;

    match timeout(ACK_TIMEOUT, response_receiver).await {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(_)) => {
            error!("Transaction channel closed without response from {}", addr);
            conn_mgr
                .transaction_manager
                .cancel_transaction(&transaction_id)
                .await;
            conn_mgr.remove(addr).await;
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Transaction channel closed",
            ))
        }
        Err(_) => {
            error!("Timeout waiting for response from {}", addr);
            conn_mgr
                .transaction_manager
                .cancel_transaction(&transaction_id)
                .await;
            conn_mgr.remove(addr).await;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Timeout waiting for response",
            ))
        }
    }
}
