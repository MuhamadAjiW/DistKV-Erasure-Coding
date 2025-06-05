use bincode;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::error;

use crate::base_libs::_paxos_types::PaxosMessage;
use crate::base_libs::network::_connection::ConnectionManager;

pub async fn listen(socket: &TcpListener) -> io::Result<(TcpStream, PaxosMessage)> {
    let (mut stream, _src) = socket.accept().await?;
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

    Ok((stream, message))
}

pub async fn send_message(
    message: PaxosMessage,
    addr: &str,
    conn_mgr: &ConnectionManager,
) -> io::Result<()> {
    // info!("Sending message to {}", addr);

    let stream_arc = conn_mgr.get_or_connect(addr).await?;
    let mut stream = stream_arc.lock().await;
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&serialized).await?;
    Ok(())
}

/// Send a message using the connection pool and receive a response (request/response pattern).
/// Locks the stream for the entire send/receive cycle.
pub async fn send_message_and_receive_response(
    message: PaxosMessage,
    addr: &str,
    conn_mgr: &ConnectionManager,
) -> io::Result<PaxosMessage> {
    let stream_arc = conn_mgr.get_or_connect(addr).await?;
    let mut stream = stream_arc.lock().await;
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&serialized).await?;

    // Now read the response
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;
    let response: PaxosMessage = match bincode::deserialize(&buffer) {
        Ok(msg) => msg,
        Err(e) => {
            error!("Failed to deserialize PaxosMessage: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };
    Ok(response)
}
