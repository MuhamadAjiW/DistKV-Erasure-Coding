use crate::standard::base_libs::_types::OmniPaxosECMessage;
use bincode;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::error;

// Send a batch of messages
pub async fn send_omnipaxos_message(
    messages: Vec<OmniPaxosECMessage>,
    addr: &str,
    local_sender: Option<mpsc::Sender<OmniPaxosECMessage>>,
) -> io::Result<TcpStream> {
    if let Some(sender) = local_sender {
        // If a local mpsc sender is provided, use it for local delivery
        for message in messages {
            let _ = sender.send(message).await;
        }
        // Return a dummy TcpStream (not used)
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Local delivery, no TCP",
        ));
    }
    let mut stream = TcpStream::connect(addr).await?;
    let serialized = bincode::serialize(&messages).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write_all(&serialized).await?;
    Ok(stream)
}

// For compatibility: send a single message
pub async fn send_omnipaxos_message_single(
    message: OmniPaxosECMessage,
    addr: &str,
    local_sender: Option<mpsc::Sender<OmniPaxosECMessage>>,
) -> io::Result<TcpStream> {
    send_omnipaxos_message(vec![message], addr, local_sender).await
}

// Receive a batch of messages
pub async fn receive_omnipaxos_message(
    socket: &TcpListener,
) -> io::Result<(TcpStream, Vec<OmniPaxosECMessage>)> {
    let (mut stream, _src) = socket.accept().await?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;
    let messages: Vec<OmniPaxosECMessage> = match bincode::deserialize(&buffer) {
        Ok(msgs) => msgs,
        Err(e) => {
            error!("Failed to deserialize OmnipaxosMessage batch: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };
    Ok((stream, messages))
}
