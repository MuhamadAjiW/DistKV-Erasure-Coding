use crate::base_libs::_types::OmniPaxosECMessage;
use bincode;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::error;

pub async fn send_omnipaxos_message(
    message: OmniPaxosECMessage,
    addr: &str,
    local_sender: Option<mpsc::Sender<OmniPaxosECMessage>>,
) -> io::Result<TcpStream> {
    if let Some(sender) = local_sender {
        // If a local mpsc sender is provided, use it for local delivery
        let _ = sender.send(message).await;
        // Return a dummy TcpStream (not used)
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "Local delivery, no TCP",
        ));
    }
    let mut stream = TcpStream::connect(addr).await?;
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write(&serialized).await?;
    Ok(stream)
}

pub async fn receive_omnipaxos_message(
    socket: &TcpListener,
) -> io::Result<(TcpStream, OmniPaxosECMessage)> {
    let (mut stream, _src) = socket.accept().await?;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;
    let message: OmniPaxosECMessage = match bincode::deserialize(&buffer) {
        Ok(msg) => msg,
        Err(e) => {
            error!("Failed to deserialize OmnipaxosMessage: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };
    Ok((stream, message))
}
