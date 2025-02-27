use bincode;
use tokio::io;
use tokio::net::UdpSocket;

use crate::base_libs::_paxos_types::PaxosMessage;

pub async fn send_message(socket: &UdpSocket, message: PaxosMessage, addr: &str) -> io::Result<()> {
    let serialized = bincode::serialize(&message).unwrap();
    socket.send_to(&serialized, addr).await?;
    Ok(())
}

pub async fn receive_message(socket: &UdpSocket) -> io::Result<(PaxosMessage, String)> {
    let mut buffer = vec![0; 65536];
    let (size, src) = socket.recv_from(&mut buffer).await?;
    let message: PaxosMessage = match bincode::deserialize(&buffer[..size]) {
        Ok(msg) => msg,
        Err(e) => {
            eprintln!("Failed to deserialize PaxosMessage: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };
    Ok((message, src.to_string()))
}
