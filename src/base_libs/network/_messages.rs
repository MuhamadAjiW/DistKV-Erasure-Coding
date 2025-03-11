use bincode;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

use crate::base_libs::_paxos_types::PaxosMessage;

pub async fn send_bytes(message: &[u8], addr: &str) -> io::Result<()> {
    println!("[SOCKET] Sending message to {}", addr);
    let mut stream = TcpStream::connect(addr).await?;
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write(&serialized).await?;
    println!("[SOCKET] Sent {} bytes to {}", serialized.len(), addr);
    Ok(())
}

pub async fn send_message(message: PaxosMessage, addr: &str) -> io::Result<()> {
    println!("[SOCKET] Sending message to {}", addr);
    let mut stream = TcpStream::connect(addr).await?;
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write(&serialized).await?;
    println!("[SOCKET] Sent {} bytes to {}", serialized.len(), addr);
    Ok(())
}

pub async fn receive_message(socket: &TcpListener) -> io::Result<(PaxosMessage, String)> {
    let (mut stream, src) = socket.accept().await?;
    println!(
        "[SOCKET] Received connection from {}",
        stream.peer_addr().unwrap()
    );
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;
    println!("[SOCKET] Received {} bytes from {}", len, src);

    let message: PaxosMessage = match bincode::deserialize(&buffer) {
        Ok(msg) => msg,
        Err(e) => {
            eprintln!("Failed to deserialize PaxosMessage: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };
    Ok((message, src.to_string()))
}

pub async fn receive_bytes(socket: &TcpListener) -> io::Result<(Vec<u8>, String)> {
    let (mut stream, src) = socket.accept().await?;
    println!("[SOCKET] Connection accepted from {}", src);

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let msg_len = u32::from_be_bytes(len_buf) as usize;

    let mut buffer = vec![0; msg_len];
    stream.read_exact(&mut buffer).await?;

    Ok((buffer, src.to_string()))
}
