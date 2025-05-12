use bincode;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::base_libs::_paxos_types::PaxosMessage;

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
            eprintln!("Failed to deserialize PaxosMessage: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };

    Ok((stream, message))
}

pub async fn send_message(message: PaxosMessage, addr: &str) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect(addr).await?;
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write(&serialized).await?;

    Ok(stream)
}

pub async fn reply_message(message: PaxosMessage, mut stream: TcpStream) -> io::Result<TcpStream> {
    let serialized = bincode::serialize(&message).unwrap();
    stream
        .write_all(&(serialized.len() as u32).to_be_bytes())
        .await?;
    stream.write(&serialized).await?;
    Ok(stream)
}

pub async fn reply_string(string: &str, mut stream: TcpStream) -> io::Result<TcpStream> {
    let len = string.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(string.as_bytes()).await?;
    Ok(stream)
}

pub async fn receive_message(mut stream: TcpStream) -> io::Result<(TcpStream, PaxosMessage)> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;

    let message: PaxosMessage = match bincode::deserialize(&buffer) {
        Ok(msg) => msg,
        Err(e) => {
            eprintln!("Failed to deserialize PaxosMessage: {:?}", e);
            return Err(io::Error::new(io::ErrorKind::InvalidData, e));
        }
    };

    Ok((stream, message))
}

pub async fn receive_string(mut stream: TcpStream) -> io::Result<(TcpStream, String)> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut buffer = vec![0; len];
    stream.read_exact(&mut buffer).await?;

    let string = String::from_utf8(buffer).expect("Invalid UTF-8 received");

    Ok((stream, string))
}
