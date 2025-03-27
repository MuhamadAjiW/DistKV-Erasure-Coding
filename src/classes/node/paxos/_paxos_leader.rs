use tokio::{io, net::TcpStream};

use crate::{
    base_libs::{_operation::Operation, _paxos_types::PaxosMessage},
    classes::node::_node::Node,
};

impl Node {
    pub async fn send_heartbeat(&mut self) -> Result<(), io::Error> {
        let heartbeat = PaxosMessage::Heartbeat {
            request_id: self.request_id,
            source: self.address.to_string(),
        };
        self.broadcast_message(heartbeat).await;

        Ok(())
    }

    // _TODO: handle false leader
    pub async fn leader_handle_leader_request(
        &self,
        _src_addr: &String,
        _stream: TcpStream,
        _request_id: u64,
    ) {
    }
    pub async fn leader_handle_leader_accepted(
        &self,
        _src_addr: &String,
        _stream: TcpStream,
        _request_id: u64,
        _operation: &Operation,
    ) {
    }

    // _TODO: handle false message
    pub async fn leader_handle_follower_ack(
        &self,
        _src_addr: &String,
        _stream: TcpStream,
        _request_id: u64,
    ) {
    }
}
