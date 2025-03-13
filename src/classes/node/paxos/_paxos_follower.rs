use tokio::{io, net::TcpStream};

use crate::{
    base_libs::{
        _operation::Operation, _paxos_types::PaxosMessage, network::_messages::reply_message,
    },
    classes::node::_node::Node,
};

impl Node {
    // ---Handlers---
    pub async fn follower_handle_leader_request(
        &self,
        src_addr: &String,
        stream: TcpStream,
        request_id: u64,
    ) {
        let leader_addr = match &self.leader_address {
            Some(addr) => addr.to_string(),
            None => {
                println!("Leader address is not set");
                return;
            }
        };
        let leader_addr = &leader_addr as &str;
        if src_addr != leader_addr {
            println!("Follower received request message from not a leader");
            return;
        }

        println!("Follower received request message from leader");
        let ack = PaxosMessage::FollowerAck {
            request_id,
            source: self.address.to_string(),
        };
        reply_message(ack, stream).await.unwrap();
        println!("Follower acknowledged request ID: {}", request_id);
    }
    pub async fn follower_handle_leader_accepted(
        &mut self,
        src_addr: &String,
        stream: TcpStream,
        request_id: u64,
        operation: &Operation,
    ) -> Result<(), io::Error> {
        let leader_addr = match &self.leader_address {
            Some(addr) => addr.to_string(),
            None => {
                println!("Leader address is not set");
                return Ok(());
            }
        };
        let leader_addr = &leader_addr as &str;

        if src_addr != leader_addr {
            println!("Follower received request message from not a leader");
            return Ok(());
        }

        self.request_id = request_id;
        self.store.persistent.process_request(operation);

        if !self.store.memory.get(&operation.kv.key).is_none() {
            self.store.memory.remove(&operation.kv.key);
        }

        println!(
            "Follower received accept message from leader:\nKey: {}, Shard: {:?}",
            operation.kv.key, operation.kv.value
        );
        let ack = PaxosMessage::FollowerAck {
            request_id,
            source: self.address.to_string(),
        };
        reply_message(ack, stream).await.unwrap();
        println!("Follower acknowledged request ID: {}", request_id);

        Ok(())
    }

    // _TODO: handle false message
    pub async fn follower_handle_follower_ack(&self, _src_addr: &String, _request_id: u64) {}
}
