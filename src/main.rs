use std::{io, sync::Arc};

use distkv::{
    base_libs::{
        _operation::Operation,
        _paxos_types::PaxosMessage,
        network::{
            _address::Address,
            _messages::{receive_string, send_message},
        },
    },
    classes::node::_node::Node,
};
use tokio::sync::Mutex;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let role = std::env::args().nth(1).expect("No role provided");

    match role.as_str() {
        "node" => {
            let follower_addr_input = std::env::args().nth(2).expect("No address provided");
            let configuration_input = std::env::args()
                .nth(3)
                .expect("No configuration file provided");

            let address = Address::from_string(&follower_addr_input).unwrap();
            println!("[INIT] Starting node with address: {}", &address);

            let node = Node::from_config(address, &configuration_input).await;
            println!("[INIT] Node started with address: {}", &node.address);

            let node_arc = Arc::new(Mutex::new(node));
            Node::run(node_arc).await;
        }
        "client" => {
            println!("Client starting...");
            let node_addr_input = std::env::args().nth(2).expect("No node address provided");

            let mut data = std::env::args()
                .nth(3)
                .expect("No request count provided")
                .as_bytes()
                .to_vec();

            if data.len() >= 3 && &data[0..3] == b"SET" {
                let data_input = std::env::args().nth(4).expect("No data provided");
                let data_count_input = std::env::args().nth(5).expect("No data count provided");

                let data_count = data_count_input
                    .parse()
                    .expect("Invalid data count parameter");
                let mut data_repeated = data_input.repeat(data_count).as_bytes().to_vec();

                data.append(&mut data_repeated);
            }

            let operation = Operation::parse(&data).unwrap();
            match send_message(
                PaxosMessage::ClientRequest {
                    operation,
                    source: "CLIENT".to_string(),
                },
                &node_addr_input,
            )
            .await
            {
                Ok(stream) => {
                    let response = receive_string(stream).await.unwrap().1;
                    println!("Reply: {}", response);
                }
                Err(e) => {
                    eprintln!("Failed to send message: {}", e);
                }
            }
        }
        _ => {
            panic!("Invalid command! Use 'node' or 'client'.");
        }
    }

    Ok(())
}
