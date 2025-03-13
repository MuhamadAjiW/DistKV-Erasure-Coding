use std::io;

use distkv::{
    base_libs::network::_address::Address,
    classes::node::{_node::Node, paxos::_paxos::PaxosState},
};

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

            let mut node = Node::from_config(address, &configuration_input).await;
            println!("[INIT] Node started with address: {}", &node.address);

            node.run().await?;
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

            // let socket = UdpSocket::bind("127.0.0.1:50000")
            //     .await
            //     .expect("Failed to bind socket");
            // println!("Sending message to {}...", node_addr_input);
            // socket.send_to(&data, node_addr_input).await?;

            // println!("Waiting for response...");
            // let mut buf = [0; 65536];
            // let (size, client_addr) = socket.recv_from(&mut buf).await.unwrap();
            // let message = String::from_utf8_lossy(&buf[..size]).to_string();
            // println!("Client received reply from {}: {}", client_addr, message);
        }
        _ => {
            panic!("Invalid command! Use 'node' or 'client'.");
        }
    }

    Ok(())
}
