use std::io;

use distkv::{
    base_libs::network::_address::Address,
    classes::node::{_node::Node, paxos::_paxos::PaxosState},
};
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> Result<(), io::Error> {
    let role = std::env::args().nth(1).expect("No role provided");
    let ec_active = true;

    match role.as_str() {
        "leader" => {
            let addr_input = std::env::args().nth(2).expect("No leader address provided");
            let load_balancer_addr_input = std::env::args()
                .nth(3)
                .expect("No load balancer address provided");
            let shard_count = std::env::args()
                .nth(4)
                .expect("No shard count provided")
                .parse()
                .unwrap();
            let parity_count = std::env::args()
                .nth(5)
                .expect("No parity count provided")
                .parse()
                .unwrap();

            let address = Address::from_string(&addr_input).unwrap();
            let leader_address = Address::from_string(&addr_input).unwrap();
            let load_balancer_address = Address::from_string(&load_balancer_addr_input).unwrap();

            let mut node = Node::new(
                address,
                leader_address,
                load_balancer_address,
                PaxosState::Leader,
                shard_count,
                parity_count,
                ec_active,
            )
            .await;

            node.run().await?;
        }
        "follower" => {
            let follower_addr_input = std::env::args()
                .nth(2)
                .expect("No follower address provided");
            let leader_addr_input = std::env::args().nth(3).expect("No leader address provided");
            let load_balancer_addr_input = std::env::args()
                .nth(4)
                .expect("No load balancer address provided");
            let shard_count = std::env::args()
                .nth(5)
                .expect("No shard count provided")
                .parse()
                .unwrap();
            let parity_count = std::env::args()
                .nth(6)
                .expect("No parity count provided")
                .parse()
                .unwrap();

            let address = Address::from_string(&follower_addr_input).unwrap();
            let leader_address = Address::from_string(&leader_addr_input).unwrap();
            let load_balancer_address = Address::from_string(&load_balancer_addr_input).unwrap();
            let mut node = Node::new(
                address,
                leader_address,
                load_balancer_address,
                PaxosState::Follower,
                shard_count,
                parity_count,
                ec_active,
            )
            .await;

            node.run().await?;
        }
        "client" => {
            println!("Client starting...");
            let load_balancer_addr_input = std::env::args()
                .nth(2)
                .expect("No load balancer address provided");

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

            let socket = UdpSocket::bind("127.0.0.1:50000")
                .await
                .expect("Failed to bind socket");
            println!("Sending message to {}...", load_balancer_addr_input);
            socket.send_to(&data, load_balancer_addr_input).await?;

            println!("Waiting for response...");
            let mut buf = [0; 65536];
            let (size, client_addr) = socket.recv_from(&mut buf).await.unwrap();
            let message = String::from_utf8_lossy(&buf[..size]).to_string();
            println!("Client received reply from {}: {}", client_addr, message);
        }
        _ => {
            panic!("Invalid role! Use 'leader', 'follower', 'client', or 'load_balancer'.");
        }
    }

    Ok(())
}
