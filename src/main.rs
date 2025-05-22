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
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    // Run as a DistKV node
    Node {
        // The address for this node (e.g., "127.0.0.1:8080")
        #[arg(long, short)]
        addr: String,

        // Path to the configuration file
        #[arg(long, short)]
        conf: String,

        // Optional: Enable tracing for debugging
        #[arg(long, short, default_value_t = false)]
        trace: bool,
    },
    // Run as a DistKV client
    Client {
        // The address of a DistKV node to connect to (e.g., "127.0.0.1:8080")
        #[arg(long, short = 'n')]
        node_addr: String,

        // The operation to perform (e.g., "GET key", "SET key value", "DEL key")
        #[arg(long, short)]
        op: String,

        // Optional: Data to set for SET operations.
        #[arg(long, short)]
        data: Option<String>,

        // Optional: Number of times to repeat the data for SET operations.
        #[arg(long, short, default_value_t = 1)]
        count: usize,

        // Optional: Enable tracing for debugging
        #[arg(long, short, default_value_t = false)]
        trace: bool,
    },
}

#[tokio::main]
#[instrument]
async fn main() -> Result<(), io::Error> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Node { addr, conf, trace } => {
            println!("Node starting...");

            if *trace {
                println!("[INIT] Tracing enabled");
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::DEBUG)
                    .with_file(true)
                    .with_target(false)
                    .with_ansi(false)
                    .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
                    .finish();

                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set global default subscriber");
            } else {
                println!("[INIT] Tracing disabled");
            }

            let address = Address::from_string(addr).unwrap();
            println!("[INIT] Starting node with address: {}", &address);

            let node = Node::from_config(address, conf).await;
            println!("[INIT] Node started with address: {}", &node.address);

            let node_arc = Arc::new(Mutex::new(node));
            Node::run(node_arc).await;
        }
        Commands::Client {
            node_addr,
            op,
            data,
            count,
            trace,
        } => {
            println!("Client starting...");

            if *trace {
                println!("[INIT] Tracing enabled");
                let subscriber = tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::DEBUG)
                    .with_file(true)
                    .with_target(false)
                    .with_ansi(false)
                    .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
                    .finish();

                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set global default subscriber");
            } else {
                println!("[INIT] Tracing disabled");
            }

            let mut operation_bytes = op.as_bytes().to_vec();

            if op.starts_with("SET") {
                if let Some(data_to_repeat) = data {
                    let mut repeated_data = data_to_repeat.repeat(*count).as_bytes().to_vec();
                    operation_bytes.append(&mut repeated_data);
                } else {
                    eprintln!("Error: 'SET' operation requires '--data' argument.");
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "SET operation requires data",
                    ));
                }
            }

            let operation = Operation::parse(&operation_bytes).unwrap();
            match send_message(
                PaxosMessage::ClientRequest {
                    operation,
                    source: "CLIENT".to_string(),
                },
                node_addr,
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
    }

    Ok(())
}
