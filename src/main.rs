use std::{io, sync::Arc};

use distkv::{
    base_libs::{_operation::Operation, network::_address::Address},
    classes::node::_node::Node,
};
use tokio::sync::RwLock;
use tracing::{error, info, instrument};
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
            let subscriber = if *trace {
                tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::DEBUG)
                    .with_file(true)
                    .with_target(false)
                    .with_ansi(false)
                    .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
                    .finish()
            } else {
                tracing_subscriber::fmt()
                    .with_max_level(tracing::Level::INFO)
                    .with_file(false)
                    .with_target(false)
                    .with_ansi(false)
                    .finish()
            };
            tracing::subscriber::set_global_default(subscriber)
                .expect("Failed to set global default subscriber");

            let address = Address::from_string(addr).unwrap();
            info!("[INIT] Starting node with address: {}", &address);

            let node = Node::from_config(address, conf).await;
            info!("[INIT] Node started with address: {}", &node.address);

            let node_arc = Arc::new(RwLock::new(node));
            Node::run(node_arc).await;
        }
        Commands::Client {
            node_addr,
            op,
            data,
            count,
            trace,
        } => {
            if *trace {
                let subscriber = tracing_subscriber::fmt().finish();
                tracing::subscriber::set_global_default(subscriber)
                    .expect("Failed to set global default subscriber");
            }

            // Build the operation string
            let mut operation_str = op.clone();
            if op.starts_with("SET") {
                if let Some(data_to_repeat) = data {
                    let repeated = data_to_repeat.repeat(*count);
                    operation_str = format!("{} {}", op, repeated);
                }
            }
            let operation = Operation::from_string(&operation_str);

            // Send the operation to the node (TCP client)
            use bincode;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            use tokio::net::TcpStream;

            let mut stream = TcpStream::connect(node_addr)
                .await
                .expect("Failed to connect to node");
            let serialized = bincode::serialize(&operation).unwrap();
            stream
                .write_all(&(serialized.len() as u32).to_be_bytes())
                .await
                .expect("Failed to write length");
            stream
                .write_all(&serialized)
                .await
                .expect("Failed to write operation");

            // Read response (assuming a simple string reply)
            let mut len_buf = [0u8; 4];
            stream
                .read_exact(&mut len_buf)
                .await
                .expect("Failed to read response length");
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buffer = vec![0; len];
            stream
                .read_exact(&mut buffer)
                .await
                .expect("Failed to read response");
            let response = String::from_utf8_lossy(&buffer);
            println!("Response: {}", response);
        }
    }

    Ok(())
}
