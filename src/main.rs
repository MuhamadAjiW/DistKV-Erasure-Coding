use std::{io, sync::Arc};

use distkv::{
    config::_address::Address, ec::classes::node::_node::Node as ECNode,
    standard::classes::node::_node::Node,
};
use tokio::sync::RwLock;
use tracing::{info, instrument};
use tracing_subscriber::fmt::format::FmtSpan;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The address for this node (e.g., "127.0.0.1:8080")
    #[arg(long, short)]
    addr: String,

    /// Path to the configuration file
    #[arg(long, short)]
    conf: String,

    /// Optional: Enable tracing for debugging
    #[arg(long, short, default_value_t = false)]
    trace: bool,

    /// Optional: Enable erasure coding
    #[arg(long, short, default_value_t = false)]
    erasure: bool,
}

#[tokio::main]
#[instrument]
async fn main() -> Result<(), io::Error> {
    let cli = Cli::parse();

    let subscriber = if cli.trace {
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

    let address = Address::from_string(&cli.addr).unwrap();
    info!("[INIT] Starting node with address: {}", &address);

    if cli.erasure {
        let node = ECNode::from_config(address, &cli.conf).await;
        info!(
            "[INIT] Node started with address without erasure coding: {}",
            &node.address
        );
        let node_arc = Arc::new(RwLock::new(node));
        ECNode::run(node_arc).await;
    } else {
        let node = Node::from_config(address, &cli.conf).await;
        info!(
            "[INIT] Node started with address with erasure coding: {}",
            &node.address
        );
        let node_arc = Arc::new(RwLock::new(node));
        Node::run(node_arc).await;
    }

    Ok(())
}
