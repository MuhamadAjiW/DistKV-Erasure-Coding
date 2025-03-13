use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

use crate::base_libs::network::_address::Address;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    shard_count: u32,
    parity_count: u32,
    nodes: Vec<NodeConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeConfig {
    ip: String,
    port: u16,
    memcached: MemcachedConfig,
    rocks_db: RocksDbConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MemcachedConfig {
    ip: String,
    port: u16,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RocksDbConfig {
    path: String,
}

impl Config {
    pub async fn get_node_config(
        address: Address,
        config_path: &str,
    ) -> Option<(usize, NodeConfig)> {
        let config_str = match fs::read_to_string(Path::new(config_path)) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Failed to read config file: {}", e);
                return None;
            }
        };

        let config: Config = match serde_json::from_str(&config_str) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("Failed to parse config JSON: {}", e);
                return None;
            }
        };

        // Find the node with the matching address and return its index
        for (index, node) in config.nodes.iter().enumerate() {
            if node.ip == address.ip && node.port == address.port {
                return Some((index, node.clone()));
            }
        }

        eprintln!("No node found for address: {}", address);
        None
    }

    pub async fn get_config(config_path: &str) -> Config {
        let config_str = match fs::read_to_string(Path::new(config_path)) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Failed to read config file: {}", e);
                panic!("Failed to read config file");
            }
        };

        match serde_json::from_str(&config_str) {
            Ok(cfg) => cfg,
            Err(e) => {
                eprintln!("Failed to parse config JSON: {}", e);
                panic!("Failed to parse config JSON");
            }
        }
    }
}
