use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

use crate::base_libs::network::_address::Address;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub nodes: Vec<NodeConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    pub erasure_coding: bool,
    pub shard_count: usize,
    pub parity_count: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeConfig {
    pub ip: String,
    pub port: u16,
    pub http_port: u16,
    pub memcached: MemcachedConfig,
    pub rocks_db: RocksDbConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MemcachedConfig {
    pub ip: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RocksDbConfig {
    pub path: String,
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

    pub fn get_node_index(config: &Config, address: &Address) -> Option<usize> {
        for (index, node) in config.nodes.iter().enumerate() {
            if node.ip == address.ip && node.port == address.port {
                return Some(index);
            }
        }

        None
    }

    pub fn get_node_addresses(config: &Config) -> Vec<Address> {
        config
            .nodes
            .iter()
            .map(|node| Address::new(&node.ip, node.port))
            .collect()
    }
}
