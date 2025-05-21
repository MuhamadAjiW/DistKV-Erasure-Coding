use std::{path::PathBuf, time::Duration};

use distkv::{base_libs::network::_address::Address, classes::config::_config::Config};
use tempfile::{tempdir, TempDir};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};

pub struct TestNodeHandle {
    pub address: Address,
    pub http_address: Address,
    _child: Child,
    _temp_dir: TempDir,
}

pub struct TestCluster {
    pub nodes: Vec<TestNodeHandle>,
}

impl TestCluster {
    pub const TEST_CLUSTER_CONFIG_FILE: &str = "./etc/test_config.json";

    pub async fn new(num_nodes: usize) -> Self {
        let config_file_path = PathBuf::from(TestCluster::TEST_CLUSTER_CONFIG_FILE);
        let base_config: Config = Config::get_config(config_file_path.to_str().unwrap()).await;

        if num_nodes == 0 || num_nodes > base_config.nodes.len() {
            panic!(
                "Requested {} nodes, but config only has {} nodes. Please request between 1 and {} nodes.",
                num_nodes,
                base_config.nodes.len(),
                base_config.nodes.len()
            );
        }

        let mut node_handles = Vec::with_capacity(num_nodes);

        for i in 0..num_nodes {
            let original_node_config = &base_config.nodes[i];

            let node_temp_dir = tempdir().expect("Failed to create temp directory for node");
            let node_db_path = node_temp_dir
                .path()
                .join("rocksdb")
                .to_str()
                .unwrap()
                .to_string();
            let node_tlog_path = node_temp_dir
                .path()
                .join("transaction.log")
                .to_str()
                .unwrap()
                .to_string();

            let mut node_specific_cluster_config = base_config.clone();

            if let Some(node_cfg_to_update) = node_specific_cluster_config.nodes.get_mut(i) {
                node_cfg_to_update.rocks_db.path = node_db_path;
                node_cfg_to_update.transaction_log = node_tlog_path;
            } else {
                panic!("Failed to find node at index {} in cloned config.", i);
            }

            let node_config_temp_path = node_temp_dir
                .path()
                .join(format!("node_{}_config.json", original_node_config.port));
            tokio::fs::write(
                &node_config_temp_path,
                serde_json::to_string_pretty(&node_specific_cluster_config).unwrap(),
            )
            .await
            .expect("Failed to write node-specific temporary config file");

            let tcp_addr = Address::new("127.0.0.1", original_node_config.port);
            let http_addr = Address::new("127.0.0.1", original_node_config.http_port);

            println!(
                "[Test Setup] Spawning node {} at TCP:{} HTTP:{} using config: {:?}",
                i, tcp_addr, http_addr, node_config_temp_path
            );

            let mut executable_path =
                std::env::current_exe().expect("Failed to get current executable path");

            println!(
                "[Test Setup] Current executable path: {:?}",
                executable_path
            );

            executable_path.pop(); // Removes the hash
            executable_path.pop(); // Removes the deps

            let distkv_bin_path = executable_path.join("distkv");

            if !distkv_bin_path.exists() {
                panic!("Compiled distkv binary not found at expected path: {:?}. Try running `cargo build` first.", distkv_bin_path);
            }
            println!(
                "[Test Setup] Identified distkv binary at: {:?}",
                distkv_bin_path
            );

            let mut child = Command::new(&executable_path)
                .arg("node")
                .arg(&tcp_addr.to_string())
                .arg(node_config_temp_path.to_str().unwrap())
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .expect(&format!("Failed to spawn node process for {}", tcp_addr));

            let stdout_reader = BufReader::new(child.stdout.take().unwrap());
            let stderr_reader = BufReader::new(child.stderr.take().unwrap());
            let node_stdout_addr = tcp_addr.to_string();
            tokio::spawn(async move {
                let mut lines = stdout_reader.lines();
                while let Some(line) = lines.next_line().await.unwrap() {
                    println!("[Node {} STDOUT] {}", node_stdout_addr, line);
                }
            });
            let node_stderr_addr = tcp_addr.to_string();
            tokio::spawn(async move {
                let mut lines = stderr_reader.lines();
                while let Some(line) = lines.next_line().await.unwrap() {
                    eprintln!("[Node {} STDERR] {}", node_stderr_addr, line);
                }
            });

            node_handles.push(TestNodeHandle {
                address: tcp_addr,
                http_address: http_addr,
                _child: child,
                _temp_dir: node_temp_dir,
            });
        }

        tokio::time::sleep(Duration::from_secs(5)).await;

        Self {
            nodes: node_handles,
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        println!("[Test Cleanup] Tearing down cluster...");
        for node in &mut self.nodes {
            let _ = node._child.kill();
            let _ = node._child.wait();
            println!("[Test Cleanup] Node {} killed.", node.address.to_string());
        }
        println!("[Test Cleanup] All temporary node data cleaned up.");
    }
}
