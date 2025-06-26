# Copilot Instructions for DistKV-Erasure-Coding

## Project Overview

This is a **distributed key-value store system** implementation for a thesis project, combining **Paxos consensus protocol** with **erasure coding** for fault-tolerant distributed storage. Built with Rust and Tokio async runtime, the system demonstrates advanced distributed systems concepts including consensus algorithms, fault tolerance, and efficient data encoding strategies.

## CRITICAL BEHAVIORAL REQUIREMENTS

### 🚨 MANDATORY RESPONSE GUIDELINES

- **ALWAYS provide comprehensive, production-quality distributed systems solutions** - No simplified implementations or academic shortcuts
- **ACTIVELY suggest performance optimizations and reliability improvements** - Enhance throughput, reduce latency, improve fault tolerance
- **PROACTIVELY identify distributed systems challenges** - Point out consensus edge cases, network partition scenarios, and timing issues
- **VERIFY ALL DISTRIBUTED SYSTEMS CONCEPTS** - Never hallucinate Paxos phases, erasure coding parameters, or Rust async patterns
- **USE RESEARCH TOOLS EXTENSIVELY** - Stay current with distributed systems research and Rust ecosystem updates
- **THINK STEP-BY-STEP FOR CONSENSUS PROTOCOLS** - Break down complex consensus scenarios and explain state transitions
- **ALWAYS UPDATE FILE DIRECTLY** - UNLESS STATED OTHERWISE BY THE USER TO NOT CODE, NEVER ONLY SUGGEST. Always edit the file directly with your implementation.

### 📋 RESPONSE STRUCTURE REQUIREMENTS

1. **Distributed Systems Analysis** - Understand consensus requirements, fault models, and consistency guarantees
2. **Architecture Overview** - Explain system design, node roles, and communication patterns
3. **Implementation Details** - Provide complete, async-safe Rust code with proper error handling
4. **Consensus & Coding Strategy** - Address Paxos phases, erasure coding parameters, and recovery procedures
5. **Performance & Reliability** - Include benchmarking considerations, fault tolerance testing, and scalability analysis
6. **Testing & Validation** - Suggest consensus testing strategies, network partition simulations, and correctness verification

## Tech Stack & Architecture

### Core Technologies

- **Rust 1.75+** with strict safety guarantees and zero-cost abstractions
- **Tokio 1.35+** for async runtime and high-performance networking
- **UDP Protocol** for low-latency node communication
- **Paxos Consensus** for distributed agreement and consistency
- **Erasure Coding** for efficient fault tolerance and storage optimization
- **Serde** for efficient serialization/deserialization of distributed messages

### System Architecture Components

- **Load Balancer** - Request distribution and client connection management
- **Leader Node** - Paxos proposer role and consensus coordination
- **Follower Nodes** - Paxos acceptor/learner roles and data storage
- **Consensus Layer** - Multi-Paxos implementation for log replication
- **Storage Layer** - Erasure-coded data storage and retrieval
- **Network Layer** - UDP-based reliable message passing with retries

### Distributed Systems Concepts

- **Byzantine Fault Tolerance** considerations and failure models
- **CAP Theorem** trade-offs (Consistency, Availability, Partition tolerance)
- **Eventual Consistency** vs **Strong Consistency** guarantees
- **Split-brain Prevention** and quorum-based decision making
- **Network Partition Handling** and recovery procedures
- **Leader Election** algorithms and failover mechanisms

## Rust Implementation Patterns

### Async Programming Standards

```rust
// ✅ PREFERRED: Proper async error handling with distributed systems context
use tokio::{net::UdpSocket, time::{timeout, Duration}};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    Prepare { proposal_id: u64, instance_id: u64 },
    Promise { proposal_id: u64, instance_id: u64, accepted_value: Option<Vec<u8>> },
    Accept { proposal_id: u64, instance_id: u64, value: Vec<u8> },
    Accepted { proposal_id: u64, instance_id: u64, value: Vec<u8> },
    Heartbeat { leader_id: Uuid, term: u64 },
}

#[derive(Debug, thiserror::Error)]
pub enum DistributedError {
    #[error("Consensus timeout: failed to reach quorum within {timeout_ms}ms")]
    ConsensusTimeout { timeout_ms: u64 },
    #[error("Network partition detected: lost connection to {node_count} nodes")]
    NetworkPartition { node_count: usize },
    #[error("Erasure coding error: {details}")]
    ErasureCoding { details: String },
    #[error("Invalid proposal: {reason}")]
    InvalidProposal { reason: String },
}

pub struct PaxosNode {
    node_id: Uuid,
    socket: UdpSocket,
    peers: HashMap<Uuid, SocketAddr>,
    state: NodeState,
    storage: ErasureCodedStorage,
    consensus_timeout: Duration,
}

impl PaxosNode {
    pub async fn new(
        node_id: Uuid,
        bind_addr: SocketAddr,
        peers: HashMap<Uuid, SocketAddr>,
    ) -> Result<Self, DistributedError> {
        let socket = UdpSocket::bind(bind_addr).await
            .map_err(|e| DistributedError::NetworkPartition {
                node_count: peers.len()
            })?;

        Ok(Self {
            node_id,
            socket,
            peers,
            state: NodeState::Follower,
            storage: ErasureCodedStorage::new(3, 2)?, // 3 data blocks, 2 parity blocks
            consensus_timeout: Duration::from_millis(5000),
        })
    }

    pub async fn start_consensus_round(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<bool, DistributedError> {
        let instance_id = self.generate_instance_id();
        let proposal_id = self.generate_proposal_id();

        // Phase 1: Prepare
        let promises = self.send_prepare(proposal_id, instance_id).await?;
        if !self.has_majority(promises.len()) {
            return Err(DistributedError::ConsensusTimeout {
                timeout_ms: self.consensus_timeout.as_millis() as u64
            });
        }

        // Phase 2: Accept
        let accepts = self.send_accept(proposal_id, instance_id, value).await?;
        Ok(self.has_majority(accepts.len()))
    }

    async fn send_prepare(
        &self,
        proposal_id: u64,
        instance_id: u64,
    ) -> Result<Vec<ConsensusMessage>, DistributedError> {
        let prepare_msg = ConsensusMessage::Prepare { proposal_id, instance_id };
        let serialized = bincode::serialize(&prepare_msg)
            .map_err(|e| DistributedError::InvalidProposal {
                reason: format!("Serialization failed: {}", e)
            })?;

        let mut promises = Vec::new();
        let futures: Vec<_> = self.peers.iter().map(|(peer_id, addr)| {
            let socket = &self.socket;
            let data = serialized.clone();
            async move {
                timeout(self.consensus_timeout, socket.send_to(&data, addr)).await
            }
        }).collect();

        // Wait for responses with timeout
        for result in futures::future::join_all(futures).await {
            if let Ok(Ok(_)) = result {
                // Process promise responses (simplified)
                // In real implementation, wait for and validate promise messages
            }
        }

        Ok(promises)
    }

    fn has_majority(&self, count: usize) -> bool {
        count > self.peers.len() / 2
    }
}
```

### Erasure Coding Integration

```rust
// ✅ PREFERRED: Efficient erasure coding with Reed-Solomon
use reed_solomon::Encoder;
use std::collections::BTreeMap;

pub struct ErasureCodedStorage {
    encoder: Encoder,
    data_shards: usize,
    parity_shards: usize,
    storage_nodes: BTreeMap<Uuid, StorageNode>,
}

impl ErasureCodedStorage {
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self, DistributedError> {
        let encoder = Encoder::new(data_shards, parity_shards)
            .map_err(|e| DistributedError::ErasureCoding {
                details: format!("Failed to create encoder: {}", e)
            })?;

        Ok(Self {
            encoder,
            data_shards,
            parity_shards,
            storage_nodes: BTreeMap::new(),
        })
    }

    pub async fn store_value(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<Vec<StorageLocation>, DistributedError> {
        // Split data into shards
        let mut shards = self.split_into_shards(value)?;

        // Generate parity shards
        self.encoder.encode(&mut shards)
            .map_err(|e| DistributedError::ErasureCoding {
                details: format!("Encoding failed: {}", e)
            })?;

        // Distribute shards across nodes
        let mut locations = Vec::new();
        for (index, shard) in shards.iter().enumerate() {
            let node_id = self.select_storage_node(index)?;
            let location = self.store_shard_on_node(node_id, &key, index, shard).await?;
            locations.push(location);
        }

        Ok(locations)
    }

    pub async fn retrieve_value(
        &self,
        key: &str,
        locations: &[StorageLocation],
    ) -> Result<Vec<u8>, DistributedError> {
        let mut shards = vec![None; self.data_shards + self.parity_shards];
        let mut retrieved_count = 0;

        // Retrieve available shards
        for location in locations {
            if let Ok(shard) = self.retrieve_shard_from_node(location).await {
                shards[location.shard_index] = Some(shard);
                retrieved_count += 1;

                // We only need data_shards to reconstruct
                if retrieved_count >= self.data_shards {
                    break;
                }
            }
        }

        if retrieved_count < self.data_shards {
            return Err(DistributedError::ErasureCoding {
                details: format!(
                    "Insufficient shards: need {}, got {}",
                    self.data_shards,
                    retrieved_count
                )
            });
        }

        // Reconstruct original data
        self.reconstruct_from_shards(shards)
    }

    fn split_into_shards(&self, data: Vec<u8>) -> Result<Vec<Vec<u8>>, DistributedError> {
        let shard_size = (data.len() + self.data_shards - 1) / self.data_shards;
        let mut shards = Vec::with_capacity(self.data_shards + self.parity_shards);

        // Create data shards
        for i in 0..self.data_shards {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, data.len());
            let mut shard = data[start..end].to_vec();

            // Pad shard to fixed size
            shard.resize(shard_size, 0);
            shards.push(shard);
        }

        // Initialize parity shards
        for _ in 0..self.parity_shards {
            shards.push(vec![0; shard_size]);
        }

        Ok(shards)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageLocation {
    pub node_id: Uuid,
    pub shard_index: usize,
    pub checksum: u32,
}
```

### Network Communication Patterns

```rust
// ✅ PREFERRED: Reliable UDP with retries and message ordering
use tokio::sync::{mpsc, RwLock};
use std::sync::Arc;

pub struct ReliableUdpTransport {
    socket: UdpSocket,
    pending_messages: Arc<RwLock<HashMap<u64, PendingMessage>>>,
    message_counter: Arc<AtomicU64>,
    retry_timeout: Duration,
    max_retries: usize,
}

#[derive(Debug, Clone)]
struct PendingMessage {
    data: Vec<u8>,
    destination: SocketAddr,
    retries: usize,
    last_sent: Instant,
}

impl ReliableUdpTransport {
    pub async fn send_reliable(
        &self,
        data: Vec<u8>,
        destination: SocketAddr,
    ) -> Result<u64, DistributedError> {
        let message_id = self.message_counter.fetch_add(1, Ordering::Relaxed);
        let message = ReliableMessage {
            id: message_id,
            data,
            message_type: MessageType::Data,
        };

        let serialized = bincode::serialize(&message)
            .map_err(|e| DistributedError::InvalidProposal {
                reason: format!("Message serialization failed: {}", e)
            })?;

        // Store for potential retry
        let pending = PendingMessage {
            data: serialized.clone(),
            destination,
            retries: 0,
            last_sent: Instant::now(),
        };

        self.pending_messages.write().await.insert(message_id, pending);

        // Send initial message
        self.socket.send_to(&serialized, destination).await
            .map_err(|e| DistributedError::NetworkPartition { node_count: 1 })?;

        Ok(message_id)
    }

    pub async fn start_retry_handler(&self) {
        let mut interval = tokio::time::interval(self.retry_timeout);

        loop {
            interval.tick().await;
            self.retry_pending_messages().await;
        }
    }

    async fn retry_pending_messages(&self) {
        let mut pending = self.pending_messages.write().await;
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for (message_id, message) in pending.iter_mut() {
            if now.duration_since(message.last_sent) > self.retry_timeout {
                if message.retries < self.max_retries {
                    // Retry sending
                    if let Ok(_) = self.socket.send_to(&message.data, message.destination).await {
                        message.retries += 1;
                        message.last_sent = now;
                    }
                } else {
                    // Max retries exceeded, remove from pending
                    to_remove.push(*message_id);
                }
            }
        }

        for id in to_remove {
            pending.remove(&id);
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ReliableMessage {
    id: u64,
    data: Vec<u8>,
    message_type: MessageType,
}

#[derive(Debug, Serialize, Deserialize)]
enum MessageType {
    Data,
    Ack,
    Heartbeat,
}
```

## Distributed Systems Testing Strategies

### Consensus Protocol Testing

```rust
// ✅ PREFERRED: Comprehensive consensus testing with fault injection
#[cfg(test)]
mod consensus_tests {
    use super::*;
    use tokio::test;
    use std::time::Duration;

    #[tokio::test]
    async fn test_basic_consensus_success() {
        let mut cluster = create_test_cluster(5).await;

        let key = "test_key".to_string();
        let value = b"test_value".to_vec();

        let result = cluster.leader_mut()
            .start_consensus_round(key.clone(), value.clone())
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());

        // Verify value is stored on all nodes
        for node in cluster.followers() {
            let stored_value = node.get_value(&key).await.unwrap();
            assert_eq!(stored_value, value);
        }
    }

    #[tokio::test]
    async fn test_consensus_with_node_failure() {
        let mut cluster = create_test_cluster(5).await;

        // Simulate node failure
        cluster.kill_node(2).await;

        let key = "test_key".to_string();
        let value = b"test_value".to_vec();

        // Should still succeed with majority
        let result = cluster.leader_mut()
            .start_consensus_round(key.clone(), value.clone())
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_network_partition_handling() {
        let mut cluster = create_test_cluster(5).await;

        // Create network partition: 2 nodes vs 3 nodes
        cluster.create_partition(vec![0, 1], vec![2, 3, 4]).await;

        let key = "test_key".to_string();
        let value = b"test_value".to_vec();

        // Minority partition should fail
        let minority_result = cluster.nodes[0]
            .start_consensus_round(key.clone(), value.clone())
            .await;
        assert!(minority_result.is_err());

        // Majority partition should succeed
        let majority_result = cluster.nodes[2]
            .start_consensus_round(key.clone(), value.clone())
            .await;
        assert!(majority_result.is_ok());
    }
}

struct TestCluster {
    nodes: Vec<PaxosNode>,
    network: TestNetwork,
}

impl TestCluster {
    async fn create_partition(&mut self, group1: Vec<usize>, group2: Vec<usize>) {
        self.network.create_partition(group1, group2).await;
    }

    async fn kill_node(&mut self, index: usize) {
        self.nodes[index].shutdown().await;
    }
}
```

### Erasure Coding Verification

```rust
// ✅ PREFERRED: Erasure coding correctness and performance testing
#[tokio::test]
async fn test_erasure_coding_data_recovery() {
    let mut storage = ErasureCodedStorage::new(3, 2).unwrap();
    let original_data = generate_random_data(1024 * 1024); // 1MB

    // Store data
    let locations = storage.store_value("test_key".to_string(), original_data.clone()).await.unwrap();

    // Simulate loss of up to 2 shards (within tolerance)
    let mut available_locations = locations;
    available_locations.remove(0); // Remove first shard
    available_locations.remove(0); // Remove second shard

    // Should still be able to recover
    let recovered_data = storage.retrieve_value("test_key", &available_locations).await.unwrap();
    assert_eq!(recovered_data, original_data);
}

#[tokio::test]
async fn test_erasure_coding_failure_threshold() {
    let mut storage = ErasureCodedStorage::new(3, 2).unwrap();
    let original_data = generate_random_data(1024);

    let locations = storage.store_value("test_key".to_string(), original_data.clone()).await.unwrap();

    // Remove too many shards (more than parity allows)
    let mut insufficient_locations = locations;
    insufficient_locations.truncate(2); // Only 2 shards available, need 3

    // Should fail to recover
    let result = storage.retrieve_value("test_key", &insufficient_locations).await;
    assert!(result.is_err());
}

fn generate_random_data(size: usize) -> Vec<u8> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen()).collect()
}
```

## Performance Benchmarking & Profiling

### Throughput and Latency Measurement

```rust
// ✅ PREFERRED: Comprehensive performance benchmarking
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

fn benchmark_consensus_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("consensus_1kb_payload", |b| {
        b.to_async(&rt).iter(|| async {
            let mut cluster = create_test_cluster(5).await;
            let payload = vec![0u8; 1024]; // 1KB payload

            let start = std::time::Instant::now();
            let result = cluster.leader_mut()
                .start_consensus_round("bench_key".to_string(), payload)
                .await;
            let duration = start.elapsed();

            assert!(result.is_ok());
            black_box(duration);
        });
    });
}

fn benchmark_erasure_coding_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("erasure_encode_1mb", |b| {
        b.to_async(&rt).iter(|| async {
            let mut storage = ErasureCodedStorage::new(10, 4).unwrap();
            let data = vec![0u8; 1024 * 1024]; // 1MB

            let start = std::time::Instant::now();
            let locations = storage.store_value("bench_key".to_string(), data).await.unwrap();
            let duration = start.elapsed();

            black_box(locations);
            black_box(duration);
        });
    });
}

criterion_group!(benches, benchmark_consensus_throughput, benchmark_erasure_coding_performance);
criterion_main!(benches);
```

### Memory and Resource Monitoring

```rust
// ✅ PREFERRED: Resource usage monitoring
pub struct SystemMetrics {
    memory_usage: AtomicU64,
    network_bytes_sent: AtomicU64,
    network_bytes_received: AtomicU64,
    consensus_rounds_completed: AtomicU64,
    average_consensus_latency: AtomicU64,
}

impl SystemMetrics {
    pub fn record_consensus_round(&self, latency: Duration) {
        self.consensus_rounds_completed.fetch_add(1, Ordering::Relaxed);

        // Update moving average of consensus latency
        let current_avg = self.average_consensus_latency.load(Ordering::Relaxed);
        let new_latency = latency.as_millis() as u64;
        let new_avg = (current_avg + new_latency) / 2;
        self.average_consensus_latency.store(new_avg, Ordering::Relaxed);
    }

    pub async fn start_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let metrics = Arc::new(self.clone());

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));

            loop {
                interval.tick().await;

                println!("=== System Metrics ===");
                println!("Memory Usage: {} MB",
                    metrics.memory_usage.load(Ordering::Relaxed) / 1024 / 1024);
                println!("Network Sent: {} MB",
                    metrics.network_bytes_sent.load(Ordering::Relaxed) / 1024 / 1024);
                println!("Network Received: {} MB",
                    metrics.network_bytes_received.load(Ordering::Relaxed) / 1024 / 1024);
                println!("Consensus Rounds: {}",
                    metrics.consensus_rounds_completed.load(Ordering::Relaxed));
                println!("Avg Consensus Latency: {} ms",
                    metrics.average_consensus_latency.load(Ordering::Relaxed));
            }
        })
    }
}
```

## Configuration Management

### Environment-Specific Configuration

```rust
// ✅ PREFERRED: Flexible configuration management
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistKVConfig {
    pub cluster: ClusterConfig,
    pub consensus: ConsensusConfig,
    pub erasure_coding: ErasureCodingConfig,
    pub network: NetworkConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node_id: String,
    pub peers: Vec<PeerConfig>,
    pub bind_address: String,
    pub bind_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    pub timeout_ms: u64,
    pub max_retries: usize,
    pub heartbeat_interval_ms: u64,
    pub election_timeout_range_ms: (u64, u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureCodingConfig {
    pub data_shards: usize,
    pub parity_shards: usize,
    pub chunk_size: usize,
    pub compression_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub max_message_size: usize,
    pub connection_timeout_ms: u64,
    pub retry_attempts: usize,
    pub buffer_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub data_directory: String,
    pub max_file_size: u64,
    pub sync_interval_ms: u64,
}

impl DistKVConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: DistKVConfig = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let config = Self {
            cluster: ClusterConfig {
                node_id: std::env::var("NODE_ID").unwrap_or_else(|_| uuid::Uuid::new_v4().to_string()),
                peers: Self::parse_peers_from_env()?,
                bind_address: std::env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string()),
                bind_port: std::env::var("BIND_PORT")
                    .unwrap_or_else(|_| "8080".to_string())
                    .parse()?,
            },
            consensus: ConsensusConfig {
                timeout_ms: std::env::var("CONSENSUS_TIMEOUT_MS")
                    .unwrap_or_else(|_| "5000".to_string())
                    .parse()?,
                max_retries: std::env::var("MAX_RETRIES")
                    .unwrap_or_else(|_| "3".to_string())
                    .parse()?,
                heartbeat_interval_ms: std::env::var("HEARTBEAT_INTERVAL_MS")
                    .unwrap_or_else(|_| "1000".to_string())
                    .parse()?,
                election_timeout_range_ms: (5000, 10000),
            },
            erasure_coding: ErasureCodingConfig {
                data_shards: std::env::var("DATA_SHARDS")
                    .unwrap_or_else(|_| "3".to_string())
                    .parse()?,
                parity_shards: std::env::var("PARITY_SHARDS")
                    .unwrap_or_else(|_| "2".to_string())
                    .parse()?,
                chunk_size: 1024 * 1024, // 1MB chunks
                compression_enabled: true,
            },
            network: NetworkConfig {
                max_message_size: 64 * 1024 * 1024, // 64MB
                connection_timeout_ms: 5000,
                retry_attempts: 3,
                buffer_size: 8192,
            },
            storage: StorageConfig {
                data_directory: std::env::var("DATA_DIR").unwrap_or_else(|_| "./data".to_string()),
                max_file_size: 1024 * 1024 * 1024, // 1GB
                sync_interval_ms: 1000,
            },
        };
        Ok(config)
    }

    fn parse_peers_from_env() -> Result<Vec<PeerConfig>, Box<dyn std::error::Error>> {
        let peers_str = std::env::var("CLUSTER_PEERS")?;
        let peers: Vec<PeerConfig> = serde_json::from_str(&peers_str)?;
        Ok(peers)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    pub id: String,
    pub address: String,
    pub port: u16,
}
```
