# DistKV-Erasure-Coding: Paxos-Based Distributed Key-Value Store in Rust

This project is a research-grade, production-quality distributed key-value store implemented in Rust. It combines the Paxos consensus protocol (Multi-Paxos) with erasure coding for fault-tolerant, high-performance distributed storage. The system is built with async Rust (Tokio), reliable UDP networking, and advanced distributed systems concepts.

---

## Distributed Systems Analysis & Design

DistKV-Erasure-Coding is designed to address the core challenges of distributed storage systems:
- **Consensus under Failures:** Uses Multi-Paxos to ensure strong consistency and distributed agreement, even during node failures or network partitions.
- **Fault Tolerance:** Integrates Reed-Solomon erasure coding to allow data recovery even if some nodes/shards are lost.
- **CAP Theorem Trade-offs:** Prioritizes Consistency and Partition Tolerance, with tunable parameters for availability.
- **Network Partition Handling:** Detects and recovers from partitions, preventing split-brain scenarios.
- **Byzantine Fault Considerations:** While not fully BFT, the design anticipates and logs suspicious or inconsistent behaviors for future extension.

---

## System Architecture

- **Load Balancer:** Distributes client requests and manages connections, ensuring even load and failover.
- **Leader Node:** Runs the Paxos proposer role, coordinates consensus rounds, and manages log replication.
- **Follower Nodes:** Act as acceptors/learners, store erasure-coded data, and participate in recovery.
- **Consensus Layer:** Implements Multi-Paxos for distributed agreement and log replication.
- **Storage Layer:** Splits and distributes data using Reed-Solomon erasure coding, enabling recovery from partial data loss.
- **Network Layer:** Custom reliable UDP transport with retries, message ordering, and heartbeat support for liveness detection.

**Communication Patterns:**
- All inter-node communication is over UDP, with reliability and ordering handled at the application layer.
- Heartbeats and timeouts are used for leader election and failure detection.
- Quorum-based decision making prevents split-brain and ensures safety.

---

## Implementation Details

- **Rust 1.75+**: Strict safety, zero-cost abstractions, and async/await for concurrency.
- **Tokio 1.35+**: High-performance async runtime for networking and timers.
- **Serde & Bincode**: Efficient serialization for all network messages.
- **Reed-Solomon Erasure Coding**: Data is split into N data shards and M parity shards, distributed across nodes. Recovery is possible as long as any N shards are available.
- **Reliable UDP**: Retries, message IDs, and acknowledgments ensure delivery and ordering. Heartbeats detect node liveness.
- **Error Handling**: All network and consensus operations use robust error types, with context for distributed failures (timeouts, partitions, coding errors).

---

## Consensus & Erasure Coding Strategy

- **Multi-Paxos Phases:**
  - *Prepare*: Leader proposes a new value/instance, collects promises from a quorum.
  - *Accept*: Leader sends value to be accepted; followers persist and acknowledge.
  - *Commit*: Once a majority accepts, the value is committed and erasure-coded for storage.
- **Quorum & Recovery:**
  - System tolerates up to M node failures (where M = parity shards).
  - On recovery, any N shards (data or parity) can reconstruct the original value.
- **Network Partition Handling:**
  - Minority partitions cannot make progress; majority partition continues.
  - On healing, nodes resynchronize via log replay and erasure-coded data recovery.

---

## Performance & Reliability

- **Metrics:** Tracks memory usage, network throughput, consensus rounds, and average latency.
- **Fault Tolerance:** Survives node failures and network partitions; erasure coding allows data recovery with partial shard loss.
- **Scalability:** Designed for horizontal scaling and robust operation under real-world distributed system failures.
- **Benchmarking:**
  - Automated with Criterion and k6 (see scripts.sh).
  - Benchmarks include throughput, latency, and resource usage under various loads and network conditions.
- **Resource Monitoring:**
  - System metrics are periodically logged for analysis and tuning.

---

## Testing & Validation

- **Automated Tests:**
  - Consensus correctness (including node failures and network partitions).
  - Erasure coding recovery (data can be reconstructed with missing shards).
  - Network partition simulation (split-brain prevention, healing).
- **Benchmarking:**
  - Run `cargo test` for correctness and regression testing.
  - Use `scripts.sh bench_system` and `run_bench_suite` for performance and fault-tolerance benchmarks. These scripts automate node startup, cleanup, and benchmarking.
- **Manual and Automated Validation:**
  - All critical paths are covered by integration and property-based tests.

---

## Prerequisites
- **Rust** (1.75+): [Install via rustup](https://rustup.rs/)
- **Tokio** (1.35+): Async runtime (included in dependencies)
- **Other dependencies:** `serde`, `reed-solomon-erasure`, `criterion`, `bincode`, `uuid`, `thiserror`

---

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/MuhamadAjiW/DistKV-Erasure-Coding
   cd DistKV-Erasure-Coding
   ```
2. Build the project:
   ```bash
   cargo build
   ```

---

## Running the System (Recommended: Use scripts.sh)

> **Highly recommended:** Use the provided `scripts.sh` to manage, run, and benchmark the system. Manual execution is possible, but `scripts.sh` automates node startup, cleanup, validation, and benchmarking for you.

### Quick Start with scripts.sh

#### 1. Clean up previous data and logs
```bash
./scripts.sh clean
```

#### 2. Validate your configuration (optional but recommended)
```bash
./scripts.sh validate_config ./etc/config.json
```

#### 3. Start all nodes (each in a separate screen window)
```bash
./scripts.sh run_all
```
- Add `--file_output` to log to files instead of terminal.
- Add `--trace` to enable tracing.
- Add `--erasure` to enable erasure coding mode.

#### 4. Stop all running nodes
```bash
./scripts.sh stop_all
```

#### 5. Run a single node (advanced/manual)
```bash
./scripts.sh run_node --addr 127.0.0.1:8081 --config_file ./etc/config.json
```

#### 6. Benchmarking
- **System benchmark:**
  ```bash
  sudo ./scripts.sh bench_system
  ```
- **Full benchmark suite (replication + erasure):**
  ```bash
  sudo ./scripts.sh run_bench_suite
  ```
- **Baseline (etcd) benchmark:**
  ```bash
  sudo ./scripts.sh bench_baseline
  ```

#### 7. Network Emulation (optional, for bandwidth/latency testing)
```bash
sudo ./scripts.sh add_netem_limits 10mbit
sudo ./scripts.sh remove_netem_limits
```

> All nodes are started in a `screen` session named `paxos_rust`. Attach with `screen -r paxos_rust` to view logs.

---

## Manual Execution (Not Recommended)

You can still run components manually with `cargo run`, but using `scripts.sh` is strongly preferred for reliability and convenience.

### 1. Start the Load Balancer
```bash
cargo run -- load_balancer 127.0.0.1:8000
```

### 2. Start the Leader
```bash
cargo run -- leader <address> <load_balancer> <shard_count> <parity_count>
```

### 3. Start the Followers
```bash
cargo run -- follower <address> <leader_address> <load_balancer> <shard_count> <parity_count>
```

### 4. Send Requests via Client
```bash
cargo run -- client 127.0.0.1:8000 "SET A " "f" 50
cargo run -- client 127.0.0.1:8000 "GET A"
```

---

## Configuration
- **File-based:** Place a TOML config file (see `etc/config.json` for example structure).
- **Environment variables:** All major settings can be overridden via environment variables (see code for details).
- **Tunable Parameters:**
  - Shard/parity counts, timeouts, retry counts, buffer sizes, and more can be set via config or env.
- **Example Configuration:**
  - See `etc/config.json` for a sample configuration file. All fields are documented in the code and can be tuned for your deployment.

---

## Performance & Reliability
- **Metrics:** Tracks memory usage, network throughput, consensus rounds, and average latency.
- **Fault Tolerance:** Survives node failures and network partitions; erasure coding allows data recovery with partial shard loss.
- **Scalability:** Designed for horizontal scaling and robust operation under real-world distributed system failures.

---

## Future Improvements
- Dynamic leader election and failover
- Enhanced load balancer for dynamic scaling
- Multicast support for efficient leader-to-follower communication
- Advanced monitoring and alerting
- Full Byzantine Fault Tolerance (BFT) extensions
- Pluggable storage backends (e.g., RocksDB, Sled)
- Dynamic reconfiguration (add/remove nodes at runtime)

---

For more details, see the code and the `benchmark/.github/distkv_copilot_instructions.md` for in-depth implementation and testing strategies.
