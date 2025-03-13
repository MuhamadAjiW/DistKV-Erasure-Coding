# Paxos-Based Distributed System in Rust

This project is a simple Paxos-based distributed system implemented in Rust using the `tokio` async runtime. It consists of a load balancer, a leader, and multiple follower nodes communicating over UDP. The goal of this system is to simulate basic consensus behavior using the Paxos protocol.

<!-- ## Features
- **Client Request Handling**: The client sends a request to the load balancer, which forwards it to a leader or follower.
- **Leader-Follower Communication**: The leader broadcasts requests to followers and waits for majority acknowledgment.
- **Load Balancing**: The load balancer distributes client requests to different nodes (leader or followers) using round-robin logic.

## System Architecture
- **Leader**: Coordinates the requests by broadcasting them to the followers and collects acknowledgments.
- **Followers**: Receive broadcasted requests and send acknowledgments back to the leader.
- **Load Balancer**: Forwards client requests to different nodes and handles routing responses back to the client. -->

## Prerequisites
- **Rust**: Make sure you have Rust installed. You can install it via [rustup](https://rustup.rs/).
- **tokio**: The project uses `tokio` for async runtime. It's already included in the dependencies in `Cargo.toml`.

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/akramab/paxos-rust.git
   cd paxos-distributed-system
   ```
2. Build the project:
   ```bash
   cargo build
   ```

<!-- ## Running the System

### Step 1: Start the Load Balancer
The load balancer listens for client requests and forwards them to the leader or followers.

```bash
cargo run -- load_balancer 127.0.0.1:8000
```

### Step 2: Start the Leader
Start the leader node. The leader handles requests and coordinates with the followers.

```bash
cargo run -- leader <address> <load_balancer> <shard_count> <parity_count>
```

### Step 3: Start the Followers
You can run multiple followers. Each follower registers with the leader and the load balancer.

```bash
cargo run -- follower <address> <leader_address> <load_balancer> <shard_count> <parity_count>
```

Run as many followers as needed by specifying different ports for each:
```bash
cargo run -- follower 127.0.0.1:8082 127.0.0.1:8080 127.0.0.1:8000 4 2
cargo run -- follower 127.0.0.1:8083 127.0.0.1:8080 127.0.0.1:8000 4 2
```

### Step 4: Send Requests via Client
```bash
cargo run -- client 127.0.0.1:8000 "SET A " "f" 50
cargo run -- client 127.0.0.1:8000 "GET A"
```

You should see the request being processed by the leader and followers in the logs, and the response will be forwarded back to the client through the load balancer.

## Future Improvements
- **Fault Tolerance**: Implement retry mechanisms for follower failures.
- **Scalability**: Improve load balancer to dynamically scale with the number of nodes.
- **Multicast Support**: Enhance leader-to-follower communication using multicast. -->
