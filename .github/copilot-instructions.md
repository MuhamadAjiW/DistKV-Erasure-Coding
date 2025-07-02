# Copilot Instructions for DistKV-Erasure-Coding

## Project Overview

This is a **distributed key-value store system** implementation for a thesis project, combining **Paxos consensus protocol** with **erasure coding** for fault-tolerant distributed storage. Built with Rust and Tokio async runtime, the system demonstrates advanced distributed systems concepts including consensus algorithms, fault tolerance, and efficient data encoding strategies.

## CRITICAL BEHAVIORAL REQUIREMENTS

### MANDATORY RESPONSE GUIDELINES

- **ALWAYS READ GIVEN FILES** - READ EACH OF THE FILES AND UNDERSTAND THE IMPLEMENTATION. DO NOT ASSUME YOU UNDERSTAND THE FILES WITHOUT READING THEM.
- **ALWAYS provide comprehensive, production-quality distributed systems solutions** - No simplified implementations or academic shortcuts
- **ACTIVELY suggest performance optimizations and reliability improvements** - Enhance throughput, reduce latency, improve fault tolerance
- **PROACTIVELY identify distributed systems challenges** - Point out consensus edge cases, network partition scenarios, and timing issues
- **VERIFY ALL DISTRIBUTED SYSTEMS CONCEPTS** - DO NOT HALLUCINATE UNIMPLEMENTED FUNCTIONS OR PATTERNS.
- **USE RESEARCH TOOLS EXTENSIVELY** - Stay current with distributed systems research and Rust ecosystem updates
- **THINK STEP-BY-STEP FOR CONSENSUS PROTOCOLS** - Break down complex consensus scenarios and explain state transitions
- **ALWAYS UPDATE FILE DIRECTLY** - UNLESS STATED OTHERWISE BY THE USER TO NOT CODE, NEVER ONLY SUGGEST. Always edit the file directly with your implementation.
- **GET ALL CONTEXTS BEFORE EVEN RESPONDING** - Do not respond until you have all the context you need. READ THE FUCKING FILES. If you need more context, ask for it before responding. Can't stress this enough, READ THE FUCKING FILES.
- **NEVER USE EMOJI** - Do not use emoji in any responses, maintain a professional tone.

### RESPONSE STRUCTURE REQUIREMENTS

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
