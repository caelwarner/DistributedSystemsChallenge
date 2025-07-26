# Fly.io Distributed Systems Challenge
This repository contains my solutions to the [Fly.io Distributed Systems Challenge](https://fly.io/dist-sys/), implemented in Rust. The project features a framework inspired by Axum, where incoming messages are routed to handler functions that process them asynchronously, making it easy to define distributed protocols in a modular way. 

## Completed Challenges
- **Echo:** when a node receives an "echo" message it returns an "echo_ok" message
- **Unique ID Generation:** nodes have to generate globally unique ids
- **Broadcast:** broadcast system that gossips messages between all nodes in the cluster
- **Grow-Only Counter:** stateless grow-only counter
- **Kafka-Style Log:** replicated log service similar to Kafka

## Requirements
- Rust
- Cargo
- [Maelstrom](https://github.com/jepsen-io/maelstrom)
