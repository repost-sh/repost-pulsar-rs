//! Integration tests against a two-broker Apache Pulsar cluster in Docker.
//!
//! ```sh
//! cargo test --features cluster-tests --test integration
//! ```
//!
//! The first run boots the cluster, which takes about two minutes. Later runs
//! reuse it. `cluster.rs` shows how to remove it.

// The telemetry feature nests the async test helpers past the default limit.
#![recursion_limit = "256"]

mod acks;
mod batch_acks;
mod cluster;
mod delivery;
mod events;
mod partitions;
mod reader;
mod reconnect;
mod seek;
mod smoke;
