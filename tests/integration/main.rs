//! Integration tests against a two-broker Apache Pulsar cluster in Docker.
//!
//! ```sh
//! cargo test --features cluster-tests --test integration
//! ```
//!
//! The first run boots the cluster, which takes about two minutes. Later runs
//! reuse it. `cluster.rs` shows how to remove it.

mod cluster;
mod delivery;
mod reconnect;
mod smoke;
