//! Transport layer that plugs an Aeron Cluster in behind kardamom's existing
//! transport trait seams.
//!
//! This crate is **transport-only**: it exposes the cluster gateway seam
//! ([`gateway`]: `ClusterIngress` / `ClusterEgress`), its live Aeron-backed
//! implementation ([`live`]), the durable [`watermark`] derivation, and the app
//! envelope ([`wire`]) shared with the Java clustered service. The trait-impl
//! adapters that bridge this transport to the sequencer / executor seams now
//! live in those crates under their respective `cluster` feature
//! (`kardamom_sequencer::outbound::cluster::ClusterRefPublisher` and
//! `kardamom_executor::reader::cluster::ClusterTxOrderingSubscription`) — this
//! avoids a Cargo dependency cycle, since the cluster-mode binaries live in the
//! sequencer / executor crates and depend on this crate.
//!
//! The live gateway drives `kardamom_cluster_client`'s `SessionDriver` over
//! Aeron pub/sub (behind that crate's `aeron-live` feature); the in-memory fakes
//! make the trait adapters deterministically testable.

pub mod config;
pub mod gateway;
pub mod live;
pub mod watermark;
pub mod wire;

pub use config::ClusterConfig;
pub use gateway::{ClusterEgress, ClusterIngress, OfferOutcome};
pub use watermark::ClusterWatermark;

pub use live::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, LiveIngress};
