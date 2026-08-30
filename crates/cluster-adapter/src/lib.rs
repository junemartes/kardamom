//! Transport layer that plugs an Aeron Cluster behind kardamom's existing
//! transport trait seams.
//!
//! This crate handles transport only. It exposes the cluster gateway seam
//! ([`gateway`]: `ClusterIngress` / `ClusterEgress`), the live Aeron-backed
//! implementation ([`live`]), the durable [`watermark`] derivation, and the
//! app envelope ([`wire`]) shared with the Java clustered service.
//!
//! The trait-impl adapters that connect this transport to the sequencer and
//! executor seams live in those crates, under their `cluster` feature
//! (`kardamom_sequencer::outbound::cluster::ClusterRefPublisher` and
//! `kardamom_executor::reader::cluster::ClusterTxOrderingSubscription`). This
//! avoids a Cargo dependency cycle: the cluster-mode binaries live in the
//! sequencer and executor crates, and those crates depend on this crate.
//!
//! The live gateway drives `kardamom_cluster_client`'s `SessionDriver` over
//! Aeron pub/sub, behind that crate's `aeron-live` feature. The in-memory
//! fakes let tests check the trait adapters in a deterministic way.

pub mod config;
pub mod gateway;
pub mod live;
pub mod watermark;
pub mod wire;

pub use config::ClusterConfig;
pub use gateway::{ClusterEgress, ClusterIngress, OfferOutcome};
pub use watermark::ClusterWatermark;

pub use live::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, LiveIngress};
