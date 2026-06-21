//! In-process adapters that plug an Aeron Cluster in behind kardamom's existing
//! transport trait seams.
//!
//! - [`publisher::ClusterRefPublisher`] implements the sequencer's
//!   `TxOrderingRefPublisher` (sequencer → cluster ingress).
//! - [`subscription::ClusterTxOrderingSubscription`] implements the executor's
//!   `TxOrderingSubscription` (executor ← cluster egress).
//! - [`watermark::ClusterWatermark`] derives ingress's durable watermark from
//!   cluster egress progress.
//!
//! All three sit on the [`gateway`] seam (`ClusterIngress` / `ClusterEgress`),
//! whose live implementation drives `kardamom_cluster_client`'s `SessionDriver`
//! over Aeron pub/sub (behind that crate's `aeron-live` feature) and whose
//! in-memory fakes make this crate deterministically testable. The app envelope
//! shared with the Java clustered service lives in [`wire`].

pub mod gateway;
pub mod live;
pub mod publisher;
pub mod subscription;
pub mod watermark;
pub mod wire;

pub use gateway::{ClusterEgress, ClusterIngress, OfferOutcome};
pub use publisher::ClusterRefPublisher;
pub use subscription::ClusterTxOrderingSubscription;
pub use watermark::ClusterWatermark;

pub use live::{LiveCluster, LiveClusterConfig, LiveEgress, LiveError, LiveIngress};

use kardamom_log::aeron_live::AeronRuntime;

/// Wire a cluster-backed **sequencer** outbound publisher in cluster mode. The
/// returned [`LiveCluster`] owns the session and MUST be kept alive for as long
/// as the publisher is used; the egress half is dropped (publisher-only).
pub fn cluster_ref_publisher(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterRefPublisher<LiveIngress>), LiveError> {
    let (cluster, ingress, _egress) = live::connect(rt, cfg)?;
    Ok((cluster, ClusterRefPublisher::new(ingress)))
}

/// Wire a cluster-backed **executor** tx_ordering subscription in cluster mode.
/// The returned [`LiveCluster`] owns the session and MUST be kept alive for as
/// long as the subscription is used; the ingress half is dropped
/// (subscriber-only).
pub fn cluster_tx_ordering_subscription(
    rt: AeronRuntime,
    cfg: LiveClusterConfig,
) -> Result<(LiveCluster, ClusterTxOrderingSubscription<LiveEgress>), LiveError> {
    let (cluster, _ingress, egress) = live::connect(rt, cfg)?;
    Ok((cluster, ClusterTxOrderingSubscription::new(egress)))
}
