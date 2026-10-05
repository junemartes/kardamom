//! Ingress metrics.
//!
//! The binary owns the exporter. This module only declares names and the
//! `describe` function, which registers human-readable descriptions.
//! The default no-op recorder works for tests. Production binaries wire
//! `metrics-exporter-prometheus` through `kardamom_obs::init`.

pub const TX_RECEIVED_TOTAL: &str = "kardamom_ingress_tx_received_total";
/// The installed shard map version. 0 is the identity map.
pub const SHARD_MAP_VERSION: &str = "kardamom_ingress_shard_map_version";
pub const TX_ACCEPTED_TOTAL: &str = "kardamom_ingress_tx_accepted_total";
pub const TX_REJECTED_TOTAL: &str = "kardamom_ingress_tx_rejected_total";
pub const QUEUE_DEPTH: &str = "kardamom_ingress_queue_depth";
/// Duplicate receipts dropped by the `tx_receipts` MDS fan-in dedup. This
/// counts the same receipt replayed by multiple executor replicas. The
/// value is 0 on the single-executor IPC path.
pub const RECEIPT_DUPLICATE_TOTAL: &str = "kardamom_ingress_receipt_duplicate_total";
/// Duplicate `tx_errors` dropped by the consumer-side dedup. P racing
/// sequencer replicas each emit the same per-tx rejection, so each error
/// can arrive up to P times. A rejection already overridden by a success
/// is also dropped. The value is 0 on a single-replica (P=1) deployment.
pub const TX_ERROR_DUPLICATE_TOTAL: &str = "kardamom_ingress_tx_error_duplicate_total";
/// Malformed cluster egress frames dropped by the on-quorum watermark
/// observer. The cluster stream is authoritative, so this value should stay
/// 0. A sustained non-zero value means a Java/Rust envelope framing
/// mismatch shipped, and records are being lost silently. See
/// `crate::cluster`.
pub const CLUSTER_FRAME_DROPPED_TOTAL: &str = "kardamom_ingress_cluster_frames_dropped_total";
/// The last L2 block posted to L1, as the cluster's status frame carries
/// it: the block `safe` names.
pub const CLUSTER_POSTED_HEAD: &str = "kardamom_ingress_cluster_posted_head";
/// The last sealed block, from the same status frame.
pub const CLUSTER_SEALED_HEAD: &str = "kardamom_ingress_cluster_sealed_head";
/// The egress frames the sealer retains for replay. Above the retention
/// window while unposted blocks hold it there; back inside it once the
/// batcher posts.
pub const CLUSTER_RETAINED_FRAMES: &str = "kardamom_ingress_cluster_retained_frames";
/// The oldest boundary block the sealer still retains: its replay floor.
pub const CLUSTER_FLOOR_BLOCK: &str = "kardamom_ingress_cluster_floor_block";
/// 0 while the proxy serves, 1 from the first moment of the shutdown
/// drain. The readiness rule requires 0: a draining replica refuses new
/// submits, so a health check must take it out of rotation at once. The
/// gauge is unset before the listeners are up, which also reads as not
/// ready.
pub const DRAINING: &str = "kardamom_ingress_draining";

/// Increments [`TX_REJECTED_TOTAL`] with the given `reason` label. This is
/// the one place for the submit-path rejection counter, so every rejection
/// counts the same way and call sites stay one line.
#[inline]
pub(crate) fn count_reject(reason: &'static str) {
    metrics::counter!(TX_REJECTED_TOTAL, "reason" => reason).increment(1);
}

pub fn describe() {
    metrics::describe_counter!(TX_RECEIVED_TOTAL, "tx submissions received");
    metrics::describe_counter!(
        TX_ACCEPTED_TOTAL,
        "tx submissions that returned a receipt (incl. cached resubmissions)"
    );
    metrics::describe_counter!(
        TX_REJECTED_TOTAL,
        "tx submissions rejected, labelled by reason"
    );
    metrics::describe_gauge!(QUEUE_DEPTH, "current pending-tx queue depth");
    metrics::describe_counter!(
        RECEIPT_DUPLICATE_TOTAL,
        "duplicate receipts dropped by tx_receipts MDS fan-in dedup (first-wins by tx hash)"
    );
    metrics::describe_counter!(
        TX_ERROR_DUPLICATE_TOTAL,
        "duplicate/overridden tx_errors dropped by the consumer-side dedup (racing sequencer replicas)"
    );
    metrics::describe_counter!(
        CLUSTER_FRAME_DROPPED_TOTAL,
        "malformed cluster egress frames dropped by the watermark observer (should stay 0)"
    );
    metrics::describe_gauge!(DRAINING, "1 while the shutdown drain refuses new submits");
    metrics::describe_gauge!(
        CLUSTER_POSTED_HEAD,
        "the last L2 block posted to L1, from the cluster's status frame"
    );
    metrics::describe_gauge!(
        CLUSTER_SEALED_HEAD,
        "the last sealed block, from the cluster's status frame"
    );
    metrics::describe_gauge!(
        CLUSTER_RETAINED_FRAMES,
        "egress frames the sealer retains for replay; above the window while unposted blocks hold it"
    );
    metrics::describe_gauge!(
        CLUSTER_FLOOR_BLOCK,
        "the oldest boundary block the sealer still retains"
    );
}

/// Export one cluster status: the heads and the retention floors.
pub(crate) fn record_cluster_status(status: &kardamom_types::ClusterStatus) {
    // Metric values; f64 precision loss only above 2^52, never reached by
    // a block number or a frame count.
    #[allow(
        clippy::cast_precision_loss,
        reason = "metric values; a block number or a frame count never nears 2^52"
    )]
    {
        metrics::gauge!(CLUSTER_POSTED_HEAD).set(status.posted_head as f64);
        metrics::gauge!(CLUSTER_SEALED_HEAD).set(status.sealed_head as f64);
        metrics::gauge!(CLUSTER_RETAINED_FRAMES).set(status.retained_frames as f64);
        metrics::gauge!(CLUSTER_FLOOR_BLOCK).set(status.floor_block as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_smoke() {
        // The default recorder is no-op. This test only checks that the
        // code compiles and does not panic.
        describe();
    }

    #[test]
    fn constants_have_expected_prefix() {
        for name in [
            TX_RECEIVED_TOTAL,
            TX_ACCEPTED_TOTAL,
            TX_REJECTED_TOTAL,
            QUEUE_DEPTH,
            CLUSTER_POSTED_HEAD,
            CLUSTER_SEALED_HEAD,
            CLUSTER_RETAINED_FRAMES,
            CLUSTER_FLOOR_BLOCK,
            RECEIPT_DUPLICATE_TOTAL,
            TX_ERROR_DUPLICATE_TOTAL,
            CLUSTER_FRAME_DROPPED_TOTAL,
        ] {
            assert!(
                name.starts_with("kardamom_ingress_"),
                "expected kardamom_ingress_ prefix, got: {name}"
            );
        }
    }
}
