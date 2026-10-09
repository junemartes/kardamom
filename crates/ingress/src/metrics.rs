//! Ingress metrics.
//!
//! The binary owns the exporter. This module only declares names and the
//! `describe` function, which registers human-readable descriptions.
//! The default no-op recorder works for tests. Production binaries wire
//! `metrics-exporter-prometheus` through `kardamom_obs::init`.

use kardamom_types::cluster_status::RecordLagStatus;

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
/// The best recorded cursor of the executors, from the record-lag tail of
/// the status frame. -1 while no executor sent a cursor, and for a frame
/// with no tail.
pub const CLUSTER_RECORDED_HEAD: &str = "kardamom_ingress_cluster_recorded_head";
/// The canonical records past the best recorded cursor when the status
/// frame arrived: the value that the record-lag guard compares with its
/// budget. 0 while no executor sent a cursor: the guard then refuses
/// nothing.
pub const CLUSTER_RECORD_LAG: &str = "kardamom_ingress_cluster_record_lag";
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
    metrics::describe_gauge!(
        CLUSTER_RECORDED_HEAD,
        "the best recorded cursor of the executors, from the cluster's status frame; -1 before the first"
    );
    metrics::describe_gauge!(
        CLUSTER_RECORD_LAG,
        "canonical records past the best recorded cursor; the record-lag guard compares it with its budget"
    );
}

/// The record-lag gauges of one status frame: the frame's record-lag
/// tail, and the durable canonical count that the ingress observed when
/// the frame arrived. The sealer sends a status frame in the order of its
/// log, so that count is the sealer's canonical count at the frame.
pub(crate) struct RecordLagGauges {
    pub(crate) lag: RecordLagStatus,
    pub(crate) durable_count: u64,
}

impl RecordLagGauges {
    /// Export the best recorded cursor and the lag. A count that is not
    /// yet past the cursor means that this session has not seen the
    /// records up to the cursor: the lag gauge then keeps its value.
    #[allow(
        clippy::cast_precision_loss,
        reason = "metric values; a canonical index never nears 2^52"
    )]
    pub(crate) fn record(&self) {
        let Some(best) = self.lag.best_recorded else {
            metrics::gauge!(CLUSTER_RECORDED_HEAD).set(-1.0);
            metrics::gauge!(CLUSTER_RECORD_LAG).set(0.0);
            return;
        };
        metrics::gauge!(CLUSTER_RECORDED_HEAD).set(best as f64);
        if let Some(lag) = self.lag() {
            metrics::gauge!(CLUSTER_RECORD_LAG).set(lag as f64);
        }
    }

    /// The records ordered past the best cursor: `count - (best + 1)`.
    fn lag(&self) -> Option<u64> {
        let best = self.lag.best_recorded?;
        self.durable_count.checked_sub(best.checked_add(1)?)
    }
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
            CLUSTER_RECORDED_HEAD,
            CLUSTER_RECORD_LAG,
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
