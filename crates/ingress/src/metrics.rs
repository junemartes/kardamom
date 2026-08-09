//! Ingress metrics.
//!
//! The binary owns the exporter; this module just declares names + the
//! `describe` function that registers human-readable descriptions.
//! Default no-op recorder is fine for tests; production binaries wire
//! `metrics-exporter-prometheus` via `kardamom_obs::init`.

pub const TX_RECEIVED_TOTAL: &str = "kardamom_ingress_tx_received_total";
pub const TX_ACCEPTED_TOTAL: &str = "kardamom_ingress_tx_accepted_total";
pub const TX_REJECTED_TOTAL: &str = "kardamom_ingress_tx_rejected_total";
pub const QUEUE_DEPTH: &str = "kardamom_ingress_queue_depth";
/// Duplicate receipts dropped by the tx_receipts MDS fan-in dedup (the same
/// receipt replayed by multiple executor replicas). 0 on the single-executor
/// IPC path.
pub const RECEIPT_DUPLICATE_TOTAL: &str = "kardamom_ingress_receipt_duplicate_total";
/// Duplicate tx_errors dropped by the consumer-side dedup (P racing sequencer
/// replicas each emit the same per-tx rejection, so every error arrives up to
/// P times; a rejection already overridden by a success is also dropped).
/// 0 on a single-replica (P=1) deployment.
pub const TX_ERROR_DUPLICATE_TOTAL: &str = "kardamom_ingress_tx_error_duplicate_total";
/// Malformed cluster egress frames dropped by the on-quorum watermark
/// observer. The cluster stream is authoritative, so this should stay 0; a
/// sustained non-zero means a Java/Rust envelope framing mismatch shipped and
/// records are being silently lost (see `crate::cluster`).
pub const CLUSTER_FRAME_DROPPED_TOTAL: &str = "kardamom_ingress_cluster_frames_dropped_total";

/// Increment [`TX_REJECTED_TOTAL`] with the given `reason` label. Single
/// home for the submit-path rejection counter so every rejection is counted
/// the same way (and call sites stay one line).
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_smoke() {
        // Default recorder is no-op; just verify this compiles and doesn't panic.
        describe();
    }

    #[test]
    fn constants_have_expected_prefix() {
        for name in [
            TX_RECEIVED_TOTAL,
            TX_ACCEPTED_TOTAL,
            TX_REJECTED_TOTAL,
            QUEUE_DEPTH,
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
