//! Mirror metric names and helpers, in one place.

const BATCHES_APPLIED_TOTAL: &str = "kardamom_state_mirror_batches_applied_total";
const PRODUCER_DISAGREEMENT_TOTAL: &str = "kardamom_state_mirror_producer_disagreement_total";
const HEAD_TX_IDX: &str = "kardamom_state_mirror_head_tx_idx";
const REBUILD_SECONDS: &str = "kardamom_state_mirror_rebuild_seconds";
const REBUILDS_TOTAL: &str = "kardamom_state_mirror_rebuilds_total";
const WRITE_RETRIES_TOTAL: &str = "kardamom_state_mirror_write_retries_total";
const WAIT_REPLICA_ZERO_TOTAL: &str = "kardamom_state_mirror_wait_replica_zero_total";
/// 1 while the mirror applies batches as they come: after its start
/// decision and outside a rebuild. The readiness rule requires 1. The
/// executors publish no batch for an empty block, so an idle chain gives
/// the mirror nothing to advance on, and a time-based rule would fail
/// a healthy mirror.
pub(crate) const SERVING: &str = "kardamom_state_mirror_serving";

/// Register the descriptions. Call once at startup.
pub(crate) fn describe() {
    metrics::describe_counter!(
        BATCHES_APPLIED_TOTAL,
        "receipt batches written to Redis, rows and receipts"
    );
    metrics::describe_counter!(
        PRODUCER_DISAGREEMENT_TOTAL,
        "rows that disagreed with a stored row at the same position"
    );
    metrics::describe_gauge!(
        HEAD_TX_IDX,
        "the highest batch end position this mirror applied, as an index"
    );
    metrics::describe_histogram!(REBUILD_SECONDS, "wall time of one rebuild");
    metrics::describe_counter!(REBUILDS_TOTAL, "rebuilds, by reason");
    metrics::describe_counter!(WRITE_RETRIES_TOTAL, "Redis writes retried after a failure");
    metrics::describe_counter!(
        WAIT_REPLICA_ZERO_TOTAL,
        "batches no replica acknowledged within the wait"
    );
    metrics::describe_gauge!(
        SERVING,
        "1 while the mirror applies batches: started, and not in a rebuild"
    );
}

pub(crate) fn record_batch(disagreed: u64) {
    metrics::counter!(BATCHES_APPLIED_TOTAL).increment(1);
    metrics::counter!(PRODUCER_DISAGREEMENT_TOTAL).increment(disagreed);
}

pub(crate) fn set_serving(serving: bool) {
    metrics::gauge!(SERVING).set(if serving { 1.0 } else { 0.0 });
}

pub(crate) fn set_head(tx_idx: u64) {
    // A gauge is an f64. A position beyond 2^53 is not a real value.
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(HEAD_TX_IDX).set(tx_idx as f64);
}

pub(crate) fn record_rebuild(reason: &'static str, seconds: f64) {
    metrics::counter!(REBUILDS_TOTAL, "reason" => reason).increment(1);
    metrics::histogram!(REBUILD_SECONDS).record(seconds);
}

pub(crate) fn record_write_retry() {
    metrics::counter!(WRITE_RETRIES_TOTAL).increment(1);
}

pub(crate) fn record_wait_replica_zero() {
    metrics::counter!(WAIT_REPLICA_ZERO_TOTAL).increment(1);
}
