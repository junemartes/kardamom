//! Sequencer metrics.
//!
//! The binary owns the exporter; this module just declares names + helper
//! functions. Default no-op recorder is a smoke test only; production binaries
//! wire `metrics-exporter-prometheus` per the workspace stack.

use metrics::{counter, gauge, histogram};

pub const TX_INGESTED: &str = "kardamom_sequencer_tx_ingested_total";
pub const TX_PUBLISHED_TO_B: &str = "kardamom_sequencer_tx_published_to_b_total";
pub const TX_BUFFERED_FUTURE: &str = "kardamom_sequencer_tx_buffered_future_total";
pub const TX_DROPPED_PAST: &str = "kardamom_sequencer_tx_dropped_past_total";
pub const PENDING_BUFFER_EVICTIONS: &str = "kardamom_sequencer_pending_evictions_total";
pub const BACKPRESSURE_EVENTS: &str = "kardamom_sequencer_backpressure_total";
pub const NONCE_CHECK_DURATION_SECONDS: &str = "kardamom_sequencer_nonce_check_duration_seconds";

// Lag detection + receipt-floor resync (docs/agents/sequencer-lag-resync-spec.md).
pub const RESYNC_MODE: &str = "kardamom_sequencer_resync_mode";
pub const RESYNC_ENTERED: &str = "kardamom_sequencer_resync_entered_total";
/// Bumped by the egress FEED thread the moment it observes a lag signature
/// (boundary-arrival gap past the silence threshold) — starvation-proof,
/// unlike `RESYNC_ENTERED`, which requires the publish loop to be running.
/// The chaos suite asserts on THIS counter.
pub const RESYNC_LAG_SUSPECTED: &str = "kardamom_sequencer_resync_lag_suspected_total";
pub const RESYNC_SKIPPED_EXECUTED: &str = "kardamom_sequencer_resync_skipped_executed_total";
pub const RECEIPT_FLOOR_SENDERS: &str = "kardamom_sequencer_receipt_floor_senders";
pub const RECEIPT_FLOOR_ADVANCES: &str = "kardamom_sequencer_receipt_floor_advances_total";
pub const CANONICAL_WATERMARK: &str = "kardamom_sequencer_canonical_watermark";

pub fn record_ingest(partition: u32) {
    counter!(TX_INGESTED, "partition" => partition.to_string()).increment(1);
}

pub fn record_publish(partition: u32) {
    counter!(TX_PUBLISHED_TO_B, "partition" => partition.to_string()).increment(1);
}

pub fn record_buffered_future(partition: u32) {
    counter!(TX_BUFFERED_FUTURE, "partition" => partition.to_string()).increment(1);
}

pub fn record_past(partition: u32) {
    counter!(TX_DROPPED_PAST, "partition" => partition.to_string()).increment(1);
}

pub fn record_eviction(partition: u32) {
    counter!(PENDING_BUFFER_EVICTIONS, "partition" => partition.to_string()).increment(1);
}

pub fn record_backpressure(partition: u32) {
    counter!(BACKPRESSURE_EVENTS, "partition" => partition.to_string()).increment(1);
}

pub fn record_nonce_check_latency(partition: u32, seconds: f64) {
    histogram!(NONCE_CHECK_DURATION_SECONDS, "partition" => partition.to_string()).record(seconds);
}

pub fn record_resync_mode(partition: u32, active: bool) {
    gauge!(RESYNC_MODE, "partition" => partition.to_string()).set(if active { 1.0 } else { 0.0 });
}

pub fn record_resync_enter(partition: u32) {
    counter!(RESYNC_ENTERED, "partition" => partition.to_string()).increment(1);
    record_resync_mode(partition, true);
}

/// Unix timestamp of process start. Restart-proof discriminator for the
/// chaos harness: counters reset across a restart to values identical to a
/// fresh baseline (entered=1 from the startup resync), but a start time
/// AFTER a known event proves the process is a newborn — over plain HTTP,
/// with no docker-exec (which wedges for minutes post-thaw on CI runners).
pub const START_TIME_SECONDS: &str = "kardamom_sequencer_start_time_seconds";

pub fn record_start_time() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    metrics::gauge!(START_TIME_SECONDS).set(now);
}

pub fn record_lag_suspected(partition: u32) {
    counter!(RESYNC_LAG_SUSPECTED, "partition" => partition.to_string()).increment(1);
}

pub fn record_resync_skip(partition: u32) {
    counter!(RESYNC_SKIPPED_EXECUTED, "partition" => partition.to_string()).increment(1);
}

pub fn record_floor_senders(partition: u32, senders: usize) {
    gauge!(RECEIPT_FLOOR_SENDERS, "partition" => partition.to_string()).set(senders as f64);
}

pub fn record_floor_advance(partition: u32) {
    counter!(RECEIPT_FLOOR_ADVANCES, "partition" => partition.to_string()).increment(1);
}

pub fn record_canonical_watermark(partition: u32, count: u64) {
    gauge!(CANONICAL_WATERMARK, "partition" => partition.to_string()).set(count as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_helpers_smoke() {
        // Default recorder is no-op until installed; these calls just exercise
        // the API surface so it stays compiling.
        record_ingest(0);
        record_publish(0);
        record_buffered_future(0);
        record_past(0);
        record_eviction(0);
        record_backpressure(0);
        record_nonce_check_latency(0, 0.0015);
    }
}
