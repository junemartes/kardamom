//! Sequencer metrics.
//!
//! The binary owns the exporter; this module just declares names + helper
//! functions. Default no-op recorder is a smoke test only; production binaries
//! wire `metrics-exporter-prometheus` per the workspace stack.

use metrics::{counter, histogram};

pub const TX_INGESTED: &str = "kardamom_sequencer_tx_ingested_total";
pub const TX_PUBLISHED_TO_B: &str = "kardamom_sequencer_tx_published_to_b_total";
pub const TX_BUFFERED_FUTURE: &str = "kardamom_sequencer_tx_buffered_future_total";
pub const TX_DROPPED_PAST: &str = "kardamom_sequencer_tx_dropped_past_total";
pub const PENDING_BUFFER_EVICTIONS: &str = "kardamom_sequencer_pending_evictions_total";
pub const BACKPRESSURE_EVENTS: &str = "kardamom_sequencer_backpressure_total";
pub const NONCE_CHECK_LATENCY_US: &str = "kardamom_sequencer_nonce_check_microseconds";
pub const STANDBY_REPLAY_LAG: &str = "kardamom_sequencer_standby_replay_lag";

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

pub fn record_nonce_check_latency(partition: u32, micros: f64) {
    histogram!(NONCE_CHECK_LATENCY_US, "partition" => partition.to_string()).record(micros);
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
        record_nonce_check_latency(0, 1.5);
    }
}
