//! DA-watcher metric names + registration.
//!
//! The binary owns the exporter (via `kardamom_obs::init`); this module just
//! declares the canonical constant names and the `describe` call that
//! registers human-readable help strings with the Prometheus recorder.

pub const L1_HEAD: &str = "kardamom_da_watcher_l1_head_block_number";
pub const L1_FINALIZED: &str = "kardamom_da_watcher_l1_finalized_block_number";
pub const DEPOSITS_DETECTED_TOTAL: &str = "kardamom_da_watcher_deposits_detected_total";
pub const TICK_TOTAL: &str = "kardamom_da_watcher_tick_total";

pub fn describe() {
    metrics::describe_gauge!(L1_HEAD, "latest L1 block number observed");
    metrics::describe_gauge!(L1_FINALIZED, "latest finalised L1 block number observed");
    metrics::describe_counter!(DEPOSITS_DETECTED_TOTAL, "deposits detected from L1");
    metrics::describe_counter!(TICK_TOTAL, "watcher loop ticks, labelled by outcome");
}
