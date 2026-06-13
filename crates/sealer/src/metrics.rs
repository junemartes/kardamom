//! Sealer metric names + registration.
//!
//! The binary owns the exporter (via `kardamom_obs::init`); this module just
//! declares the canonical constant names and the `describe` call that
//! registers human-readable help strings with the Prometheus recorder.

pub const BOUNDARIES_EMITTED_TOTAL: &str = "kardamom_sealer_boundaries_emitted_total";
pub const BLOCK_NUMBER: &str = "kardamom_sealer_block_number";
pub const TICK_SKIPPED_TOTAL: &str = "kardamom_sealer_tick_skipped_total";

pub fn describe() {
    metrics::describe_counter!(BOUNDARIES_EMITTED_TOTAL, "block boundaries emitted");
    metrics::describe_gauge!(BLOCK_NUMBER, "block number of the last emitted boundary");
    metrics::describe_counter!(TICK_SKIPPED_TOTAL, "ticks skipped, labelled by reason");
}
