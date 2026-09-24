//! Cache metric names and helpers, in one place.

use crate::client::RowsWritten;

/// Lookups by `layer` (`live`, `redis`) and `outcome` (`hit`, `miss`,
/// `error`, `timeout`).
pub const LOOKUPS_TOTAL: &str = "kardamom_cache_lookups_total";
/// Wall time of one Redis lookup.
pub const LOOKUP_SECONDS: &str = "kardamom_cache_lookup_seconds";
/// Times a reader skipped its checks, by `reason` (`stale`, `gap`,
/// `error`, `disabled`).
pub const DEGRADED_TOTAL: &str = "kardamom_cache_degraded_total";
/// The newest position a reader has seen minus the mirror head.
pub const HEAD_LAG_TXS: &str = "kardamom_cache_head_lag_txs";
/// Rows written to Redis, by `outcome` (`applied`, `discarded`,
/// `disagreed`).
pub const ROWS_WRITTEN_TOTAL: &str = "kardamom_cache_rows_written_total";

/// Register the descriptions. Call once at startup.
pub fn describe() {
    metrics::describe_counter!(LOOKUPS_TOTAL, "cache lookups, by layer and outcome");
    metrics::describe_histogram!(LOOKUP_SECONDS, "wall time of one Redis lookup");
    metrics::describe_counter!(
        DEGRADED_TOTAL,
        "times a reader skipped its admission checks, by reason"
    );
    metrics::describe_gauge!(
        HEAD_LAG_TXS,
        "the newest position a reader has seen minus the mirror head, in canonical positions"
    );
    metrics::describe_counter!(
        ROWS_WRITTEN_TOTAL,
        "account rows written to Redis, by outcome"
    );
}

pub fn record_lookup(layer: &'static str, outcome: &'static str) {
    metrics::counter!(LOOKUPS_TOTAL, "layer" => layer, "outcome" => outcome).increment(1);
}

pub fn record_lookup_seconds(seconds: f64) {
    metrics::histogram!(LOOKUP_SECONDS).record(seconds);
}

pub fn record_degraded(reason: &'static str) {
    metrics::counter!(DEGRADED_TOTAL, "reason" => reason).increment(1);
}

pub fn set_head_lag(txs: u64) {
    // A gauge is an f64. A lag beyond 2^53 positions is not a real value.
    #[allow(clippy::cast_precision_loss)]
    metrics::gauge!(HEAD_LAG_TXS).set(txs as f64);
}

pub fn record_rows_written(written: &RowsWritten) {
    metrics::counter!(ROWS_WRITTEN_TOTAL, "outcome" => "applied").increment(written.applied);
    metrics::counter!(ROWS_WRITTEN_TOTAL, "outcome" => "discarded").increment(written.discarded);
    metrics::counter!(ROWS_WRITTEN_TOTAL, "outcome" => "disagreed").increment(written.disagreed);
}
