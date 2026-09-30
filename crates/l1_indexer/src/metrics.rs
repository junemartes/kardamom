//! The indexer's metrics.

pub const L1_FINALIZED: &str = "kardamom_l1_indexer_l1_finalized_block_number";
pub const INDEXED_BLOCK: &str = "kardamom_l1_indexer_indexed_block_number";
pub const LAST_BATCH: &str = "kardamom_l1_indexer_last_batch_index";
pub const BATCHES_TOTAL: &str = "kardamom_l1_indexer_batches_total";
pub const BLOBS_TOTAL: &str = "kardamom_l1_indexer_blobs_total";
pub const TICK_TOTAL: &str = "kardamom_l1_indexer_tick_total";

/// Describe every metric once.
pub fn describe() {
    metrics::describe_gauge!(L1_FINALIZED, "latest finalized L1 block number observed");
    metrics::describe_gauge!(
        INDEXED_BLOCK,
        "highest L1 block whose batches and epoch are indexed"
    );
    metrics::describe_gauge!(LAST_BATCH, "highest batch index indexed");
    metrics::describe_counter!(BATCHES_TOTAL, "batches indexed");
    metrics::describe_counter!(BLOBS_TOTAL, "blobs stored");
    metrics::describe_counter!(TICK_TOTAL, "ticks, by outcome");
}

/// A counter or a block number as a gauge value.
#[allow(
    clippy::cast_precision_loss,
    reason = "a gauge is an f64; block numbers stay far below 2^53"
)]
#[must_use]
pub const fn gauge_value(n: u64) -> f64 {
    n as f64
}
