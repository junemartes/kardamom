//! The indexer's metrics.

pub const L1_FINALIZED: &str = "kardamom_l1_indexer_l1_finalized_block_number";
pub const INDEXED_BLOCK: &str = "kardamom_l1_indexer_indexed_block_number";
pub const LAST_BATCH: &str = "kardamom_l1_indexer_last_batch_index";
pub const BATCHES_TOTAL: &str = "kardamom_l1_indexer_batches_total";
pub const PAYLOAD_BYTES_TOTAL: &str = "kardamom_l1_indexer_payload_bytes_total";
pub const TICK_TOTAL: &str = "kardamom_l1_indexer_tick_total";
/// The unix time of the last completed tick.
pub const LAST_TICK_UNIX_SECONDS: &str = "kardamom_l1_indexer_last_tick_unix_seconds";
/// The unix time the follower plans to read L1 next. The follower is ready
/// while now is before it plus one slot; the wake alert fires when a wake
/// is overdue.
pub const NEXT_WAKE: &str = "kardamom_l1_follower_next_wake_seconds";
/// The newest block the follower published on `l1_blocks`.
pub const PUBLISHED_BLOCK: &str = "kardamom_l1_follower_published_block_number";
/// The follower's L1 reads, by kind: the provider cost of the follower.
pub const L1_READS_TOTAL: &str = "kardamom_l1_follower_l1_reads_total";

/// Describe every metric once.
pub fn describe() {
    metrics::describe_gauge!(L1_FINALIZED, "latest finalized L1 block number observed");
    metrics::describe_gauge!(
        INDEXED_BLOCK,
        "highest L1 block whose batches and epoch are indexed"
    );
    metrics::describe_gauge!(LAST_BATCH, "highest batch index indexed");
    metrics::describe_counter!(BATCHES_TOTAL, "batches indexed");
    metrics::describe_counter!(PAYLOAD_BYTES_TOTAL, "payload bytes stored");
    metrics::describe_counter!(TICK_TOTAL, "ticks, by outcome");
    metrics::describe_gauge!(NEXT_WAKE, "unix time of the next planned L1 read");
    metrics::describe_gauge!(
        PUBLISHED_BLOCK,
        "newest L1 block published on the l1_blocks stream"
    );
    metrics::describe_counter!(
        L1_READS_TOTAL,
        "L1 reads, by kind: tip, headers (one batch), logs, light_client"
    );
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
