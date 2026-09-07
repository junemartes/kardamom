//! Sequencer metrics.
//!
//! The binary owns the exporter. This module only declares names and helper
//! functions. The default no-op recorder is for smoke tests only. Production
//! binaries wire up `metrics-exporter-prometheus`, per the workspace stack.

use metrics::{counter, gauge, histogram};

/// Read from outside the crate by the `/metrics` endpoint test.
pub const TX_INGESTED: &str = "kardamom_sequencer_tx_ingested_total";
pub(crate) const TX_PUBLISHED_TO_B: &str = "kardamom_sequencer_tx_published_to_b_total";
pub(crate) const TX_BUFFERED_FUTURE: &str = "kardamom_sequencer_tx_buffered_future_total";
pub(crate) const TX_DROPPED_PAST: &str = "kardamom_sequencer_tx_dropped_past_total";
pub(crate) const PENDING_BUFFER_EVICTIONS: &str = "kardamom_sequencer_pending_evictions_total";
pub(crate) const BACKPRESSURE_EVENTS: &str = "kardamom_sequencer_backpressure_total";
pub(crate) const NONCE_CHECK_DURATION_SECONDS: &str =
    "kardamom_sequencer_nonce_check_duration_seconds";

// Lag detection and receipt-floor resync.
pub(crate) const RESYNC_MODE: &str = "kardamom_sequencer_resync_mode";
pub(crate) const RESYNC_ENTERED: &str = "kardamom_sequencer_resync_entered_total";
/// The egress FEED thread bumps this counter as soon as it sees a lag
/// signature (a boundary-arrival gap past the silence threshold). This
/// works even if the publish loop stalls, unlike `RESYNC_ENTERED`, which
/// needs the publish loop to run. The chaos suite checks this counter.
pub(crate) const RESYNC_LAG_SUSPECTED: &str = "kardamom_sequencer_resync_lag_suspected_total";
pub(crate) const RESYNC_SKIPPED_EXECUTED: &str = "kardamom_sequencer_resync_skipped_executed_total";
pub(crate) const RECEIPT_FLOOR_SENDERS: &str = "kardamom_sequencer_receipt_floor_senders";
pub(crate) const RECEIPT_FLOOR_ADVANCES: &str = "kardamom_sequencer_receipt_floor_advances_total";
pub(crate) const CANONICAL_WATERMARK: &str = "kardamom_sequencer_canonical_watermark";
/// A gauge for refs that are published but not yet receipt-confirmed as
/// committed. A counter for refs that are rewound and republished after
/// the confirm timeout. A steady nonzero republish rate means offers land
/// in a void (a dead-leader window), or receipts are not flowing.
pub(crate) const REF_UNCONFIRMED: &str = "kardamom_sequencer_ref_unconfirmed";
pub(crate) const REF_REPUBLISHED: &str = "kardamom_sequencer_ref_republished_total";

/// Remote epochs, and the messages inside them, relayed from
/// `tx_remote_epochs` onto the canonical stream. Labeled by origin chain,
/// not by partition: a peer pairing is its own fault domain, so a stalled
/// pair must be attributable to the peer, not to a shard. Racing
/// sequencers each count their own offer, so these are relay attempts.
/// Cluster dedup collapses the M copies downstream.
pub(crate) const REMOTE_EPOCHS_RELAYED: &str = "kardamom_sequencer_remote_epochs_relayed_total";
pub(crate) const REMOTE_MESSAGES_RELAYED: &str = "kardamom_sequencer_remote_messages_relayed_total";

/// Pre-registered per-partition metric handles for the hot loop.
///
/// Each `counter!(NAME, "partition" => p.to_string())` call boxes a new
/// metrics `Key` and allocates the label `String`. The allocation profile
/// found this caused 6 of the sequencer's 10 allocations per transaction.
/// The handles are registered once at construction, so recording through
/// them does not allocate.
pub struct HotMetrics {
    pub ingest: metrics::Counter,
    pub publish: metrics::Counter,
    pub buffered_future: metrics::Counter,
    pub dropped_past: metrics::Counter,
    pub evictions: metrics::Counter,
    pub backpressure: metrics::Counter,
    pub nonce_check_seconds: metrics::Histogram,
}

impl HotMetrics {
    #[must_use]
    pub fn new(partition: u32) -> Self {
        let p = partition.to_string();
        Self {
            ingest: counter!(TX_INGESTED, "partition" => p.clone()),
            publish: counter!(TX_PUBLISHED_TO_B, "partition" => p.clone()),
            buffered_future: counter!(TX_BUFFERED_FUTURE, "partition" => p.clone()),
            dropped_past: counter!(TX_DROPPED_PAST, "partition" => p.clone()),
            evictions: counter!(PENDING_BUFFER_EVICTIONS, "partition" => p.clone()),
            backpressure: counter!(BACKPRESSURE_EVENTS, "partition" => p.clone()),
            nonce_check_seconds: histogram!(NONCE_CHECK_DURATION_SECONDS, "partition" => p),
        }
    }
}

/// Increment a partition-labeled counter by `n`. Every per-partition
/// counter recorder in this module is one call to this, instead of its
/// own `counter!(NAME, "partition" => p.to_string()).increment(n)` line.
fn bump(name: &'static str, partition: u32, n: u64) {
    counter!(name, "partition" => partition.to_string()).increment(n);
}

/// Set a partition-labeled gauge to `v`. The gauge counterpart of
/// [`bump`].
fn set(name: &'static str, partition: u32, v: f64) {
    gauge!(name, "partition" => partition.to_string()).set(v);
}

pub(crate) fn record_resync_mode(partition: u32, active: bool) {
    set(RESYNC_MODE, partition, if active { 1.0 } else { 0.0 });
}

pub(crate) fn record_resync_enter(partition: u32) {
    bump(RESYNC_ENTERED, partition, 1);
    record_resync_mode(partition, true);
}

/// Unix timestamp of process start.
/// This is a restart-proof signal for the chaos harness. After a restart,
/// counters reset to the same values as a fresh baseline (entered=1 from
/// the startup resync). But a start time after a known event proves the
/// process is new. This works over plain HTTP, with no docker-exec, which
/// can hang for minutes after a thaw on CI runners.
pub(crate) const START_TIME_SECONDS: &str = "kardamom_sequencer_start_time_seconds";

pub fn record_start_time() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |d| d.as_secs_f64());
    metrics::gauge!(START_TIME_SECONDS).set(now);
}

pub fn record_lag_suspected(partition: u32) {
    bump(RESYNC_LAG_SUSPECTED, partition, 1);
}

pub(crate) fn record_resync_skip(partition: u32, count: u64) {
    bump(RESYNC_SKIPPED_EXECUTED, partition, count);
}

pub(crate) fn record_floor_senders(partition: u32, senders: usize) {
    #[allow(
        clippy::cast_precision_loss,
        reason = "a gauge value; the sender count never nears 2^52, so the f64 mantissa holds it exactly"
    )]
    let senders = senders as f64;
    set(RECEIPT_FLOOR_SENDERS, partition, senders);
}

pub(crate) fn record_floor_advance(partition: u32) {
    bump(RECEIPT_FLOOR_ADVANCES, partition, 1);
}

pub(crate) fn record_canonical_watermark(partition: u32, count: u64) {
    #[allow(
        clippy::cast_precision_loss,
        reason = "a gauge value; the canonical tx count never nears 2^52, so the f64 mantissa holds it exactly"
    )]
    let count = count as f64;
    set(CANONICAL_WATERMARK, partition, count);
}

pub(crate) fn record_unconfirmed_refs(partition: u32, n: usize) {
    #[allow(
        clippy::cast_precision_loss,
        reason = "a gauge value; the unconfirmed-ref count never nears 2^52, so the f64 mantissa holds it exactly"
    )]
    let n = n as f64;
    set(REF_UNCONFIRMED, partition, n);
}

pub(crate) fn record_ref_republished(partition: u32, n: usize) {
    bump(REF_REPUBLISHED, partition, n as u64);
}

pub(crate) fn record_remote_epoch_relayed(origin_chain_id: u64, messages: usize) {
    let origin = origin_chain_id.to_string();
    counter!(REMOTE_EPOCHS_RELAYED, "origin" => origin.clone()).increment(1);
    counter!(REMOTE_MESSAGES_RELAYED, "origin" => origin).increment(messages as u64);
}
