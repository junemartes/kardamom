//! DA-watcher metric names + registration.
//!
//! The binary owns the exporter (via `kardamom_obs::init`); this module just
//! declares the canonical constant names and the `describe` call that
//! registers human-readable help strings with the Prometheus recorder.

pub const L1_FINALIZED: &str = "kardamom_da_watcher_l1_finalized_block_number";
pub const DEPOSITS_DETECTED_TOTAL: &str = "kardamom_da_watcher_deposits_detected_total";
pub const TICK_TOTAL: &str = "kardamom_da_watcher_tick_total";
pub const EPOCHS_PUBLISHED_TOTAL: &str = "kardamom_da_watcher_epochs_published_total";
/// Highest L1 block number the watcher has published an epoch for. Together
/// with `L1_FINALIZED` this is the origin lag — the rule-5 liveness alarm.
pub const EPOCH_ORIGIN: &str = "kardamom_da_watcher_epoch_origin_block_number";

pub fn describe() {
    metrics::describe_gauge!(L1_FINALIZED, "latest finalised L1 block number observed");
    metrics::describe_counter!(
        DEPOSITS_DETECTED_TOTAL,
        "deposit publishes; a range retried after backpressure re-counts its already-published deposits"
    );
    metrics::describe_counter!(TICK_TOTAL, "watcher loop ticks, labelled by outcome");
    metrics::describe_counter!(
        EPOCHS_PUBLISHED_TOTAL,
        "epochs published; one per finalized L1 block, including depositless ones"
    );
    metrics::describe_gauge!(
        EPOCH_ORIGIN,
        "highest L1 block number an epoch has been published for; L1_FINALIZED minus this is the origin lag"
    );
}
