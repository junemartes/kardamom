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

// Interop watcher (crate::interop) counterparts. All carry an `origin` label
// (the peer chain id) because one process may watch several peers and a
// per-pair fault must be attributable without log spelunking.
pub const REMOTE_MESSAGES_RECEIVED_TOTAL: &str =
    "kardamom_da_watcher_remote_messages_received_total";
pub const REMOTE_MESSAGES_TOTAL: &str = "kardamom_da_watcher_remote_messages_total";
pub const REMOTE_EPOCHS_PUBLISHED_TOTAL: &str = "kardamom_da_watcher_remote_epochs_published_total";
pub const REMOTE_FEED_RESUBSCRIBE_TOTAL: &str = "kardamom_da_watcher_remote_resubscribe_total";
pub const REMOTE_WATCHER_TICK_TOTAL: &str = "kardamom_da_watcher_remote_tick_total";
/// First per-pair seq not yet canonicalised. A pair that fail-stopped is
/// exactly the one whose gauge is flat while the peer keeps sending — the
/// interop analogue of the L1 origin lag.
pub const REMOTE_CURSOR_SEQ: &str = "kardamom_da_watcher_remote_cursor_seq";

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
    metrics::describe_counter!(
        REMOTE_MESSAGES_RECEIVED_TOTAL,
        "outbox messages read off a peer feed, labelled by origin chain id; a reconnect replays from the cursor so this counts reads, not distinct messages"
    );
    metrics::describe_counter!(
        REMOTE_MESSAGES_TOTAL,
        "cross-chain messages published inside remote epochs, labelled by origin chain id"
    );
    metrics::describe_counter!(
        REMOTE_EPOCHS_PUBLISHED_TOTAL,
        "remote epochs published (one per origin block that carried messages), labelled by origin chain id"
    );
    metrics::describe_counter!(
        REMOTE_FEED_RESUBSCRIBE_TOTAL,
        "peer feed re-subscriptions, labelled by origin chain id and cause (lagged/closed/stream_error)"
    );
    metrics::describe_counter!(
        REMOTE_WATCHER_TICK_TOTAL,
        "interop watcher passes, labelled by outcome and origin chain id; a `fault` outcome is terminal"
    );
    metrics::describe_gauge!(
        REMOTE_CURSOR_SEQ,
        "first per-pair outbox seq not yet canonicalised, labelled by origin chain id"
    );
}
