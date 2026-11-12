//! Notifier metrics: names and descriptions. The binary owns the
//! exporter through `kardamom_obs::init`.

/// Events the ring stored, by `stage`.
pub const EVENTS_TOTAL: &str = "kardamom_notifier_events_total";
/// Stages the ring already held: the executors' receipt copies and the
/// ingress replicas' `sealed` copies.
pub const DUPLICATES_TOTAL: &str = "kardamom_notifier_duplicates_total";
/// `tx_errors` records the ring could not join to a hash: no stage named
/// that sender and nonce.
pub const UNRESOLVED_ERRORS_TOTAL: &str = "kardamom_notifier_unresolved_errors_total";
/// Events the ring evicted by age or by count.
pub const EVICTED_TOTAL: &str = "kardamom_notifier_evicted_total";
/// Events in the ring.
pub const RING_EVENTS: &str = "kardamom_notifier_ring_events";
/// Transactions with at least one event in the ring.
pub const RING_TRANSACTIONS: &str = "kardamom_notifier_ring_transactions";
/// Open WebSocket subscriptions.
pub const WS_SUBSCRIPTIONS: &str = "kardamom_notifier_ws_subscriptions";
/// Events sent to WebSocket subscribers.
pub const WS_EVENTS_TOTAL: &str = "kardamom_notifier_ws_events_total";
/// Lag markers sent to slow WebSocket subscribers.
pub const WS_LAGGED_TOTAL: &str = "kardamom_notifier_ws_lagged_total";
/// The events path (`KARDAMOM_NOTIFIER_EVENTS`): seconds from the first
/// observed event of a hash to the first observed event of each later
/// stage, by `stage`. A stage counts when an `accepted` came by then.
pub const STAGE_LATENCY_SECONDS: &str = "kardamom_notifier_stage_latency_seconds";
/// Seconds from the first observed event of a hash to the receipt of each
/// executor on `tx_receipts`, by `executor` (the slot of its session).
pub const EXECUTED_LATENCY_SECONDS: &str = "kardamom_notifier_executed_latency_seconds";
/// Seconds from the first `executed` of a hash to its first `receipt`.
pub const MERGE_LATENCY_SECONDS: &str = "kardamom_notifier_merge_latency_seconds";
/// Events of the stage table that were not sent, by `stage`: a copy, or
/// an event that came after a later stage.
pub const STAGE_NOT_SENT_TOTAL: &str = "kardamom_notifier_stage_not_sent_total";
/// Entries that a full probe of the stage table overwrote.
pub const STAGE_TABLE_EVICTED_TOTAL: &str = "kardamom_notifier_stage_table_evicted_total";
/// Jumps in the `seq` of a publisher's `tx_events` frames, by `origin`.
pub const EVENT_GAPS_TOTAL: &str = "kardamom_notifier_event_gaps_total";
/// `tx_events` frames that did not read.
pub const BAD_FRAMES_TOTAL: &str = "kardamom_notifier_bad_frames_total";
/// A rollback that reached an entry at `posted` or above. Any increase
/// needs an operator.
pub const ROLLBACK_PAST_POSTED_TOTAL: &str = "kardamom_notifier_rollback_past_posted_total";
/// `tx_status` and `tx_errors` records that the events path replaces,
/// dropped by the hub.
pub const SUPERSEDED_TOTAL: &str = "kardamom_notifier_superseded_total";

/// A count as a gauge value.
#[allow(
    clippy::cast_precision_loss,
    reason = "a gauge of a count; no count here reaches 2^53"
)]
#[must_use]
pub fn count(n: usize) -> f64 {
    n as f64
}

pub fn describe() {
    metrics::describe_counter!(EVENTS_TOTAL, "status events stored in the ring, by stage");
    metrics::describe_counter!(
        DUPLICATES_TOTAL,
        "status events dropped because the transaction already held the stage"
    );
    metrics::describe_counter!(
        UNRESOLVED_ERRORS_TOTAL,
        "tx_errors records with no known transaction hash"
    );
    metrics::describe_counter!(EVICTED_TOTAL, "events evicted from the ring");
    metrics::describe_gauge!(RING_EVENTS, "events in the ring");
    metrics::describe_gauge!(RING_TRANSACTIONS, "transactions in the ring");
    metrics::describe_gauge!(WS_SUBSCRIPTIONS, "open WebSocket subscriptions");
    metrics::describe_counter!(WS_EVENTS_TOTAL, "events sent to WebSocket subscribers");
    metrics::describe_counter!(WS_LAGGED_TOTAL, "lag markers sent to WebSocket subscribers");
    metrics::describe_histogram!(
        STAGE_LATENCY_SECONDS,
        "seconds from the first observed event of a hash to each later stage, by stage"
    );
    metrics::describe_histogram!(
        EXECUTED_LATENCY_SECONDS,
        "seconds from the first observed event of a hash to the receipt of each executor"
    );
    metrics::describe_histogram!(
        MERGE_LATENCY_SECONDS,
        "seconds from the first executed of a hash to its first receipt"
    );
    metrics::describe_counter!(STAGE_NOT_SENT_TOTAL, "stage events not sent, by stage");
    metrics::describe_counter!(
        STAGE_TABLE_EVICTED_TOTAL,
        "stage table entries a full probe overwrote"
    );
    metrics::describe_counter!(
        EVENT_GAPS_TOTAL,
        "jumps in the seq of tx_events frames, by origin"
    );
    metrics::describe_counter!(BAD_FRAMES_TOTAL, "tx_events frames that did not read");
    metrics::describe_counter!(
        ROLLBACK_PAST_POSTED_TOTAL,
        "rollbacks that reached an entry at posted or above"
    );
    metrics::describe_counter!(
        SUPERSEDED_TOTAL,
        "tx_status and tx_errors records that the events path replaces"
    );
}
