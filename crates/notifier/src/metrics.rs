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
/// Registered webhook subscriptions this instance holds, by `owned`.
pub const WEBHOOK_SUBSCRIPTIONS: &str = "kardamom_notifier_webhook_subscriptions";
/// Live events the webhook fan-out missed because it lagged the feed.
pub const WEBHOOK_FEED_LAGGED_TOTAL: &str = "kardamom_notifier_webhook_feed_lagged_total";
/// Live events a full worker queue dropped, by `subscription`. The
/// fan-out never waits for one subscription's queue.
pub const WEBHOOK_QUEUE_FULL_TOTAL: &str = "kardamom_notifier_webhook_queue_full_total";
/// Events appended to an outbox, by `subscription`.
pub const OUTBOX_APPENDED_TOTAL: &str = "kardamom_notifier_outbox_appended_total";
/// Events a delivery loop finished with, by `subscription` and `outcome`
/// (`delivered` or `gave_up`).
pub const OUTBOX_DELIVERED_TOTAL: &str = "kardamom_notifier_outbox_delivered_total";
/// Bytes of an outbox not yet delivered, by `subscription`.
pub const OUTBOX_BACKLOG_BYTES: &str = "kardamom_notifier_outbox_backlog_bytes";
/// Webhook POST attempts, by `outcome` (`ok`, `status`, `error`).
pub const WEBHOOK_ATTEMPTS_TOTAL: &str = "kardamom_notifier_webhook_attempts_total";
/// Seconds from the event's arrival at the notifier to its delivery.
pub const WEBHOOK_DELIVERY_SECONDS: &str = "kardamom_notifier_webhook_delivery_seconds";

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
    metrics::describe_gauge!(
        WEBHOOK_SUBSCRIPTIONS,
        "registered webhook subscriptions, by owned"
    );
    metrics::describe_counter!(
        WEBHOOK_FEED_LAGGED_TOTAL,
        "live events the webhook fan-out missed"
    );
    metrics::describe_counter!(
        WEBHOOK_QUEUE_FULL_TOTAL,
        "live events a full webhook worker queue dropped"
    );
    metrics::describe_counter!(OUTBOX_APPENDED_TOTAL, "events appended to an outbox");
    metrics::describe_counter!(
        OUTBOX_DELIVERED_TOTAL,
        "events a delivery loop finished with, by outcome"
    );
    metrics::describe_gauge!(OUTBOX_BACKLOG_BYTES, "outbox bytes not yet delivered");
    metrics::describe_counter!(WEBHOOK_ATTEMPTS_TOTAL, "webhook POST attempts, by outcome");
    metrics::describe_histogram!(
        WEBHOOK_DELIVERY_SECONDS,
        "seconds from event arrival to webhook delivery"
    );
}
