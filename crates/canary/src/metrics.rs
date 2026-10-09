//! The canary's metric names, their descriptions, and the helpers that
//! write them. The binary owns the exporter through `kardamom_obs`.

use std::time::{Duration, SystemTime};

use alloy_primitives::U256;

/// Probe runs, by `probe`, `endpoint` and `outcome`. A failure outcome
/// adds one detail label: `code`, `reason`, `stage` or `field`.
pub const PROBE_TOTAL: &str = "kardamom_canary_probe_total";
/// Stage latencies, by `probe`, `endpoint` and `stage`.
pub const STAGE_SECONDS: &str = "kardamom_canary_stage_seconds";
/// The unix time of the last success, by `probe`.
pub const LAST_SUCCESS: &str = "kardamom_canary_last_success_timestamp_seconds";
/// Account balances, by `layer` (`l2`, `l1`) and `account`.
pub const BALANCE_WEI: &str = "kardamom_canary_balance_wei";
/// The balance under which an account is unfunded, by `layer`.
pub const BALANCE_FLOOR_WEI: &str = "kardamom_canary_balance_floor_wei";
/// 1 while a ring account holds a transaction the canary cannot resolve,
/// by `account`.
pub const ACCOUNT_STALLED: &str = "kardamom_canary_account_stalled";
/// The nonce of that transaction, by `account`.
pub const STALLED_NONCE: &str = "kardamom_canary_account_stalled_nonce";
/// Status feed delivery gaps, by `kind`: `lagged`, `disconnect`, or
/// `missing_<stage>` for a stage that never arrived though the receipt
/// did. A gap is not a transaction failure.
pub const FEED_GAPS_TOTAL: &str = "kardamom_canary_feed_gaps_total";
/// 1 while the status feed subscription is open.
pub const FEED_CONNECTED: &str = "kardamom_canary_feed_connected";

/// A wei amount as a gauge value.
#[allow(
    clippy::cast_precision_loss,
    reason = "a balance gauge; the dashboards show ETH, far above the f64 rounding"
)]
#[must_use]
pub fn wei(amount: U256) -> f64 {
    let as_f64: f64 = amount.into();
    as_f64
}

pub fn describe() {
    metrics::describe_counter!(
        PROBE_TOTAL,
        "canary probe runs, by probe, endpoint and outcome"
    );
    metrics::describe_histogram!(
        STAGE_SECONDS,
        metrics::Unit::Seconds,
        "canary stage latencies, by probe, endpoint and stage"
    );
    metrics::describe_gauge!(LAST_SUCCESS, "unix time of the last success, by probe");
    metrics::describe_gauge!(
        BALANCE_WEI,
        "canary account balances in wei, by layer and account"
    );
    metrics::describe_gauge!(BALANCE_FLOOR_WEI, "the unfunded floor in wei, by layer");
    metrics::describe_gauge!(
        ACCOUNT_STALLED,
        "1 while a ring account holds an unresolved transaction"
    );
    metrics::describe_gauge!(
        STALLED_NONCE,
        "the nonce of a ring account's unresolved transaction"
    );
    metrics::describe_counter!(FEED_GAPS_TOTAL, "status feed delivery gaps, by kind");
    metrics::describe_gauge!(
        FEED_CONNECTED,
        "1 while the status feed subscription is open"
    );
}

/// One stage latency.
pub fn stage(probe: &'static str, endpoint: &str, stage: &'static str, took: Duration) {
    metrics::histogram!(
        STAGE_SECONDS,
        "probe" => probe,
        "endpoint" => endpoint.to_string(),
        "stage" => stage,
    )
    .record(took.as_secs_f64());
}

/// Mark a success of `probe` now.
pub fn success_now(probe: &'static str) {
    metrics::gauge!(LAST_SUCCESS, "probe" => probe)
        .set(kardamom_obs::ready::unix_seconds(SystemTime::now()));
}

/// One feed gap of `kind`.
pub fn feed_gap(kind: &'static str) {
    metrics::counter!(FEED_GAPS_TOTAL, "kind" => kind).increment(1);
}

/// Mark `account` stalled on the transaction at `nonce`, or clear the
/// mark with `None`.
#[allow(
    clippy::cast_precision_loss,
    reason = "a nonce gauge; no ring account reaches 2^53 transactions"
)]
pub fn stalled(account: alloy_primitives::Address, nonce: Option<u64>) {
    let label = format!("{account:#x}");
    metrics::gauge!(ACCOUNT_STALLED, "account" => label.clone()).set(if nonce.is_some() {
        1.0
    } else {
        0.0
    });
    if let Some(n) = nonce {
        metrics::gauge!(STALLED_NONCE, "account" => label).set(n as f64);
    }
}
