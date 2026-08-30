//! Skip-path metric emission. The executor's full metric namespace lives in
//! `kardamom-engine`'s `metrics` module (which re-exports this constant for
//! `describe()`); this one counter is emitted from inside
//! [`crate::executor`]'s `invalid_skip`, which is `no_std` code — so the
//! emission lives here, std-gated, while the skip semantics are not.

/// Deterministically-invalid canonical txs skipped with a marker receipt
/// (#92). ANY nonzero value means an upstream guard (sequencer nonce fence,
/// cluster dedup, resync floors) let an invalid record into the canonical
/// log — standing-alert material.
pub const INVALID_TX_SKIPPED_TOTAL: &str = "kardamom_executor_invalid_tx_skipped_total";

pub fn record_invalid_tx_skipped(reason: kardamom_types::SkipReason) {
    metrics::counter!(INVALID_TX_SKIPPED_TOTAL, "reason" => reason.as_str()).increment(1);
}
