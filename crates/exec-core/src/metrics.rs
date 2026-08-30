//! Skip-path metric emission.
//!
//! The executor's full metric namespace lives in `kardamom-engine`'s
//! `metrics` module, which re-exports this constant for `describe()`. This
//! one counter is emitted inside [`crate::executor`]'s `invalid_skip`, which
//! is `no_std` code. So the emission lives here, gated by `std`, while the
//! skip logic is not.

/// Count of deterministically invalid canonical transactions skipped, each
/// with a marker receipt. A nonzero value means an upstream guard (sequencer
/// nonce fence, cluster dedup, or resync floor) let an invalid record into
/// the canonical log. Treat this as a standing alert.
pub const INVALID_TX_SKIPPED_TOTAL: &str = "kardamom_executor_invalid_tx_skipped_total";

pub fn record_invalid_tx_skipped() {
    metrics::counter!(INVALID_TX_SKIPPED_TOTAL).increment(1);
}
