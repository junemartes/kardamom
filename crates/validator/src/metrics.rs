//! Validator metric names + helpers. Mirrors the lightweight style of the other
//! services: thin wrappers over the `metrics` facade so the call sites stay
//! readable and the names live in one place.

pub const DIVERGENCE_TOTAL: &str = "validator_divergence_total";
pub const BLOCKS_VERIFIED_TOTAL: &str = "validator_blocks_verified_total";
pub const BAL_MISSING_TOTAL: &str = "validator_bal_missing_total";
pub const RECEIPT_MISSING_TOTAL: &str = "validator_receipt_missing_total";
pub const COMMITTED_BLOCK: &str = "validator_committed_block";
pub const STATE_ROOT_BLOCK: &str = "validator_state_root_block";

/// Register metric descriptions. Call once at startup (after `kardamom_obs::init`).
pub fn describe() {
    metrics::describe_counter!(
        DIVERGENCE_TOTAL,
        "Proven divergences (write-set or receipt) between local re-execution and the sequencer"
    );
    metrics::describe_counter!(
        BLOCKS_VERIFIED_TOTAL,
        "Blocks whose write-set matched the executor's BAL"
    );
    metrics::describe_counter!(
        BAL_MISSING_TOTAL,
        "Blocks for which no BAL arrived within the wait window (left unverified)"
    );
    metrics::describe_counter!(
        RECEIPT_MISSING_TOTAL,
        "Receipts for which no published receipt arrived within the wait window"
    );
    metrics::describe_gauge!(COMMITTED_BLOCK, "Highest block the validator has committed");
    metrics::describe_gauge!(
        STATE_ROOT_BLOCK,
        "Block number of the most recent OBSERVED MPT state root (set only when \
         the committed snapshot actually yielded a root — an independent \
         measurement, not a mirror of validator_committed_block)"
    );
}

pub const RESYNC_TOTAL: &str = "validator_resync_total";
pub const BAL_SUB_REOPEN_TOTAL: &str = "validator_bal_sub_reopen_total";

/// Replay-window-overrun resync outcomes (#143), labeled
/// `outcome=peer-checkpoint|unrecoverable` — the validator twin of
/// `kardamom_executor_resync_total`. Any `peer-checkpoint` increment also
/// means blocks through the adopted checkpoint are UNVERIFIED by this
/// validator (same trust class as #78 catch-up).
pub fn resync_counter(outcome: &'static str) -> metrics::Counter {
    metrics::counter!(RESYNC_TOTAL, "outcome" => outcome)
}

/// tx_bal subscription reopened after prolonged silence (#144) — a never-
/// joined or silently-dead multicast image self-healing. Sustained growth
/// means BAL delivery to this node is genuinely broken.
pub fn counter_bal_sub_reopen() {
    metrics::counter!(BAL_SUB_REOPEN_TOTAL).increment(1);
}

pub fn counter_divergence() {
    metrics::counter!(DIVERGENCE_TOTAL).increment(1);
}

/// Blocks re-executed as seeded parallel batches (label: batch count).
pub fn counter_parallel_block(batches: usize) {
    metrics::counter!("kardamom_validator_parallel_blocks_total").increment(1);
    metrics::histogram!("kardamom_validator_parallel_batches").record(batches as f64);
}

/// Blocks that fell back to sequential re-execution (claims absent/deposits).
pub fn counter_parallel_fallback() {
    metrics::counter!("kardamom_validator_parallel_fallback_total").increment(1);
}

pub fn counter_block_verified() {
    metrics::counter!(BLOCKS_VERIFIED_TOTAL).increment(1);
}

pub fn counter_bal_missing() {
    metrics::counter!(BAL_MISSING_TOTAL).increment(1);
}

pub fn counter_receipt_missing() {
    metrics::counter!(RECEIPT_MISSING_TOTAL).increment(1);
}

/// Record that the validator has committed `block`.
pub fn set_committed_block(block: u64) {
    metrics::gauge!(COMMITTED_BLOCK).set(block as f64);
}

/// Record that `block`'s MPT state root was actually observed on the committed
/// snapshot. Kept separate from [`set_committed_block`] so the "state root
/// advancing" signal is a real measurement rather than a mirror of the
/// committed-block gauge.
pub fn set_state_root_block(block: u64) {
    metrics::gauge!(STATE_ROOT_BLOCK).set(block as f64);
}
