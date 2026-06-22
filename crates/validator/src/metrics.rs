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
        "Block number of the most recent computed state root"
    );
}

pub fn counter_divergence() {
    metrics::counter!(DIVERGENCE_TOTAL).increment(1);
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

/// Record that the validator has committed `block` (with its state root present).
pub fn set_committed_block(block: u64) {
    metrics::gauge!(COMMITTED_BLOCK).set(block as f64);
    metrics::gauge!(STATE_ROOT_BLOCK).set(block as f64);
}
