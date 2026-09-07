//! Validator metric names and helpers. This mirrors the lightweight style
//! of the other services: thin wrappers over the `metrics` facade, so the
//! call sites stay readable and the names live in one place.

pub const DIVERGENCE_TOTAL: &str = "validator_divergence_total";
pub const BLOCKS_VERIFIED_TOTAL: &str = "validator_blocks_verified_total";
pub const BAL_MISSING_TOTAL: &str = "validator_bal_missing_total";
pub const RECEIPT_MISSING_TOTAL: &str = "validator_receipt_missing_total";
pub const COMMITTED_BLOCK: &str = "validator_committed_block";
pub const STATE_ROOT_BLOCK: &str = "validator_state_root_block";
/// Epochs whose deposits were re-derived from L1 and matched.
pub const EPOCHS_VERIFIED_TOTAL: &str = "validator_epochs_verified_total";
/// Epochs that failed verification. This is a chain fault, always paired
/// with a divergence.
pub const EPOCH_FAULTS_TOTAL: &str = "validator_epoch_faults_total";
/// Epochs skipped because L1 was unreachable. Not a fault: an RPC outage
/// must not read as a divergence. A sustained non-zero rate means
/// verification coverage has holes.
pub const EPOCHS_UNVERIFIED_TOTAL: &str = "validator_epochs_unverified_total";
/// Remote-epoch records (interop) that passed the inline pair-sequence checks.
pub const REMOTE_EPOCHS_VERIFIED_TOTAL: &str = "validator_remote_epochs_verified_total";
/// Remote-epoch records that FAILED verification — a chain fault, always
/// paired with a divergence.
pub const REMOTE_EPOCH_FAULTS_TOTAL: &str = "validator_remote_epoch_faults_total";
/// Outbox messages extracted from re-executed receipts and fed to the serving
/// feed store (egress spec E1).
pub const OUTBOX_EXTRACTED_TOTAL: &str = "validator_outbox_extracted_total";
/// Blocks whose extracted outbox messages could not be cross-checked against
/// BAL claims (claims never arrived). NOT a fault — the bal_missing posture.
pub const OUTBOX_UNCHECKED_TOTAL: &str = "validator_outbox_unchecked_total";
/// Feed subscriptions rejected because a cap was hit (per destination or
/// total; see `interop::serve::FeedServerLimits`).
pub const FEED_SUBSCRIPTION_REJECTED_TOTAL: &str = "validator_feed_subscription_rejected_total";

/// Register metric descriptions. Call once at startup, after `kardamom_obs::init`.
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
    metrics::describe_counter!(
        FEED_SUBSCRIPTION_REJECTED_TOTAL,
        "Feed subscriptions rejected because a subscription cap was hit"
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

/// Replay-window-overrun resync outcomes, labeled
/// `outcome=peer-checkpoint|unrecoverable`. This is the validator twin of
/// `kardamom_executor_resync_total`. Any `peer-checkpoint` increment also
/// means blocks through the adopted checkpoint are unverified by this
/// validator.
pub fn resync_counter(outcome: &'static str) -> metrics::Counter {
    metrics::counter!(RESYNC_TOTAL, "outcome" => outcome)
}

/// The tx_bal subscription reopened after prolonged silence. This is a
/// never-joined or silently-dead multicast image healing itself.
/// Sustained growth means BAL delivery to this node is genuinely broken.
pub fn counter_bal_sub_reopen() {
    metrics::counter!(BAL_SUB_REOPEN_TOTAL).increment(1);
}

pub fn counter_divergence() {
    metrics::counter!(DIVERGENCE_TOTAL).increment(1);
}

pub fn counter_epoch_verified() {
    metrics::counter!(EPOCHS_VERIFIED_TOTAL).increment(1);
}

pub fn counter_epoch_fault() {
    metrics::counter!(EPOCH_FAULTS_TOTAL).increment(1);
}

pub fn counter_epoch_unverified() {
    metrics::counter!(EPOCHS_UNVERIFIED_TOTAL).increment(1);
}

pub fn counter_remote_epoch_verified() {
    metrics::counter!(REMOTE_EPOCHS_VERIFIED_TOTAL).increment(1);
}

pub fn counter_remote_epoch_fault() {
    metrics::counter!(REMOTE_EPOCH_FAULTS_TOTAL).increment(1);
}

pub fn counter_outbox_extracted(n: usize) {
    metrics::counter!(OUTBOX_EXTRACTED_TOTAL).increment(n as u64);
}

pub fn counter_outbox_unchecked() {
    metrics::counter!(OUTBOX_UNCHECKED_TOTAL).increment(1);
}

pub fn counter_feed_subscription_rejected() {
    metrics::counter!(FEED_SUBSCRIPTION_REJECTED_TOTAL).increment(1);
}

/// Blocks re-executed as seeded parallel batches. The label is the batch count.
pub fn counter_parallel_block(batches: usize) {
    metrics::counter!("kardamom_validator_parallel_blocks_total").increment(1);
    metrics::histogram!("kardamom_validator_parallel_batches").record(batches as f64);
}

/// Blocks that fell back to sequential re-execution: claims absent, or deposits.
pub fn counter_parallel_fallback() {
    metrics::counter!("kardamom_validator_parallel_fallback_total").increment(1);
}

/// Pool workers that could not mint an independent snapshot fork
/// (`StateDatabase::fork_view` refused because the writer advanced
/// mid-mint) and fell back to sharing the strategy's snapshot. This is
/// correct but serialized. A sustained non-zero rate means the mdbx
/// read-parallelism fix is off.
pub fn counter_fork_fallback(workers: u64) {
    metrics::counter!("kardamom_validator_snapshot_fork_fallback_total").increment(workers);
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

/// Record that `block`'s MPT state root was actually observed on the
/// committed snapshot. This is kept separate from [`set_committed_block`],
/// so the "state root advancing" signal is a real measurement, not a
/// mirror of the committed-block gauge.
pub fn set_state_root_block(block: u64) {
    metrics::gauge!(STATE_ROOT_BLOCK).set(block as f64);
}

/// Prover-spool outcomes (spec 3c): frames written, blocks dropped (the
/// pre-state window was missed, or records aged out), assembly failures.
pub fn counter_prover_spooled() {
    metrics::counter!("validator_prover_frames_spooled_total").increment(1);
}
pub fn counter_prover_skipped(n: u64) {
    metrics::counter!("validator_prover_blocks_skipped_total").increment(n);
}
pub fn counter_prover_failed() {
    metrics::counter!("validator_prover_blocks_failed_total").increment(1);
}
