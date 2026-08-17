//! Metric name constants for the executor. Emission sites live in
//! `actor.rs` (block-apply, state-commit, block-number gauge) and
//! `executor.rs` (per-tx counter via `execute_tx` / `execute_deposit_tx`
//! return values inspected in `actor::spawn_exec`).

pub const TX_APPLIED_TOTAL: &str = "kardamom_executor_tx_applied_total";
pub const BLOCK_APPLY_DURATION_SECONDS: &str = "kardamom_executor_block_apply_duration_seconds";
pub const STATE_COMMIT_DURATION_SECONDS: &str = "kardamom_executor_state_commit_duration_seconds";
pub const BLOCK_NUMBER: &str = "kardamom_executor_block_number";

// The clustered sealer (the Java Aeron Cluster service) exposes no Prometheus
// endpoint of its own, so the EXECUTOR re-exports the sealer's output as it
// decodes cluster egress (`reader/cluster.rs`): each in-order delivered
// Boundary bumps the counter and sets the gauge to the sealer's declared block
// number. They measure the boundary stream as observed at that executor's
// subscription, not JVM-internal state. The validator consumes the same
// shared subscription but constructs it with the emission SUPPRESSED
// (`suppress_sealer_metrics`), so only executor exporters publish these
// series; probes may still `max()` across the executor replicas (each replica
// re-exports its own view).
pub const SEALER_BLOCK_NUMBER: &str = "kardamom_sealer_block_number";
pub const SEALER_BOUNDARIES_TOTAL: &str = "kardamom_sealer_boundaries_emitted_total";

// Full-resync fallback (replay window overrun): bumped by the executor binary
// when the cluster refuses REPLAY_FROM (`REPLAY_UNAVAILABLE`) and the node
// repairs itself with a peer checkpoint — or fails to. Labelled
// `outcome=peer-checkpoint|unrecoverable`. Rare by design; any non-zero rate
// is worth an alert (a node fell behind the retention window).
/// EIP-7928 BAL publication (spec: bal-attribution-parallel-validation).
pub const BAL_FRAME_BYTES: &str = "kardamom_executor_bal_frame_bytes";
pub const BAL_ENCODE_SECONDS: &str = "kardamom_executor_bal_encode_seconds";
pub const BAL_PUBLISH_TOTAL: &str = "kardamom_executor_bal_publish_total";
pub const BAL_RETAINED_BLOCKS: &str = "kardamom_executor_bal_retained_blocks";

/// Health-beacon beats recorded (i.e. blocks closed with the health-check
/// feature active). Flat at 0 while the feature is dormant; once activated it
/// advances once per block, so a stalled counter against a rising
/// `BLOCK_NUMBER` means the feature stopped firing.
pub const HEALTH_BEACON_BEATS_TOTAL: &str = "kardamom_executor_health_beacon_beats_total";

pub const RESYNC_TOTAL: &str = "kardamom_executor_resync_total";
// The invalid-tx-skip counter is emitted from inside the `no_std` exec core
// (`invalid_skip`), so the constant and its `record_` helper live there;
// re-exported here so the metric namespace stays browsable in one place.
pub use kardamom_exec_core::metrics::{INVALID_TX_SKIPPED_TOTAL, record_invalid_tx_skipped};

pub fn describe() {
    metrics::describe_counter!(TX_APPLIED_TOTAL, "tx executions, labelled by outcome");
    metrics::describe_histogram!(
        BLOCK_APPLY_DURATION_SECONDS,
        "wall time spent applying a block's tx batch"
    );
    metrics::describe_histogram!(
        STATE_COMMIT_DURATION_SECONDS,
        "wall time spent committing state to the backing DB"
    );
    metrics::describe_gauge!(BLOCK_NUMBER, "most recently committed block number");
    metrics::describe_gauge!(
        SEALER_BLOCK_NUMBER,
        "sealer's block number per its latest boundary, observed at cluster egress"
    );
    metrics::describe_counter!(
        SEALER_BOUNDARIES_TOTAL,
        "sealer block boundaries observed at cluster egress"
    );
    metrics::describe_counter!(
        INVALID_TX_SKIPPED_TOTAL,
        "deterministically-invalid canonical txs skipped with a marker receipt (#92); any nonzero value means an upstream guard failed"
    );
    metrics::describe_counter!(
        RESYNC_TOTAL,
        "full-resync fallbacks after a cluster replay-window overrun, by outcome"
    );
}
