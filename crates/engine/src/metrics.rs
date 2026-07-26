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
pub const RESYNC_TOTAL: &str = "kardamom_executor_resync_total";

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
        RESYNC_TOTAL,
        "full-resync fallbacks after a cluster replay-window overrun, by outcome"
    );
}
