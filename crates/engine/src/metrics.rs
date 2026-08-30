//! Metric name constants for the executor.
//!
//! Emission sites:
//!   - `actor.rs`: block-apply duration, state-commit duration, block-number
//!     gauge.
//!   - `executor.rs`: per-tx counter, from `execute_tx` and
//!     `execute_deposit_tx` return values, checked in `actor::spawn_exec`.

pub const TX_APPLIED_TOTAL: &str = "kardamom_executor_tx_applied_total";
pub const BLOCK_APPLY_DURATION_SECONDS: &str = "kardamom_executor_block_apply_duration_seconds";
pub const STATE_COMMIT_DURATION_SECONDS: &str = "kardamom_executor_state_commit_duration_seconds";
pub const BLOCK_NUMBER: &str = "kardamom_executor_block_number";

// The clustered sealer (the Java Aeron Cluster service) has no Prometheus
// endpoint. So the executor re-exports the sealer's output as it decodes
// cluster egress (`reader/cluster.rs`). Each in-order Boundary message bumps
// the counter and sets the gauge to the sealer's block number. These metrics
// measure the boundary stream at that executor's subscription. They do not
// measure JVM-internal state.
//
// The validator uses the same shared subscription, but it suppresses the
// emission (`suppress_sealer_metrics`). So only executor exporters publish
// these series. Probes can still take the `max()` across executor replicas;
// each replica re-exports its own view.
pub const SEALER_BLOCK_NUMBER: &str = "kardamom_sealer_block_number";
pub const SEALER_BOUNDARIES_TOTAL: &str = "kardamom_sealer_boundaries_emitted_total";

/// EIP-7928 BAL publication.
pub const BAL_FRAME_BYTES: &str = "kardamom_executor_bal_frame_bytes";
pub const BAL_ENCODE_SECONDS: &str = "kardamom_executor_bal_encode_seconds";
pub const BAL_PUBLISH_TOTAL: &str = "kardamom_executor_bal_publish_total";
pub const BAL_RETAINED_BLOCKS: &str = "kardamom_executor_bal_retained_blocks";

/// Footprint shadow. The `footprint-shadow`
/// thread (`crate::shadow`) emits this. It runs only for the executor role,
/// behind `KARDAMOM_FOOTPRINT_SHADOW=1`. The names match
/// `footprint_prediction_hit_rate` and `footprint_false_independent_total`
/// exactly, namespaced like every other executor series.
pub const FOOTPRINT_BLOCKS_TOTAL: &str = "kardamom_executor_footprint_blocks_total";
pub const FOOTPRINT_PREDICTION_HIT_RATE: &str = "kardamom_executor_footprint_prediction_hit_rate";
pub const FOOTPRINT_FALSE_INDEPENDENT_TOTAL: &str =
    "kardamom_executor_footprint_false_independent_total";
pub const FOOTPRINT_FALSE_EDGE_TOTAL: &str = "kardamom_executor_footprint_false_edge_total";
pub const FOOTPRINT_COLD_TX_TOTAL: &str = "kardamom_executor_footprint_cold_tx_total";
pub const FOOTPRINT_ACCUMULATOR_READ_TOTAL: &str =
    "kardamom_executor_footprint_accumulator_read_total";
pub const FOOTPRINT_PREDICTED_WAVES: &str = "kardamom_executor_footprint_predicted_waves";
pub const FOOTPRINT_PREDICTED_WIDTH: &str = "kardamom_executor_footprint_predicted_width";
pub const FOOTPRINT_PREDICTED_EDGES: &str = "kardamom_executor_footprint_predicted_edges";
pub const FOOTPRINT_PREDICTED_CP_RATIO: &str = "kardamom_executor_footprint_predicted_cp_ratio";
pub const FOOTPRINT_ORACLE_CP_RATIO: &str = "kardamom_executor_footprint_oracle_cp_ratio";

/// Health-beacon beats recorded: blocks closed with the health-check feature
/// active. This stays at 0 while the feature is off. Once active, it advances
/// once per block. If this counter stalls while `BLOCK_NUMBER` rises, the
/// feature has stopped firing.
pub const HEALTH_BEACON_BEATS_TOTAL: &str = "kardamom_executor_health_beacon_beats_total";

// Full-resync fallback, for a replay-window overrun. The executor binary
// bumps this when the cluster refuses REPLAY_FROM (`REPLAY_UNAVAILABLE`) and
// the node repairs itself with a peer checkpoint, or fails to repair itself.
// The label is `outcome=peer-checkpoint` or `outcome=unrecoverable`. This
// event is rare by design. Any non-zero rate is worth an alert. It means a
// node fell behind the retention window.
pub const RESYNC_TOTAL: &str = "kardamom_executor_resync_total";
// The invalid-tx-skip counter comes from inside the `no_std` exec core
// (`invalid_skip`). The constant and its `record_` helper live there. This
// re-export keeps the metric namespace browsable in one place.
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
    metrics::describe_counter!(
        FOOTPRINT_BLOCKS_TOTAL,
        "footprint-shadow blocks, labelled by outcome (graded|dropped)"
    );
    metrics::describe_gauge!(
        FOOTPRINT_PREDICTION_HIT_RATE,
        "per-block share of actual (non-excluded) cells the footprint predictor contained"
    );
    metrics::describe_counter!(
        FOOTPRINT_FALSE_INDEPENDENT_TOTAL,
        "true-conflicting tx pairs predicted independent — the dangerous miss class (would abort under STM)"
    );
    metrics::describe_counter!(
        FOOTPRINT_FALSE_EDGE_TOTAL,
        "predicted-conflicting tx pairs with no true conflict — over-merge, forfeited parallelism"
    );
    metrics::describe_counter!(
        FOOTPRINT_COLD_TX_TOTAL,
        "graded txs with an untrained selector (wildcard/Tail lane)"
    );
    metrics::describe_counter!(
        FOOTPRINT_ACCUMULATOR_READ_TOTAL,
        "txs whose execution took a pure BALANCE read of the fee sink (P2 Accumulator-guard trigger rate)"
    );
    metrics::describe_gauge!(
        FOOTPRINT_PREDICTED_WAVES,
        "per-block level count of the predicted dependency DAG"
    );
    metrics::describe_gauge!(
        FOOTPRINT_PREDICTED_WIDTH,
        "per-block widest level of the predicted dependency DAG"
    );
    metrics::describe_gauge!(
        FOOTPRINT_PREDICTED_EDGES,
        "per-block predicted direct-conflict pair count"
    );
    metrics::describe_gauge!(
        FOOTPRINT_PREDICTED_CP_RATIO,
        "per-block gas / predicted critical-path gas — the schedule's speedup bound"
    );
    metrics::describe_gauge!(
        FOOTPRINT_ORACLE_CP_RATIO,
        "per-block gas / true critical-path gas — the bound no predictor beats"
    );
}
