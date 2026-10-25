//! Metric name constants for the executor.

pub const TX_APPLIED_TOTAL: &str = "kardamom_executor_tx_applied_total";
/// The canonical frames that the reader did not forward to the egress:
/// the queue to the egress publisher was full.
pub const TEE_DROPPED_TOTAL: &str = "kardamom_executor_egress_frames_dropped_total";
pub const BLOCK_APPLY_DURATION_SECONDS: &str = "kardamom_executor_block_apply_duration_seconds";
pub const STATE_COMMIT_DURATION_SECONDS: &str = "kardamom_executor_state_commit_duration_seconds";
pub const BLOCK_NUMBER: &str = "kardamom_executor_block_number";

// The clustered sealer (the Java Aeron Cluster service) has no Prometheus
// endpoint. So the executor re-exports the sealer's output as it reads the
// canonical stream (`reader::cluster::Reorder`). Each in-order Boundary
// message bumps the counter and sets the gauge to the sealer's block number.
// The end of each replay sets the gauge to the head that the replay reached,
// so a reader at the head exports the head before the next boundary. The
// gauge does not move down. These metrics measure the stream at that
// executor's subscription. They do not measure JVM-internal state.
//
// The validator and the batcher read the same stream, but they suppress the
// emission (`suppress_sealer_metrics`), on the cluster egress and on the
// egress of the executors. So only executor exporters publish these series.
// Probes can still take the `max()` across executor replicas; each replica
// re-exports its own view.
pub const SEALER_BLOCK_NUMBER: &str = "kardamom_sealer_block_number";
pub const SEALER_BOUNDARIES_TOTAL: &str = "kardamom_sealer_boundaries_emitted_total";
/// The sealer's block number as the reader of the validator, or of the
/// batcher, sees it: by the same rules as `SEALER_BLOCK_NUMBER`. The name
/// differs from `SEALER_BLOCK_NUMBER` on purpose: the executor's series is
/// the documented observation point, and a second copy under that name
/// breaks every `sum()` query over it. The validator's readiness rule
/// compares this head with `validator_committed_block`.
pub const VALIDATOR_SEALER_BLOCK_NUMBER: &str = "validator_sealer_block_number";

/// The source of the canonical stream with its bytes: 1 for the source that
/// the executor reads, 0 for the other. The label is `source="feed"` or
/// `source="service"`.
pub const INPUT_SOURCE: &str = "kardamom_executor_input_source";
/// Canonical entries whose raw transaction does not hash to the canonical
/// id, by the `source` that sent them. The executor does not execute such an
/// entry: it reads the entry again from the other source.
pub const SOURCE_MISMATCH_TOTAL: &str = "kardamom_executor_source_mismatch_total";

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
pub(crate) const HEALTH_BEACON_BEATS_TOTAL: &str = "kardamom_executor_health_beacon_beats_total";

/// Live queue depth of one `tx_data` subscription's tokio channel. The Aeron
/// poll task feeds this channel and cannot block, so the channel is
/// unbounded (see [`crate::bin_support::LiveTxDataSub`]): this gauge is the
/// operator's signal that the engine reader has stalled and the channel is
/// growing without limit.
pub(crate) const TX_DATA_QUEUE_DEPTH: &str = "kardamom_executor_tx_data_queue_depth";

// Full-resync fallback, for a replay-window overrun. The executor binary
// bumps this when the cluster refuses REPLAY_FROM (`REPLAY_UNAVAILABLE`) and
// the node repairs itself with a peer checkpoint, or fails to repair itself.
// The label is `outcome=peer-checkpoint` or `outcome=unrecoverable`. This
// event is rare by design. Any non-zero rate is worth an alert. It means a
// node fell behind the retention window.
pub const RESYNC_TOTAL: &str = "kardamom_executor_resync_total";

// The automatic rollback. The executor binary bumps this when the canonical
// stream diverges from its committed history: the cluster continued from a
// shorter log after a power loss on a majority of the members. The label is
// `outcome=local-checkpoint`, `outcome=peer-checkpoint`, or
// `outcome=no-checkpoint`. The last one stops the executor for the operator.
pub const ROLLBACK_TOTAL: &str = "kardamom_executor_rollback_total";

/// Whether the must-deliver publication of `topic` has a connected
/// subscriber: 1 while its publishes land, 0 while they fail with
/// `NOT_CONNECTED`. The commit thread writes it on each change.
pub const PUBLICATION_CONNECTED: &str = "kardamom_publication_connected";
/// How long the current unconnected period of `topic` has lasted, in
/// seconds. 0 while the publication is connected. The escalation reopens
/// the publication after one stall budget and exits after four.
pub const PUBLICATION_NOT_CONNECTED_SECONDS: &str = "kardamom_publication_not_connected_seconds";
/// Joins whose first in-band wait ended without the envelope: the
/// `tx_data` lane fell behind by a whole wait slice. The join then
/// refetches from an archive, or ends.
pub(crate) const JOIN_MISSES_TOTAL: &str = "kardamom_executor_join_misses_total";
/// Archive refetch attempts of a join, by `outcome`: `ok`, `absent` (the
/// archive does not hold the range), or `error`.
pub(crate) const ARCHIVE_REFETCH_TOTAL: &str = "kardamom_executor_archive_refetch_total";
/// Envelopes that archive refetches put into the join buffer.
pub(crate) const ARCHIVE_REFETCH_RECOVERED_TOTAL: &str =
    "kardamom_executor_archive_refetch_recovered_total";
/// Entries that every archive refused, for which this voter asked the
/// sealer for a void.
pub(crate) const VOID_VOTES_TOTAL: &str = "kardamom_executor_void_votes_total";
/// The gas of every sealed block. Its rate is the gas per second.
pub(crate) const GAS_USED_TOTAL: &str = "kardamom_executor_gas_used_total";
/// Failed must-deliver attempts on `tx_receipts`. Each one pauses the
/// commit thread before the next attempt.
pub(crate) const RECEIPT_PUBLISH_RETRIES_TOTAL: &str =
    "kardamom_executor_receipt_publish_retries_total";
/// Blocks whose BAL the exec thread dropped, because the queue to the BAL
/// publisher was full.
pub(crate) const BAL_HANDOFF_DROPPED_TOTAL: &str = "kardamom_executor_bal_handoff_dropped_total";

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
        "the exec thread's wait for a free commit slot, when the commit pipeline is full"
    );
    metrics::describe_gauge!(BLOCK_NUMBER, "most recently committed block number");
    metrics::describe_counter!(
        HEALTH_BEACON_BEATS_TOTAL,
        "blocks closed with the health-check feature active"
    );
    metrics::describe_gauge!(
        TX_DATA_QUEUE_DEPTH,
        "live queue depth of one tx_data subscription's unbounded channel"
    );
    metrics::describe_gauge!(
        SEALER_BLOCK_NUMBER,
        "sealer's block number per its latest boundary or replay end, observed by the executor's reader"
    );
    metrics::describe_counter!(
        SEALER_BOUNDARIES_TOTAL,
        "sealer block boundaries observed at cluster egress"
    );
    metrics::describe_gauge!(
        VALIDATOR_SEALER_BLOCK_NUMBER,
        "sealer's block number per its latest boundary or replay end, as the validator's reader sees it"
    );
    metrics::describe_counter!(
        INVALID_TX_SKIPPED_TOTAL,
        "deterministically-invalid canonical txs skipped with a marker receipt (#92); any nonzero value means an upstream guard failed"
    );
    metrics::describe_gauge!(
        INPUT_SOURCE,
        "1 for the source of the canonical stream that the executor reads (feed|service)"
    );
    metrics::describe_counter!(
        SOURCE_MISMATCH_TOTAL,
        "canonical entries whose raw transaction does not hash to the canonical id, by source"
    );
    metrics::describe_counter!(
        RESYNC_TOTAL,
        "full-resync fallbacks after a cluster replay-window overrun, by outcome"
    );
    metrics::describe_counter!(
        ROLLBACK_TOTAL,
        "rollbacks to a checkpoint below a divergence of the canonical stream, by outcome"
    );
    metrics::describe_gauge!(
        PUBLICATION_CONNECTED,
        "1 while the must-deliver publication of the topic has a connected subscriber"
    );
    metrics::describe_gauge!(
        PUBLICATION_NOT_CONNECTED_SECONDS,
        "seconds the current unconnected period of the topic has lasted; 0 while connected"
    );
    describe_footprint();
    describe_pipeline();
}

/// The descriptions of the counters that locate a bottleneck on the
/// executor path.
fn describe_pipeline() {
    metrics::describe_counter!(
        JOIN_MISSES_TOTAL,
        "joins whose first in-band wait ended without the tx_data envelope"
    );
    metrics::describe_counter!(
        ARCHIVE_REFETCH_TOTAL,
        "archive refetch attempts of a join, by outcome (ok|absent|error)"
    );
    metrics::describe_counter!(
        ARCHIVE_REFETCH_RECOVERED_TOTAL,
        "envelopes that archive refetches put into the join buffer"
    );
    metrics::describe_counter!(
        VOID_VOTES_TOTAL,
        "entries that every archive refused, for which this voter asked for a void"
    );
    metrics::describe_counter!(GAS_USED_TOTAL, "the gas of every sealed block");
    metrics::describe_counter!(
        RECEIPT_PUBLISH_RETRIES_TOTAL,
        "failed must-deliver attempts on tx_receipts"
    );
    metrics::describe_counter!(
        BAL_HANDOFF_DROPPED_TOTAL,
        "blocks whose BAL the exec thread dropped: the queue to the BAL publisher was full"
    );
}

/// The descriptions of the footprint-shadow series.
fn describe_footprint() {
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
