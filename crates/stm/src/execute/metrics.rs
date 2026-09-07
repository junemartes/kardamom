use crate::mv::ReadRecord;
use alloy_primitives::U256;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::delta::WriteSet;
use kardamom_types::Receipt;
use std::sync::atomic::AtomicU32;

/// Feed-side per-phase nanoseconds and counts, one struct instead of
/// eight repeated fields: `BlockSession` accumulates it per
/// transaction, `TailJob` carries it across the thread hand-off, and
/// `TailStats` hands it to `block_tail` for the final report.
#[derive(Default, Clone, Copy)]
pub(super) struct FeedTimings {
    pub(super) admit_ns: u64,
    pub(super) feed_pre_ns: u64,
    pub(super) feed_dag_ns: u64,
    pub(super) decode_ns: u64,
    pub(super) predict_ns: u64,
    pub(super) redundant_edges: u64,
    pub(super) fifo_covered: u64,
    pub(super) feed_ns: u64,
}

/// One executed transaction's artifacts, before commit. The canonical-order
/// commit pass adds cumulative gas and the accumulator write-set-hash fixup.
pub(super) struct TxResult {
    pub(super) receipt: Receipt,
    pub(super) ws: WriteSet,
    pub(super) reads: Vec<ReadRecord>,
    /// EIP-7928 capture fragment: this transaction's BAL updates at its
    /// block-global index (`bal_base + local_idx + 1`), recorded through
    /// the same `Bal::update_account` the streaming path uses. `None`
    /// when capture is off, or the transaction was an invalid skip (a
    /// skip carries no fragment in either mode). The commit pass rewrites
    /// the fee-sink balance write to the computed prefix value (workers
    /// see the block-start sink, not the canonical running sum) and
    /// folds the fragments in canonical order.
    pub(super) bal_frag: Option<revm::state::bal::Bal>,
    /// This transaction's exact credit to the fee sink (the value after,
    /// minus the block-start value seen).
    pub(super) fee_delta: U256,
    /// The write set contains the fee sink, so its hash is finalized at
    /// commit, after the prefix balance is computed. Hashing it during
    /// execution would be wasted work. Offline measurement found this
    /// case at nearly every transaction, so the saved keccak is not an
    /// edge case.
    pub(super) sink_touched: bool,
}

/// Outcome of one block through the STM engine.
#[derive(Default)]
pub struct StmOutcome {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
    /// EIP-7928 capture: the block's per-transaction fragments folded in
    /// canonical order (see `merge_bal_fragments`), with the fee-sink
    /// writes computed to the canonical prefix and wounded transactions'
    /// fragments replaced by their repair capture. `Some` only when the
    /// session was opened with capture (`begin_block_layered_bal`).
    /// Wire-identical to the streaming path's sequential capture by
    /// construction.
    pub bal: Option<revm::state::bal::Bal>,
    /// Transactions wounded at validation: a conflict the marks missed,
    /// repaired by re-executing at the canonical position (per
    /// transaction; the whole block never re-runs). Zero on every
    /// measured workload so far.
    pub wounds: usize,
    /// Whether any wound fired (spec invariant #3's counter, per block).
    pub fallback: bool,
    /// The pool declined this block and ran it sequentially, because the
    /// work per transaction was too small for parallel execution to pay
    /// for its own coordination. See `PARALLEL_WORTH_NS`.
    pub declined: bool,
    /// Mean per-transaction execution time this block taught the pool:
    /// the input to the next block's decline decision. Non-zero after a
    /// declined block too. That is what keeps the gate from being a trap
    /// door.
    pub learned_tx_ns: u64,
    /// Account writes whose domain belongs to another worker.
    pub writes_own: u64,
    pub writes_foreign: u64,
    /// Chain links ordered by FIFO position instead of a DAG edge, and
    /// how often a taken transaction had to wait on a stolen FIFO
    /// predecessor.
    pub fifo_covered: u64,
    pub fifo_stalls: u64,
    pub read_us: u64,
    /// Per-worker busy microseconds. See `Metrics::busy_per_worker`.
    pub busy_per_worker_us: Vec<u64>,
    /// ⊤ (cold, untrained-selector) transactions. They wait out the
    /// prefix.
    pub cold: usize,
    /// Live-DAG edges created across the block (only against predecessors
    /// that were still outstanding at admission).
    pub edges: usize,
    /// Transactions dispatched per worker queue: the domain-affinity
    /// histogram.
    pub dispatch: Vec<u32>,
    /// Nodes observed leaving the graph more than once. Always zero.
    /// Asserted by the test suite and worth an alert in production: a
    /// non-zero value means a transaction completed twice and the edges
    /// registered in between were stranded.
    pub double_exit: u32,
    /// Scheduler cost, measured (the numbers the prune-batch knob is
    /// tuned on): time held in the graph lock split by cause, prune
    /// invocations and how many were starvation-forced, the realized
    /// batch size, and worker idle time.
    pub feed_us: u64,
    pub redundant_edges: u64,
    pub steals: u64,
    pub reads_total: u64,
    pub reads_mv_hit: u64,
    pub reads_base_hit: u64,
    pub reads_backend: u64,
    pub evm_us: u64,
    pub publish_us: u64,
    /// Where the block's wall time went. `busy_us / (workers *
    /// parallel_span_us)` is the honest core utilization. `ramp_us` and
    /// `commit_us` are the serial head and tail that no worker count
    /// reduces.
    pub busy_us: u64,
    pub parallel_span_us: u64,
    pub ramp_us: u64,
    pub commit_us: u64,
    pub commit_hash_us: u64,
    pub commit_delta_us: u64,
    pub decode_us: u64,
    pub predict_us: u64,
    pub admit_us: u64,
    pub commit_fold_us: u64,
    pub commit_lane_us: u64,
    pub feed_pre_us: u64,
    pub feed_dag_us: u64,
    pub prune_us: u64,
    pub prune_calls: u64,
    pub prune_forced: u64,
    pub avg_batch: f64,
    pub idle_us: u64,
}

/// Engine instrumentation: the numbers the prune-batch decision is made
/// on. Health is judged on the pair of error rates plus realized
/// utilization; the same discipline applies to the scheduler's own
/// cost. One completion counter per worker, each on its own cache line.
#[derive(Default)]
#[repr(align(64))]
pub(crate) struct PaddedLen(pub(crate) AtomicU32);

/// One u64 counter per cache line (see [`PaddedLen`]).
#[derive(Default)]
#[repr(align(64))]
pub(crate) struct PaddedLen64(pub(crate) std::sync::atomic::AtomicU64);

#[derive(Default)]
pub(crate) struct Metrics {
    pub(super) commit_lane_ns: std::sync::atomic::AtomicU64,
    pub(super) prune_ns: std::sync::atomic::AtomicU64,
    /// Prune invocations and how many were starvation-forced (a worker
    /// had nothing to run and had to apply pending completions itself).
    pub(super) prune_calls: std::sync::atomic::AtomicU64,
    pub(super) prune_forced: std::sync::atomic::AtomicU64,
    /// Completions applied by prunes (divided by `prune_calls` gives the
    /// realized batch size).
    pub(super) completions: std::sync::atomic::AtomicU64,
    /// Nanoseconds workers spent parked with nothing to run.
    pub(super) idle_ns: std::sync::atomic::AtomicU64,
    /// Ready transactions taken from another thread's queue to fix
    /// imbalance.
    pub(super) steals: std::sync::atomic::AtomicU64,
    /// Read-path breakdown. The multi-version path costs 1.7x sequential
    /// single-threaded, so which lookup dominates decides what to fix.
    pub(super) reads_total: std::sync::atomic::AtomicU64,
    /// Reads served by a version written earlier in this block.
    pub(super) reads_mv_hit: std::sync::atomic::AtomicU64,
    /// Reads that fell through to the shared base cache, and of those,
    /// the ones that had to touch the backing store.
    pub(super) reads_base_hit: std::sync::atomic::AtomicU64,
    pub(super) reads_backend: std::sync::atomic::AtomicU64,
    /// Split of a worker's per-transaction time: inside revm (`transact`,
    /// which includes the read path) versus publishing the write set
    /// into the multi-version cache.
    pub(super) evm_ns: std::sync::atomic::AtomicU64,
    pub(super) publish_ns: std::sync::atomic::AtomicU64,
    /// Nanoseconds workers spent inside revm, the only work that is
    /// actually the point. `busy / (workers x parallel_span)` is the true
    /// core utilization. Idle time alone cannot tell "the DAG had no work
    /// to give" apart from "work existed and nobody picked it up".
    pub(super) busy_ns: std::sync::atomic::AtomicU64,
    /// Nanoseconds of the first dispatch and last completion, as offsets
    /// from the session start (interior mutability so workers can stamp
    /// them).
    pub(super) first_dispatch_ns: std::sync::atomic::AtomicU64,
    pub(super) last_done_ns: std::sync::atomic::AtomicU64,
    /// Account writes published, split by whether the account's domain
    /// belongs to the publishing worker. A foreign write is one that two
    /// or more workers can perform on the same account: the true sharing
    /// that no lock granularity removes.
    pub(super) writes_own: std::sync::atomic::AtomicU64,
    pub(super) writes_foreign: std::sync::atomic::AtomicU64,
    pub(super) fifo_stalls: std::sync::atomic::AtomicU64,
    /// Nanoseconds inside `MvView`'s read path, carved out of `evm_ns`.
    pub(super) read_ns: std::sync::atomic::AtomicU64,
    /// Per-worker busy nanoseconds: the straggler detector. Dispatch can
    /// be perfectly balanced and idle time still high if cores run at
    /// different speeds (this box's bimodal memory state is per-thread).
    /// The histogram shows it directly.
    pub(super) busy_per_worker: Vec<PaddedLen64>,
}
