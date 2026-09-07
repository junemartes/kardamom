//! The exec thread's loop state: the `ExecState` struct and its constructor.

use std::collections::VecDeque;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};

use kardamom_types::{BlockBoundary, SnapshotSource};

use crate::delta::PendingDelta;
use crate::exec_types::TxIndex;
use crate::reader::{EpochObserver, ReaderToExec, RemoteEpochObserver};

use super::ports::{StateWriterQueue, StateWriterSignal};
use super::types::{
    BalHandoff, BlockExec, BufferedRecord, ExecToCommit, ExecutorConfig, ResumePoint,
};

/// The optional role-specific hooks `ExecState` carries: BAL capture,
/// footprint-shadow capture, a whole-block execution strategy, and the two
/// epoch observers. Grouped so `ExecInputs` and `spawn_exec` pass one value
/// instead of five loose parameters.
pub(crate) struct ExecHooks<Db, E> {
    pub(super) bal_tx: Option<Sender<BalHandoff>>,
    pub(super) shadow_tx: Option<Sender<crate::shadow::ShadowBlock>>,
    pub(super) block_exec: Option<BlockExec<Db>>,
    pub(super) epoch_observer: Option<E>,
    pub(super) remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>,
}

/// Every input [`ExecState::new`] and `spawn_exec` need: the config, the
/// two channels, the three storage ports, the resume cursor, and the
/// optional hooks. `Executor::run` builds one of these from the grouped
/// `Outbound`/`RoleHooks` it already destructures, instead of forwarding
/// twelve positional arguments.
pub(crate) struct ExecInputs<S: SnapshotSource, Q, P, E> {
    pub(super) cfg: ExecutorConfig,
    pub(super) rx: Receiver<ReaderToExec>,
    pub(super) tx: Sender<ExecToCommit>,
    pub(super) snapshots: S,
    pub(super) sw_signal: Q,
    pub(super) sw_queue: P,
    pub(super) start: ResumePoint,
    pub(super) hooks: ExecHooks<S::Db, E>,
}

/// The exec thread's mutable loop state. Each `spawn_exec` thread has one
/// instance. Each `ReaderToExec` arm is one method, split across
/// `exec_records.rs`, `exec_markers.rs`, and `exec_boundary.rs`.
pub(super) struct ExecState<S: SnapshotSource, Q, P, E> {
    pub(super) cfg: ExecutorConfig,
    pub(super) rx: Receiver<ReaderToExec>,
    pub(super) tx: Sender<ExecToCommit>,
    pub(super) snapshots: S,
    pub(super) sw_signal: Q,
    pub(super) sw_queue: P,
    pub(super) bal_tx: Option<Sender<BalHandoff>>,
    /// Footprint shadow handoff (`crate::shadow`), one per block. Only the
    /// executor role uses this; it is `None` elsewhere. The whole-block
    /// (validator) path ignores it: captures use the streaming arm instead.
    pub(super) shadow_tx: Option<Sender<crate::shadow::ShadowBlock>>,
    /// Shadow tx captures and the serial-lane count for the current block.
    /// The code hands these off with `try_send` at each boundary; this never
    /// blocks. Both stay empty when the shadow is off.
    pub(super) shadow_captures: Vec<crate::shadow::ShadowTxCapture>,
    pub(super) shadow_serial: u32,
    pub(super) block_exec: Option<BlockExec<S::Db>>,
    /// A role-specific epoch check, statically dispatched. See
    /// [`crate::reader::EpochObserver`]. `None` means the code trusts the
    /// ordered stream.
    pub(super) epoch_observer: Option<E>,
    /// The interop mirror of `epoch_observer`, invoked per `RemoteEpoch`
    /// marker. `None` everywhere until the destination-validator
    /// `RemoteEpochVerifier` lands.
    pub(super) remote_epoch_observer: Option<Box<dyn RemoteEpochObserver>>,
    /// The snapshot source returns owned snapshots keyed by block number:
    /// the block just committed. This is the [`ResumePoint`]'s `block` field
    /// (0 on a fresh start).
    pub(super) snapshot: S::Db,
    pub(super) delta: PendingDelta,
    /// EIP-7928 capture: the per-block Bal. The code resets it at each
    /// boundary. It is maintained only when a publisher is attached
    /// (executor role).
    pub(super) block_bal: revm::state::bal::Bal,
    /// Whole-block buffer. Used only when a block-exec strategy is
    /// supplied (the validator parallel path).
    pub(super) buffered: Vec<BufferedRecord>,
    /// Per-block execution scope (streaming path): one EVM and one
    /// commit-into cache for the whole block. Building these per tx used
    /// about 90% of the allocation in the execution path.
    ///
    /// The code drops the scope at each boundary. It rebuilds the scope
    /// lazily, at the block's first tx. The rebuild seeds it with the
    /// parent and anything already in the live delta, for example deposits
    /// that landed before the first tx.
    pub(super) scope: Option<crate::executor::Executor<S::Db>>,
    /// Pipelined commit, at depth K. At each boundary, the code submits the
    /// finalized delta to the writer, but does not wait for it. The next
    /// block executes against the snapshot, then the merged unsettled layer,
    /// then the delta. Completed commits settle opportunistically, through a
    /// non-blocking probe at each boundary. The exec thread parks only when
    /// the writer is a full K blocks behind.
    ///
    /// A single slow fsync does not touch execution at all: a blocking
    /// `wait_committed` call runs only when the pipeline is at full depth.
    ///
    /// Durability semantics do not change. A boundary reaches `tx_receipts`
    /// only after its block is durable. Receipts stream out before
    /// durability, at least once: a crash replay re-publishes them, and
    /// they are byte-identical.
    ///
    /// `parent` is the merged union of every unsettled block's writes. A
    /// later block's write wins over an earlier one. This gives one layer
    /// regardless of depth, so per-tx cache seeding stays O(one map). The
    /// code rebuilds `parent` from the survivors when commits settle.
    pub(super) parent: Option<PendingDelta>,
    pub(super) inflight: VecDeque<(BlockBoundary, PendingDelta)>,
    /// Per-block receipts, in arrival order. The code drains this into the
    /// `BlockDelta` at each boundary, so the writer can persist the receipts
    /// and the `tx_hash_index` tables. Each tx's receipt is cloned once, to
    /// feed both this list and the streaming `tx_receipts` publisher. This
    /// clone cost is flagged for saturation validation.
    pub(super) block_receipts: Vec<kardamom_types::Receipt>,
    /// Block-number bookkeeping. Blocks are 1-indexed; genesis is block 0.
    /// The exec thread assumes every block boundary it sees is for the
    /// current in-flight block. It does not re-derive block numbers on its
    /// own; it relies on the sealer.
    pub(super) current_block: u64,
    /// Block N's txs execute with boundary N-1's timestamp. On resume, the
    /// code already consumed that boundary before the restart. So its
    /// persisted value seeds the state. See [`ResumePoint::l2_timestamp`].
    pub(super) current_l2_ts: u64,
    /// Per-block RPC enrichment counters. The code resets these at each
    /// `BoundaryStart`.
    pub(super) tx_index_in_block: u64,
    pub(super) cumulative_gas_used: u64,
    /// The cumulative count of canonical records (`TxRef` and `DepositRef`)
    /// this exec thread has folded into a receipt.
    ///
    /// This is the boundary alignment key. `BlockBoundaryStart.end_tx_idx`
    /// carries the sealer's cumulative count of republished canonical
    /// records, encoded through `BPosition::from_index`. At each boundary,
    /// the two counts must match. `expected_tx_idx` already tracks this
    /// count: it advances once per applied Tx or Deposit, and never resets
    /// across blocks. So the code compares against it directly.
    ///
    /// The key is a count, not an Aeron byte position. A byte position is
    /// per-publication under the canonical-publisher MDC merge, and is
    /// ambiguous between an offer-return frame and a frame-start frame. The
    /// count stays correct across MDC merges; a byte position does not.
    ///
    /// The counter is seeded at the resume cursor. Boundary counts on the
    /// wire are absolute, and delivery resumes at the cursor, so the
    /// counter must start there too.
    pub(super) expected_tx_idx: TxIndex,
    /// Wall time spent executing the block's txs and deposits. This
    /// excludes channel idle time between txs. The `BoundaryStart` handler
    /// records this value when it closes the block. It is `None` for empty
    /// blocks.
    pub(super) block_apply_elapsed: Option<Duration>,
    /// Pre-resolved counter handles. This loop is the executor's hottest
    /// path, so the code skips the per-event registry lookup.
    pub(super) tx_applied_ok: metrics::Counter,
    pub(super) tx_applied_error: metrics::Counter,
}

impl<S, Q, P, E> ExecState<S, Q, P, E>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
    E: EpochObserver + 'static,
{
    /// Seed the loop state from the [`ResumePoint`] cursor.
    ///
    /// The canonical stream source delivers records from the persisted
    /// cursor onward (the cluster client's `REPLAY_FROM`; `reader::cluster`
    /// dedups any records below the cursor). So the exec thread seeds its
    /// absolute counters here, instead of replaying from record 0 and
    /// counting through them. A fresh start uses [`ResumePoint::GENESIS`],
    /// the same seeding with all-zero values.
    pub(super) fn new(inputs: ExecInputs<S, Q, P, E>) -> Self {
        let ExecInputs {
            cfg,
            rx,
            tx,
            snapshots,
            sw_signal,
            sw_queue,
            start,
            hooks:
                ExecHooks {
                    bal_tx,
                    shadow_tx,
                    block_exec,
                    epoch_observer,
                    remote_epoch_observer,
                },
        } = inputs;
        let snapshot = snapshots.snapshot_after(start.block);
        Self {
            cfg,
            rx,
            tx,
            snapshots,
            sw_signal,
            sw_queue,
            bal_tx,
            shadow_tx,
            shadow_captures: Vec::new(),
            shadow_serial: 0,
            block_exec,
            epoch_observer,
            remote_epoch_observer,
            snapshot,
            delta: PendingDelta::new(),
            block_bal: revm::state::bal::Bal::new(),
            buffered: Vec::new(),
            scope: None,
            parent: None,
            inflight: VecDeque::new(),
            block_receipts: Vec::new(),
            // `start.block` is read from the persisted resume cursor; a
            // corrupt value near `u64::MAX` must not wrap to block 0 and
            // silently re-execute the chain from genesis.
            current_block: start.block.saturating_add(1),
            current_l2_ts: start.l2_timestamp,
            tx_index_in_block: 0,
            cumulative_gas_used: 0,
            expected_tx_idx: TxIndex(start.record_count),
            block_apply_elapsed: None,
            tx_applied_ok: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "ok"),
            tx_applied_error: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "error"),
        }
    }
}
