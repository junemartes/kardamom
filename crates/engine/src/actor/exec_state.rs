//! The exec thread's loop state: the `ExecState` struct and its constructor.

use std::collections::VecDeque;

use crossbeam_channel::{Receiver, Sender};

use kardamom_types::{BlockBoundary, BlockFees, SnapshotSource};

use crate::delta::PendingDelta;
use crate::exec_types::TxIndex;
use crate::reader::ReaderToExec;

use super::exec_block::{BalCapture, BlockRun, BlockState};
use super::types::{BalHandoff, ExecToCommit, ExecutorConfig, ResumePoint};
use super::wiring::{ExecPorts, SnapshotDb};

/// The optional role-specific hooks `ExecState` takes: BAL capture,
/// footprint-shadow capture, a whole-block execution strategy, and the two
/// epoch observers. Grouped so `ExecInputs` and `ExecState::spawn` pass one value
/// instead of five loose parameters. [`ExecState::new`] moves the first three
/// into the [`BlockState`] and the observers into [`ExecObservers`].
pub(crate) struct ExecHooks<W: ExecPorts> {
    pub(super) bal_tx: Option<Sender<BalHandoff>>,
    /// Footprint shadow handoff (`crate::shadow`), one per block. Only the
    /// executor role uses this; it is `None` elsewhere. The whole-block
    /// path drops it: captures use the streaming arm instead.
    pub(super) shadow_tx: Option<Sender<crate::shadow::ShadowBlock>>,
    pub(super) block_exec: Option<W::BlockExec>,
    /// A role-specific epoch check, statically dispatched. See
    /// [`crate::reader::EpochObserver`]. `None` means the code trusts the
    /// ordered stream.
    pub(super) epoch_observer: Option<W::Epoch>,
    /// The interop mirror of `epoch_observer`, invoked per `RemoteEpoch`
    /// marker. `None` everywhere until the destination-validator
    /// `RemoteEpochVerifier` lands.
    pub(super) remote_epoch_observer: Option<W::RemoteEpoch>,
}

/// Every input [`ExecState::new`] and [`ExecState::spawn`] need: the config, the
/// two channels, the three storage ports, the resume cursor, and the
/// optional hooks. `Executor::run` builds one of these from the grouped
/// `Outbound`/`RoleHooks` it already destructures, instead of forwarding
/// twelve positional arguments.
pub(crate) struct ExecInputs<W: ExecPorts> {
    pub(super) cfg: ExecutorConfig,
    pub(super) rx: Receiver<ReaderToExec>,
    pub(super) tx: Sender<ExecToCommit>,
    pub(super) snapshots: W::Snapshots,
    pub(super) sw_signal: W::WriterSignal,
    pub(super) sw_queue: W::WriterQueue,
    pub(super) start: ResumePoint,
    pub(super) hooks: ExecHooks<W>,
}

/// The exec thread's channels and storage ports: the reader channel in,
/// the commit channel out, and the three writer-side seams.
pub(super) struct ExecIo<W: ExecPorts> {
    pub(super) rx: Receiver<ReaderToExec>,
    pub(super) tx: Sender<ExecToCommit>,
    pub(super) snapshots: W::Snapshots,
    pub(super) sw_signal: W::WriterSignal,
    pub(super) sw_queue: W::WriterQueue,
}

/// The role's epoch checks, which the marker arms run. `None` trusts the
/// ordered stream.
pub(super) struct ExecObservers<W: ExecPorts> {
    pub(super) epoch_observer: Option<W::Epoch>,
    pub(super) remote_epoch_observer: Option<W::RemoteEpoch>,
}

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
pub(super) struct CommitPipeline<W: ExecPorts> {
    /// The snapshot source returns owned snapshots keyed by block number:
    /// the block just committed. This is the [`ResumePoint`]'s `block` field
    /// (0 on a fresh start).
    pub(super) snapshot: SnapshotDb<W>,
    /// The merged union of every unsettled block's writes. A later block's
    /// write wins over an earlier one. This gives one layer regardless of
    /// depth, so per-tx cache seeding stays O(one map). The code rebuilds
    /// `parent` from the survivors when commits settle.
    pub(super) parent: Option<PendingDelta>,
    pub(super) inflight: VecDeque<(BlockBoundary, PendingDelta)>,
}

/// Where the exec thread is in the chain. Seeded from the [`ResumePoint`]
/// and advanced by the records and boundaries it consumes.
pub(super) struct Cursor {
    /// Block-number bookkeeping. Blocks are 1-indexed; genesis is block 0.
    /// The exec thread assumes every block boundary it sees is for the
    /// current in-flight block. It does not re-derive block numbers on its
    /// own; it relies on the sealer.
    pub(super) block: u64,
    /// Block N's txs execute with boundary N-1's timestamp. On resume, the
    /// code already consumed that boundary before the restart. So its
    /// persisted value seeds the state. See [`ResumePoint::l2_timestamp`].
    pub(super) l2_ts: u64,
    /// The fees of the block in flight: its base fee and beneficiary.
    /// Advanced at each boundary from the block's gas used, and seeded
    /// from the resume cursor and the chain's schedule.
    pub(super) fees: BlockFees,
    /// The next absolute record index, which is also the cumulative count
    /// of canonical records this exec thread has consumed.
    ///
    /// This is the boundary alignment key. `BlockBoundaryStart.end_tx_idx`
    /// carries the sealer's cumulative count of republished canonical
    /// records, encoded through `BPosition::from_index`. At each boundary,
    /// the two counts must match. `next_tx_idx` advances once per record
    /// in arrival order, and never resets across blocks. So the code
    /// compares against it directly.
    ///
    /// The key is a count, not an Aeron byte position. A byte position is
    /// per-publication under the canonical-publisher MDC merge, and is
    /// ambiguous between an offer-return frame and a frame-start frame. The
    /// count stays correct across MDC merges; a byte position does not.
    ///
    /// The counter is seeded at the resume cursor. Boundary counts on the
    /// wire are absolute, and delivery resumes at the cursor, so the
    /// counter must start there too.
    pub(super) next_tx_idx: TxIndex,
}

/// Pre-resolved counter handles. The exec loop is the executor's hottest
/// path, so the code skips the per-event registry lookup.
pub(super) struct ExecMetrics {
    pub(super) applied_ok: metrics::Counter,
    pub(super) applied_error: metrics::Counter,
}

/// The exec thread's mutable loop state. Each [`ExecState::spawn`] thread has one
/// instance. Each `ReaderToExec` arm is one method, split across
/// `exec_records.rs`, `exec_markers.rs`, and `exec_boundary.rs`.
pub(crate) struct ExecState<W: ExecPorts> {
    pub(super) cfg: ExecutorConfig,
    pub(super) io: ExecIo<W>,
    pub(super) observers: ExecObservers<W>,
    pub(super) block: BlockState<W>,
    pub(super) commits: CommitPipeline<W>,
    pub(super) cursor: Cursor,
    pub(super) metrics: ExecMetrics,
}

impl<W: ExecPorts> ExecState<W> {
    /// Seed the loop state from the [`ResumePoint`] cursor.
    ///
    /// The canonical stream source delivers records from the persisted
    /// cursor onward (the cluster client's `REPLAY_FROM`; `reader::cluster`
    /// dedups any records below the cursor). So the exec thread seeds its
    /// absolute counters here, instead of replaying from record 0 and
    /// counting through them. A fresh start uses [`ResumePoint::GENESIS`],
    /// the same seeding with all-zero values.
    pub(super) fn new(inputs: ExecInputs<W>) -> Self {
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
        let fees = start.block_fees(cfg.fees);
        Self {
            cfg,
            io: ExecIo {
                rx,
                tx,
                snapshots,
                sw_signal,
                sw_queue,
            },
            observers: ExecObservers {
                epoch_observer,
                remote_epoch_observer,
            },
            block: BlockState::new(
                BlockRun::new(block_exec, shadow_tx),
                bal_tx.map(BalCapture::new),
            ),
            commits: CommitPipeline {
                snapshot,
                parent: None,
                inflight: VecDeque::new(),
            },
            cursor: Cursor {
                // `start.block` is read from the persisted resume cursor; a
                // corrupt value near `u64::MAX` must not wrap to block 0 and
                // silently re-execute the chain from genesis.
                block: start.block.saturating_add(1),
                l2_ts: start.l2_timestamp,
                fees,
                next_tx_idx: TxIndex(start.record_count),
            },
            metrics: ExecMetrics {
                applied_ok: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "ok"),
                applied_error: metrics::counter!(crate::metrics::TX_APPLIED_TOTAL, "outcome" => "error"),
            },
        }
    }
}
