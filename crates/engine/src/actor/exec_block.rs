//! The per-block state: [`BlockState`], the execution mode [`BlockRun`], and
//! the optional captures. Each optional capture owns its sender and its
//! per-block buffer together, so a role that does not wire it carries
//! neither.

use std::time::Duration;

use crossbeam_channel::Sender;
use kardamom_types::BlockBoundary;
use revm::state::bal::Bal;

use crate::block_env::ExecEnv;
use crate::delta::{PendingDelta, WriteSet};
use crate::error::ExecutorError;
use crate::shadow::{ShadowBlock, ShadowTxCapture};

use super::exec_thread::Flow;
use super::types::{BalHandoff, BlockExecOutput, BlockExecStrategy, BufferedRecord};
use super::wiring::{ExecPorts, SnapshotDb};

/// Everything that belongs to the block being executed. The boundary arm
/// consumes or resets each field when it seals the block.
pub(super) struct BlockState<W: ExecPorts> {
    pub(super) delta: PendingDelta,
    /// Per-block receipts, in arrival order. The code drains this into the
    /// `BlockDelta` at each boundary, so the writer can persist the receipts
    /// and the `tx_hash_index` tables. Each tx's receipt is cloned once, to
    /// feed both this list and the streaming `tx_receipts` publisher. This
    /// clone cost is flagged for saturation validation.
    pub(super) receipts: Vec<kardamom_types::Receipt>,
    /// The reference of every transaction of the block, in arrival
    /// order: where its bytes are on a `tx_data` archive. The boundary
    /// hands them to the state writer, which keeps each one with its
    /// receipt, so the batcher can rebuild a block the sealer no longer
    /// retains. One 56-byte copy per transaction.
    pub(super) refs: Vec<kardamom_types::TxRef>,
    /// Per-block RPC enrichment counters.
    pub(super) tx_index: u64,
    pub(super) cumulative_gas_used: u64,
    /// Wall time spent executing the block's txs and deposits. This
    /// excludes channel idle time between txs. The `BoundaryStart` handler
    /// records this value when it closes the block. It is `None` for empty
    /// blocks.
    pub(super) apply_elapsed: Option<Duration>,
    /// EIP-7928 capture. It is `Some` only when a BAL publisher is
    /// attached (executor role).
    pub(super) bal: Option<BalCapture>,
    /// How the block's records execute.
    pub(super) run: BlockRun<W>,
}

impl<W: ExecPorts> BlockState<W> {
    pub(super) fn new(run: BlockRun<W>, bal: Option<BalCapture>) -> Self {
        Self {
            delta: PendingDelta::new(),
            receipts: Vec::new(),
            refs: Vec::new(),
            tx_index: 0,
            cumulative_gas_used: 0,
            apply_elapsed: None,
            bal,
            run,
        }
    }
}

/// EIP-7928 capture: the BAL publisher's sender and the block's Bal.
pub(super) struct BalCapture {
    tx: Sender<BalHandoff>,
    pub(super) bal: Bal,
}

impl BalCapture {
    pub(super) fn new(tx: Sender<BalHandoff>) -> Self {
        Self {
            tx,
            bal: Bal::new(),
        }
    }

    /// The capture target for the tx at `tx_index`: the block's Bal and
    /// the tx's 1-based BAL index.
    pub(super) fn slot(&mut self, tx_index: u64) -> (&mut Bal, u64) {
        (&mut self.bal, tx_index + 1)
    }

    /// Log capture progress every 512 txs.
    pub(super) fn log_progress(&self, block: u64, tx_index: u64, ws: &WriteSet) {
        if tx_index.is_multiple_of(512) {
            tracing::debug!(
                block,
                tx_index_in_block = tx_index,
                bal_accounts = self.bal.accounts.len(),
                ws_accounts = ws.accounts.len(),
                "BAL capture progress"
            );
        }
    }

    /// Move the block's Bal, and a receipts-free copy of the merged delta,
    /// to the publisher thread. Encoding and reliable delivery happen
    /// entirely off the exec thread. `try_send` keeps the handoff off the
    /// critical path: a dropped frame costs one block of BAL retention,
    /// which verifies as `bal_missing`, a tolerated path. A dropped send
    /// also means the publisher is gone mid-shutdown; this is not fatal.
    pub(super) fn handoff(&mut self, boundary: &BlockBoundary, pending: &PendingDelta) {
        let block_number = boundary.block_number;
        let delta = pending.clone().finalize(block_number, Vec::new());
        self.tx.try_handoff(
            BalHandoff {
                boundary: boundary.clone(),
                delta,
                bal: std::mem::take(&mut self.bal),
            },
            block_number,
            BAL_HANDOFF,
            || {},
        );
    }
}

/// How a block's records execute. The role picks one at startup.
pub(super) enum BlockRun<W: ExecPorts> {
    /// Each record executes on arrival. Boxed: the scope makes this
    /// variant far larger than `Whole`, and the box is allocated once per
    /// exec thread.
    Streaming(Box<Streaming<W>>),
    /// Records buffer until the boundary. Then a whole-block strategy
    /// executes them (the validator's parallel path, the executor's STM
    /// pool).
    Whole(WholeBlock<W>),
}

impl<W: ExecPorts> BlockRun<W> {
    /// A strategy selects the whole-block mode. That mode has no per-tx
    /// captures, so it drops the shadow sender, and the shadow thread
    /// exits.
    pub(super) fn new(
        block_exec: Option<W::BlockExec>,
        shadow_tx: Option<Sender<ShadowBlock>>,
    ) -> Self {
        match block_exec {
            Some(strategy) => Self::Whole(WholeBlock {
                strategy,
                buffered: Vec::new(),
            }),
            None => Self::Streaming(Box::new(Streaming {
                scope: None,
                shadow: shadow_tx.map(ShadowCapture::new),
            })),
        }
    }

    /// True while a block has records in progress: the streaming scope is
    /// built, or records are buffered. Between blocks, this is false.
    pub(super) fn is_open(&self) -> bool {
        match self {
            Self::Streaming(streaming) => streaming.scope.is_some(),
            Self::Whole(whole) => !whole.buffered.is_empty(),
        }
    }

    /// End the streaming block: drop its execution scope, then hand off
    /// its shadow captures. The next block gets a new parent layer and
    /// block env, and, once commits settle, a fresh snapshot. The drop is
    /// unconditional: a scope reused across a boundary would execute
    /// against the previous block's parent and env. The whole-block mode
    /// holds nothing here.
    pub(super) fn seal(&mut self, block_number: u64) {
        if let Self::Streaming(streaming) = self {
            streaming.scope = None;
            if let Some(shadow) = streaming.shadow.as_mut() {
                shadow.handoff(block_number);
            }
        }
    }
}

/// The streaming mode's per-block state.
pub(super) struct Streaming<W: ExecPorts> {
    /// Per-block execution scope: one EVM and one commit-into cache for
    /// the whole block. Building these per tx used about 90% of the
    /// allocation in the execution path.
    ///
    /// The code drops the scope at each boundary. It rebuilds the scope
    /// lazily, at the block's first tx. The rebuild seeds it with the
    /// parent and anything already in the live delta, for example deposits
    /// that landed before the first tx.
    pub(super) scope: Option<crate::executor::Executor<SnapshotDb<W>>>,
    /// Footprint shadow capture (`crate::shadow`). It is `Some` only when
    /// the shadow is on (executor role).
    pub(super) shadow: Option<ShadowCapture>,
}

/// The whole-block mode's strategy and the records it replays at the
/// boundary.
pub(super) struct WholeBlock<W: ExecPorts> {
    strategy: W::BlockExec,
    buffered: Vec<BufferedRecord>,
}

impl<W: ExecPorts> WholeBlock<W> {
    /// Buffer `rec` for the strategy to replay at the boundary, instead of
    /// executing it now. Returns the `Flow::Continue` the caller must
    /// return immediately, with no further work this call.
    pub(super) fn defer(&mut self, rec: BufferedRecord) -> Flow {
        self.buffered.push(rec);
        Flow::Continue
    }

    /// Execute every buffered record against `snapshot` and `parent`, and
    /// empty the buffer for the next block. Pair each record with its
    /// receipt. A strategy must return one receipt per record, in record
    /// order; any other count is an error.
    pub(super) fn execute(
        &mut self,
        snapshot: &SnapshotDb<W>,
        parent: Option<&PendingDelta>,
        env: ExecEnv,
        block_number: u64,
    ) -> Result<ExecutedBlock, ExecutorError> {
        let records = std::mem::take(&mut self.buffered);
        let BlockExecOutput {
            receipts,
            delta,
            bal,
        } = self
            .strategy
            .execute_block(snapshot, parent, &records, env, block_number)?;
        if receipts.len() != records.len() {
            return Err(ExecutorError::State(format!(
                "block-exec strategy returned {} receipts for {} records in block {block_number}",
                receipts.len(),
                records.len()
            )));
        }
        Ok(ExecutedBlock {
            records: records.into_iter().zip(receipts).collect(),
            delta,
            bal,
        })
    }
}

/// A whole-block strategy's output, with each record paired to its
/// receipt in record order.
pub(super) struct ExecutedBlock {
    pub(super) records: Vec<(BufferedRecord, kardamom_types::Receipt)>,
    pub(super) delta: PendingDelta,
    pub(super) bal: Option<Bal>,
}

/// Footprint shadow capture: the grader's sender, the block's tx captures,
/// and its serial-lane count. The code hands these off with `try_send` at
/// each boundary; this never blocks.
pub(super) struct ShadowCapture {
    tx: Sender<ShadowBlock>,
    captures: Vec<ShadowTxCapture>,
    serial: u32,
}

impl ShadowCapture {
    fn new(tx: Sender<ShadowBlock>) -> Self {
        Self {
            tx,
            captures: Vec::new(),
            serial: 0,
        }
    }

    /// Record one applied tx. Cloning the envelope is just a refcount
    /// increment. Cell extraction is one pass over the small per-tx sets.
    pub(super) fn capture(
        &mut self,
        envelope: &kardamom_types::TxEnvelope,
        touches: crate::executor::TouchSet,
        receipt: &kardamom_types::Receipt,
        ws: &WriteSet,
    ) {
        self.captures.push(ShadowTxCapture {
            envelope: envelope.clone(),
            gas_used: receipt.gas_used,
            touches,
            write_cells: crate::shadow::write_cells(ws),
        });
    }

    /// Count one record on the serial barrier lane. Deposits take this
    /// lane: the shadow counts them, it does not model them.
    pub(super) fn count_serial(&mut self) {
        self.serial += 1;
    }

    /// Hand the block's captures to the grader, with the same never-block
    /// discipline as the BAL handoff. A dropped block costs one block of
    /// measurement, which is counted, not the chain. Skips empty blocks;
    /// there is nothing to grade.
    fn handoff(&mut self, block_number: u64) {
        if self.captures.is_empty() && self.serial == 0 {
            return;
        }
        let blk = ShadowBlock {
            block_number,
            captures: std::mem::take(&mut self.captures),
            serial_records: std::mem::take(&mut self.serial),
        };
        self.tx.try_handoff(blk, block_number, SHADOW_HANDOFF, || {
            metrics::counter!(
                crate::metrics::FOOTPRINT_BLOCKS_TOTAL,
                "outcome" => "dropped"
            )
            .increment(1);
        });
    }
}

/// Never-block handoff to a bounded auxiliary channel (the BAL publisher,
/// the footprint-shadow grader): `try_send`, then ignore a disconnected
/// receiver (the consumer is gone mid-shutdown, not fatal), or run
/// `on_full` (any extra bookkeeping, for example a dropped-block metric)
/// and warn when the channel is full.
///
/// An extension trait: `Sender` is foreign to this crate, so this cannot
/// be an inherent method.
trait SenderHandoff<T> {
    fn try_handoff(
        &self,
        item: T,
        block_number: u64,
        labels: HandoffLabels,
        on_full: impl FnOnce(),
    );
}

impl<T> SenderHandoff<T> for Sender<T> {
    fn try_handoff(
        &self,
        item: T,
        block_number: u64,
        labels: HandoffLabels,
        on_full: impl FnOnce(),
    ) {
        match self.try_send(item) {
            Ok(()) | Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                on_full();
                tracing::warn!(
                    block = block_number,
                    "{} handoff full; dropping this block's {}",
                    labels.what,
                    labels.dropped
                );
            }
        }
    }
}

/// One [`SenderHandoff::try_handoff`] call site's log wording: the name
/// used in the warn line, and the noun for what gets dropped.
struct HandoffLabels {
    what: &'static str,
    dropped: &'static str,
}

const BAL_HANDOFF: HandoffLabels = HandoffLabels {
    what: "BAL",
    dropped: "frame (publisher pump stalled?)",
};

const SHADOW_HANDOFF: HandoffLabels = HandoffLabels {
    what: "footprint-shadow",
    dropped: "capture",
};
