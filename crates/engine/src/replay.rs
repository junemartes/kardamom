//! Offline re-execution of an already-ordered L2 block stream.
//!
//! This rebuilds L2 state, and its canonical Ethereum MPT world-state root,
//! from ordered transactions alone, with no Aeron substrate. It is the
//! inverse of a live executor run. The DA-recovery path (`rebuild-from-L1`)
//! decodes the blobs the batcher posted, turns them back into ordered
//! blocks, and drives them through here to rebuild a byte-identical state
//! DB. It reuses the same per-tx execution ([`execute_tx`]), delta
//! accumulation, and trie-aware state writer the live executor and
//! validator use. So the reconstructed state root matches the canonical
//! chain's root.
//!
//! ## Scope
//!
//! This handles L2 transactions, L1 deposits, and cross-chain (interop)
//! deliveries.
//!
//! Remote-epoch records travel in the DA payload by value. They are not
//! re-derivable from this chain's L1, so replay re-executes their messages
//! as 0x7D txs at the head of the block each record leads. This is exactly
//! where the live exec thread applied them.
//!
//! Deposits do not travel in the DA payload: a deposit is unsigned, so a
//! payload-carried deposit would be an unverifiable claim. L1 fixes each
//! deposit and its order inside its epoch, and the block's L1 origin fixes
//! its L2 block. The caller derives the epochs from L1 and gives them in
//! [`ReplayBlock::l1_epochs`]. Replay applies each epoch at the head of its
//! block as the live exec thread does: a marker slot, then each deposit
//! through [`execute_deposit_tx`].

use alloy_primitives::B256;
use kardamom_state::{StateEnv, StateSnapshot, StateWriter, TrieMode, seed_genesis};
use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockFees, CodeEntry, Deposit, EpochRecord,
    FeeSchedule, Receipt, SnapshotSource, TxEnvelope,
};

use crate::actor::StateWriterSignal;
use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::exec_types::TxIndex;
use crate::executor::{Executor, execute_deposit_tx, execute_xchain_tx};
use crate::persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
use kardamom_exec_core::exec_types::TxSlot;
use kardamom_exec_core::executor::XChainDelivery;

/// Where a block ends on the canonical stream and the L1 block it
/// derives from, as the DA payload carries them. Neither enters the
/// state trie, and neither is derivable from the block's payload items:
/// an epoch marker and each of its deposits take a canonical slot and
/// never reach the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalEnd {
    /// The count of canonical records through the end of the block.
    pub end_tx_idx: u64,
    /// The L1 block number of the newest epoch at or before the block.
    pub l1_origin: u64,
}

/// One block to re-execute: its boundary metadata and ordered items.
///
/// The transaction list is the block's canonical order, as recovered from
/// the DA payload. Each [`TxEnvelope`]'s `sender` and `tx_hash` are trusted
/// as-is, the same as on the hot path. The proxy stamped them at the system
/// boundary, and the DA round-trip preserved them. The same trust applies to
/// `remote_epochs`: each message's `source_hash` and `seq` carry over
/// verbatim, and to `l1_epochs`, which the caller derived from L1. Replay
/// reproduces the bytes; verification is the validator's job.
#[derive(Clone, Debug)]
pub struct ReplayBlock {
    pub block_number: u64,
    pub l2_timestamp: u64,
    /// `None` for a block of a payload that predates the field. Replay
    /// then counts its own items, and the produced cursor is too low for
    /// a resume: it misses every slot an epoch took.
    pub canonical_end: Option<CanonicalEnd>,
    /// L1 epochs leading this block, in origin order. Each takes a marker
    /// slot, then one slot per deposit, and its deposits execute in log
    /// order before `remote_epochs` and `txs`.
    pub l1_epochs: Vec<EpochRecord>,
    /// Remote-epoch records leading this block. Their messages execute (as
    /// 0x7D txs, in record order then seq order) before `txs`. The sealer
    /// closes the open block on a remote origin advance, so a record's
    /// messages open the next block.
    pub remote_epochs: Vec<RemoteEpochRecord>,
    pub txs: Vec<TxEnvelope>,
}

impl ReplayBlock {
    /// The canonical slots the block's items take: one per epoch marker,
    /// one per deposit, one per message, one per transaction.
    fn slots(&self) -> u64 {
        let l1: usize = self
            .l1_epochs
            .iter()
            .map(|epoch| 1 + epoch.deposits.len())
            .sum();
        let remote: usize = self
            .remote_epochs
            .iter()
            .map(|record| 1 + record.messages.iter().count())
            .sum();
        (l1 + remote + self.txs.len()) as u64
    }

    /// The block's items in the order the live exec thread applies them.
    ///
    /// The sealer closes the open block before it relays an epoch, when
    /// that block holds a record. So an epoch leads its block, and its
    /// marker and items come first. An L1 epoch comes before a remote
    /// epoch. A live block holds at most one epoch, because an epoch takes
    /// at least one slot; this order only matters for a caller that puts
    /// more than one epoch in one block.
    fn items(&self) -> impl Iterator<Item = BlockItem<'_>> {
        let l1 = self.l1_epochs.iter().flat_map(|epoch| {
            std::iter::once(BlockItem::Marker).chain(
                epoch
                    .deposits
                    .iter()
                    .map(|deposit| BlockItem::Exec(ExecItem::Deposit(deposit))),
            )
        });
        let remote = self.remote_epochs.iter().flat_map(|record| {
            std::iter::once(BlockItem::Marker).chain(record.messages.iter().map(move |message| {
                BlockItem::Exec(ExecItem::XChain {
                    origin_chain_id: record.origin_chain_id,
                    message,
                })
            }))
        });
        let txs = self.txs.iter().map(|tx| BlockItem::Exec(ExecItem::Tx(tx)));
        l1.chain(remote).chain(txs)
    }
}

/// Result of a reconstruction run.
#[derive(Clone, Copy, Debug)]
pub struct ReplayOutcome {
    /// Highest block re-executed (0 if no blocks were applied).
    pub head_block: u64,
    /// The canonical end index of `head_block`, when the payload carried
    /// it: the cursor a consumer resumes from. `None` means the state is
    /// correct but not resumable.
    pub head_end_tx_idx: Option<u64>,
    /// Number of blocks applied.
    pub blocks_applied: u64,
    /// Number of executed items (deposits, interop deliveries and
    /// transactions) applied across all blocks.
    pub txs_applied: u64,
    /// Canonical MPT world-state root committed at `head_block`.
    pub state_root: B256,
}

/// Errors surfaced by [`replay_blocks`].
#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("state: {0}")]
    State(#[from] kardamom_state::StateError),
    #[error("execution: {0}")]
    Execution(#[from] crate::error::ExecutorError),
    /// The writer committed, but stored no state root. This means it was
    /// spawned with `TrieMode::Off`. `replay_blocks` always uses
    /// `Incremental`, so this fires only if that invariant breaks.
    #[error("reconstruction produced no state root")]
    NoStateRoot,
    /// A block's cumulative gas overflowed `u64`. Replay applies no block
    /// gas limit, so a corrupt or adversarial receipt stream is the only
    /// way to reach this.
    #[error("cumulative gas overflow in block {block_number}")]
    GasOverflow { block_number: u64 },
    /// The block's canonical end leaves no room for its own items after
    /// the previous block's end. Either the payload's cursor is wrong, or
    /// the caller gave the block epochs or deposits that the chain does not
    /// hold there.
    #[error(
        "block {block_number}: canonical end {end_tx_idx} leaves no room for its {slots} slots (epoch markers, deposits, remote records, transactions) after the previous end {previous_end}"
    )]
    CursorRegress {
        block_number: u64,
        end_tx_idx: u64,
        slots: u64,
        previous_end: u64,
    },
}

/// Running counters threaded through [`drive_blocks`].
#[derive(Default)]
struct Counters {
    head: u64,
    blocks_applied: u64,
    txs_applied: u64,
    /// Executor-local sanity counter (mirrors the live `tx_ordering` reader).
    tx_idx: u64,
    /// Synthetic canonical position. Neither this nor `tx_idx` feeds the state
    /// trie; only account, storage, and code writes do. So any monotonic
    /// sequence gives the same reconstructed root. We keep them consistent so
    /// the per-tx receipts stay internally coherent.
    global_pos: u64,
    /// The canonical end of the head block, when its payload carried it.
    head_end_tx_idx: Option<u64>,
}

impl Counters {
    /// Move both position counters to the first slot of `block`'s items,
    /// which fill the tail of the block's index range. Without a vacant
    /// slot that start is the previous end, as on the live stream. A
    /// vacant slot (a voided entry, or the void record) applies nothing and
    /// never reaches the payload, so the block's items may leave slots
    /// free; they may not need more slots than the range holds.
    fn anchor(&mut self, block: &ReplayBlock, end: CanonicalEnd) -> Result<(), ReplayError> {
        let slots = block.slots();
        let start = end
            .end_tx_idx
            .checked_sub(slots)
            .filter(|start| *start >= self.global_pos)
            .ok_or(ReplayError::CursorRegress {
                block_number: block.block_number,
                end_tx_idx: end.end_tx_idx,
                slots,
                previous_end: self.global_pos,
            })?;
        self.global_pos = start;
        self.tx_idx = start;
        Ok(())
    }
}

/// The chain a replay rebuilds: its id, its genesis allocation, and its
/// fee schedule. Every value feeds the state roots, so a replay with the
/// wrong one silently produces a different, wrong, root.
#[derive(Debug, Clone, Copy)]
pub struct ReplayGenesis<'a> {
    pub chain_id: u64,
    pub accounts: &'a [AccountChange],
    pub code: &'a [CodeEntry],
    pub fees: Option<FeeSchedule>,
}

impl<'a> ReplayGenesis<'a> {
    /// The replay chain of a parsed genesis file and its allocation.
    #[must_use]
    pub fn of(
        genesis: &kardamom_types::Genesis,
        accounts: &'a [AccountChange],
        code: &'a [CodeEntry],
    ) -> Self {
        Self {
            chain_id: genesis.chain_id,
            accounts,
            code,
            fees: genesis.fees,
        }
    }
}

/// Re-execute `blocks`, in canonical order, into the state DB at `env`.
/// This seeds the genesis allocation first.
///
/// `env` should be a fresh state DB, for a from-scratch reconstruction.
/// Seeding is idempotent: an already-seeded env is left untouched. So
/// re-running against a partly built DB is safe, as long as the genesis
/// matches. This returns the head block and its canonical state root. On
/// success, the state DB at `env` is fully populated.
///
/// `chain_id` must match the chain being reconstructed; it feeds every tx's
/// `CfgEnv`. A mismatch silently produces a different, wrong, root.
///
/// # Errors
///
/// Returns `Err` when a block fails to re-execute (a malformed
/// transaction, or a state-DB failure), or when the writer commits with
/// no state root (an internal invariant break: see [`ReplayError::NoStateRoot`]).
pub fn replay_blocks<I>(
    env: StateEnv,
    genesis: &ReplayGenesis<'_>,
    blocks: I,
) -> Result<ReplayOutcome, ReplayError>
where
    I: IntoIterator<Item = ReplayBlock>,
{
    // Genesis must be in place before the writer publishes its initial
    // snapshot. Then block 1 already has accounts to debit.
    seed_genesis(&env, genesis.accounts, genesis.code)?;

    let handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental)?;

    // The writer adapters live inside this scope. Everything holding a delta
    // sender drops at the end of the scope. This lets the writer thread exit.
    // `handle.shutdown()` below joins the thread. It would deadlock if any
    // sender were still alive.
    let (state_root, counters) = {
        let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
        let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());
        let source = MdbxSnapshotSource::new(handle.snapshot_rx.clone());
        let mut replay = Replay::new(&mut queue, &mut signal, &source, genesis);

        blocks
            .into_iter()
            .try_for_each(|block| replay.drive_block(&block))?;

        // Read the final root only if the drive succeeded, and before
        // tearing down.
        let root = source
            .snapshot_after(replay.counters.head)
            .state_root()
            .map_err(ReplayError::from)
            .and_then(|o| o.ok_or(ReplayError::NoStateRoot))?;
        (root, replay.counters)
    };

    Ok(ReplayOutcome {
        head_block: counters.head,
        head_end_tx_idx: counters.head_end_tx_idx,
        blocks_applied: counters.blocks_applied,
        txs_applied: counters.txs_applied,
        state_root,
    })
}

/// One item to apply inside a block, in the order the live exec thread
/// would see it.
enum BlockItem<'a> {
    /// An L1 epoch's or a remote-epoch record's marker. It consumes one
    /// canonical slot on the live stream, with no tx applied.
    Marker,
    Exec(ExecItem<'a>),
}

/// An item that executes as one tx: a deposit, an interop delivery, or an
/// ordinary transaction.
enum ExecItem<'a> {
    Deposit(&'a Deposit),
    XChain {
        origin_chain_id: u64,
        message: &'a XChainMessage,
    },
    Tx(&'a TxEnvelope),
}

/// Per-block accumulator: the live delta, the receipts collected so far,
/// and the running gas and in-block index.
struct BlockAcc {
    delta: PendingDelta,
    receipts: Vec<Receipt>,
    cumulative_gas: u64,
    tx_index_in_block: u64,
}

impl BlockAcc {
    fn new(tx_capacity: usize) -> Self {
        Self {
            delta: PendingDelta::new(),
            receipts: Vec::with_capacity(tx_capacity),
            cumulative_gas: 0,
            tx_index_in_block: 0,
        }
    }

    /// Apply one block item, and advance `counters`. A marker only
    /// advances the counters, to mirror the live shape (see [`Counters`]).
    /// A deposit, an `XChain` message and a `Tx` share this body; only the
    /// execution call differs.
    fn apply_one(
        &mut self,
        snapshot: &StateSnapshot,
        exec_env: ExecEnv,
        counters: &mut Counters,
        item: BlockItem<'_>,
    ) -> Result<(), ReplayError> {
        let exec_item = match item {
            BlockItem::Marker => {
                counters.tx_idx += 1;
                counters.global_pos += 1;
                return Ok(());
            }
            BlockItem::Exec(e) => e,
        };
        let tx_position = BPosition::from_index(counters.global_pos);
        let slot = TxSlot {
            tx_idx: TxIndex(counters.tx_idx),
            tx_position,
            tx_index_in_block: self.tx_index_in_block,
            cumulative_gas_used_before: self.cumulative_gas,
        };
        // Replay executes one durably committed block at a time against its
        // own committed snapshot. There is no pipelined parent layer.
        let (receipt, ws) = match exec_item {
            ExecItem::Deposit(deposit) => {
                execute_deposit_tx(snapshot, None, &self.delta, exec_env, slot, deposit, None)?
            }
            ExecItem::XChain {
                origin_chain_id,
                message,
            } => execute_xchain_tx(
                snapshot,
                None,
                &self.delta,
                exec_env,
                slot,
                XChainDelivery {
                    origin_chain_id,
                    message,
                },
                None,
            )?,
            ExecItem::Tx(tx) => {
                Executor::execute_once(snapshot, None, &self.delta, exec_env, slot, tx, None)?
            }
        };
        self.delta.apply(ws);
        // Replay applies no block gas limit, so nothing else bounds this
        // sum; a corrupt receipt stream is fail-stop rather than wrapped.
        self.cumulative_gas =
            self.cumulative_gas
                .checked_add(receipt.gas_used)
                .ok_or(ReplayError::GasOverflow {
                    block_number: exec_env.block_number,
                })?;
        self.receipts.push(receipt);
        self.tx_index_in_block += 1;
        counters.tx_idx += 1;
        counters.global_pos += 1;
        counters.txs_applied += 1;
        Ok(())
    }
}

/// Drives one reconstruction run: owns the writer adapters and the running
/// counters across every block.
struct Replay<'a> {
    queue: &'a mut MdbxWriterQueue,
    signal: &'a mut MdbxWriterSignal,
    source: &'a MdbxSnapshotSource,
    chain_id: u64,
    /// The fees of the next block to replay. A replay starts at genesis,
    /// so this starts at the schedule's first values and advances with
    /// each block's gas used, as the live exec thread's cursor does.
    fees: BlockFees,
    counters: Counters,
}

impl<'a> Replay<'a> {
    fn new(
        queue: &'a mut MdbxWriterQueue,
        signal: &'a mut MdbxWriterSignal,
        source: &'a MdbxSnapshotSource,
        genesis: &ReplayGenesis<'_>,
    ) -> Self {
        Self {
            queue,
            signal,
            source,
            chain_id: genesis.chain_id,
            fees: BlockFees::genesis(genesis.fees),
            counters: Counters::default(),
        }
    }

    /// Finalize `acc`'s delta and receipts into a boundary, run the
    /// block-close protocol actions, then submit the block and wait for
    /// the durable commit.
    ///
    /// Same block-close protocol actions the live engine runs, through the
    /// same shared implementation — a reconstructor that skipped them
    /// would rebuild a chain whose state diverges from the canonical one
    /// the moment any feature is active. Replay commits one block at a
    /// time against its own committed snapshot, so there is no parent
    /// layer to consult: `delta` then snapshot.
    fn seal_block(
        &mut self,
        snapshot: &StateSnapshot,
        block: &ReplayBlock,
        mut acc: BlockAcc,
    ) -> Result<(), ReplayError> {
        // Offline replay commits one block at a time with no pipelined
        // layer, so this reads only `acc.delta` then the snapshot.
        kardamom_exec_core::features::apply_block_close_actions(
            &mut acc.delta,
            block.block_number,
            block.l2_timestamp,
            None,
            snapshot,
        )?;

        let gas_used = acc.receipts.last().map_or(0, |r| r.cumulative_gas_used);
        let block_delta = acc.delta.finalize(block.block_number, acc.receipts);
        let boundary = BlockBoundary {
            block_number: block.block_number,
            // After `Counters::anchor` and the block's items, the position
            // counter stands on the payload's end index. Without the
            // field it is the count of replayed items, and the origin is
            // unknown.
            end_tx_idx: BPosition::from_index(self.counters.global_pos),
            l2_timestamp: block.l2_timestamp,
            l1_origin: block.canonical_end.map_or(0, |end| end.l1_origin),
            base_fee: self.fees.base_fee,
            gas_used,
        };
        self.fees = self.fees.next(gas_used);
        self.queue.submit_rebuilt(boundary, block_delta)?;
        self.signal.wait_committed(block.block_number)?;
        self.counters.head = block.block_number;
        self.counters.head_end_tx_idx = block.canonical_end.map(|end| end.end_tx_idx);
        self.counters.blocks_applied += 1;
        Ok(())
    }

    /// Executes one block's txs, submits the block delta, and waits for
    /// the durable commit.
    fn drive_block(&mut self, block: &ReplayBlock) -> Result<(), ReplayError> {
        // Take the snapshot after the previously committed block, or
        // genesis for the first block. `wait_committed` below keeps the
        // published snapshot anchored at `self.counters.head`. So this is
        // exactly the pre-block state view.
        if let Some(end) = block.canonical_end {
            self.counters.anchor(block, end)?;
        }
        let snapshot = self.source.snapshot_after(self.counters.head);
        let exec_env = ExecEnv {
            chain_id: self.chain_id,
            block_number: block.block_number,
            l2_timestamp: block.l2_timestamp,
            fees: self.fees,
        };

        let mut acc = BlockAcc::new(block.txs.len());
        block
            .items()
            .try_for_each(|item| acc.apply_one(&snapshot, exec_env, &mut self.counters, item))?;

        self.seal_block(&snapshot, block, acc)
    }
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
