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
//! This handles L2 transactions plus cross-chain (interop) deliveries.
//! Remote-epoch records travel in the DA payload by value. They are not
//! re-derivable from this chain's L1, so replay re-executes their messages
//! as 0x7D txs at the head of the block each record leads. This is exactly
//! where the live exec thread applied them.
//!
//! Deposits (L1-originated txs) are not carried in the DA payload; the
//! batcher's `MultiArchiveReader` skips `DepositRef`s. So a block range with
//! deposits cannot reconstruct a byte-identical root from blobs alone. That
//! needs re-deriving deposits from L1 events (the `da_watcher` path) and
//! merging them back in canonical order. This is a documented follow-up.
//! For deposit-free ranges (the common case, and everything the load
//! harness produces), the reconstructed root is exact.

use alloy_primitives::B256;
use kardamom_state::{StateEnv, StateSnapshot, StateWriter, TrieMode, seed_genesis};
use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, CodeEntry, Receipt, SnapshotSource, TxEnvelope,
};

use crate::actor::{StateWriterQueue, StateWriterSignal};
use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::exec_types::TxIndex;
use crate::executor::{Executor, execute_xchain_tx};
use crate::persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
use kardamom_exec_core::exec_types::TxSlot;
use kardamom_exec_core::executor::XChainDelivery;

/// One block to re-execute: its boundary metadata and ordered transactions.
///
/// The transaction list is the block's canonical order, as recovered from
/// the DA payload. Each [`TxEnvelope`]'s `sender` and `tx_hash` are trusted
/// as-is, the same as on the hot path. The proxy stamped them at the system
/// boundary, and the DA round-trip preserved them. The same trust applies to
/// `remote_epochs`: each message's `source_hash` and `seq` carry over
/// verbatim. Replay reproduces the bytes; verification is the validator's
/// job.
/// Where a block ends on the canonical stream and the L1 block it
/// derives from, as the DA payload carries them. Neither enters the
/// state trie, and neither is derivable from the block's items: an
/// epoch marker and each of its deposits take a canonical slot and never
/// reach the payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalEnd {
    /// The count of canonical records through the end of the block.
    pub end_tx_idx: u64,
    /// The L1 block number of the newest epoch at or before the block.
    pub l1_origin: u64,
}

#[derive(Clone, Debug)]
pub struct ReplayBlock {
    pub block_number: u64,
    pub l2_timestamp: u64,
    /// `None` for a block of a payload that predates the field. Replay
    /// then counts its own items, and the produced cursor is too low for
    /// a resume: it misses every slot an epoch took.
    pub canonical_end: Option<CanonicalEnd>,
    /// Remote-epoch records leading this block. Their messages execute (as
    /// 0x7D txs, in record order then seq order) BEFORE `txs` — mirroring the
    /// live pipeline, where the sealer closes the open block on a remote
    /// origin advance so a record's messages open the next one.
    pub remote_epochs: Vec<RemoteEpochRecord>,
    pub txs: Vec<TxEnvelope>,
}

impl ReplayBlock {
    /// The canonical slots the block's items take: one per remote-epoch
    /// marker, one per message, one per transaction.
    fn slots(&self) -> u64 {
        let remote: usize = self
            .remote_epochs
            .iter()
            .map(|record| 1 + record.messages.iter().count())
            .sum();
        (remote + self.txs.len()) as u64
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
    /// Number of transactions applied across all blocks.
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
    /// the previous block's end. The payload's cursor is wrong.
    #[error(
        "block {block_number}: canonical end {end_tx_idx} minus its {slots} slots is below the previous end {previous_end}"
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
    /// Move both position counters to the first slot of `block`'s items.
    /// Any epoch closes the open block before the sealer relays it, so the
    /// items the payload carries are the tail of the block's index range,
    /// and the slots before them belong to epoch markers and deposits.
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

/// Re-execute `blocks`, in canonical order, into the state DB at `env`.
/// This seeds `genesis_accounts` and `genesis_code` first.
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
    chain_id: u64,
    genesis_accounts: &[AccountChange],
    genesis_code: &[CodeEntry],
    blocks: I,
) -> Result<ReplayOutcome, ReplayError>
where
    I: IntoIterator<Item = ReplayBlock>,
{
    // Genesis must be in place before the writer publishes its initial
    // snapshot. Then block 1 already has accounts to debit.
    seed_genesis(&env, genesis_accounts, genesis_code)?;

    let handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental)?;

    // The writer adapters live inside this scope. Everything holding a delta
    // sender drops at the end of the scope. This lets the writer thread exit.
    // `handle.shutdown()` below joins the thread. It would deadlock if any
    // sender were still alive.
    let (state_root, counters) = {
        let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
        let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());
        let source = MdbxSnapshotSource::new(handle.snapshot_rx.clone());
        let mut replay = Replay::new(&mut queue, &mut signal, &source, chain_id);

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
    /// A remote-epoch record's marker. It consumes one canonical slot on
    /// the live stream, with no tx applied.
    RemoteEpochMarker,
    Exec(ExecItem<'a>),
}

/// An item that executes as one tx: an interop delivery, or an ordinary
/// transaction.
enum ExecItem<'a> {
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
    /// A `Tx` and an `XChain` message share this body; only the execution
    /// call differs.
    fn apply_one(
        &mut self,
        snapshot: &StateSnapshot,
        exec_env: ExecEnv,
        counters: &mut Counters,
        item: BlockItem<'_>,
    ) -> Result<(), ReplayError> {
        let exec_item = match item {
            BlockItem::RemoteEpochMarker => {
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
    counters: Counters,
}

impl<'a> Replay<'a> {
    fn new(
        queue: &'a mut MdbxWriterQueue,
        signal: &'a mut MdbxWriterSignal,
        source: &'a MdbxSnapshotSource,
        chain_id: u64,
    ) -> Self {
        Self {
            queue,
            signal,
            source,
            chain_id,
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
        };
        self.queue.submit(boundary, block_delta)?;
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
        };

        // Remote-epoch messages lead the block: the sealer closed the
        // previous block on the origin advance. They execute first, as
        // 0x7D txs in record order then seq order, the same order the
        // live exec thread uses, followed by the block's ordinary
        // transactions.
        let mut items = block
            .remote_epochs
            .iter()
            .flat_map(|record| {
                std::iter::once(BlockItem::RemoteEpochMarker).chain(record.messages.iter().map(
                    move |message| {
                        BlockItem::Exec(ExecItem::XChain {
                            origin_chain_id: record.origin_chain_id,
                            message,
                        })
                    },
                ))
            })
            .chain(block.txs.iter().map(|tx| BlockItem::Exec(ExecItem::Tx(tx))));

        let mut acc = BlockAcc::new(block.txs.len());
        items.try_for_each(|item| acc.apply_one(&snapshot, exec_env, &mut self.counters, item))?;

        self.seal_block(&snapshot, block, acc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256, address};
    use alloy_signer_local::PrivateKeySigner;
    use kardamom_state::{Durability, StateEnvBuilder, StateSnapshot, empty_root};
    use kardamom_types::StateDatabase;
    use revm::primitives::KECCAK_EMPTY;

    const CHAIN_ID: u64 = 1;

    /// A signed legacy transfer. Thin wrapper over
    /// `actor::test_support::legacy`, the one signed-legacy-transfer
    /// fixture this crate's tests share; `CHAIN_ID` here is 1, matching
    /// that helper's own chain id.
    fn transfer(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> TxEnvelope {
        crate::actor::test_support::legacy(signer, to, nonce, value)
    }

    fn fresh_env() -> (tempfile::TempDir, StateEnv) {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        (dir, env)
    }

    /// A genesis funding `from` with 1 ETH and the two recipients we assert on.
    fn genesis_for(from: Address) -> Vec<AccountChange> {
        vec![AccountChange {
            address: from,
            nonce: 0,
            balance: U256::from(1_000_000_000_000_000_000u128),
            code_hash: KECCAK_EMPTY,
        }]
    }

    /// Two blocks of transfers: block 1 sends 100 then 50, block 2 sends 25.
    fn two_blocks(signer: &PrivateKeySigner, to1: Address, to2: Address) -> Vec<ReplayBlock> {
        vec![
            ReplayBlock {
                block_number: 1,
                l2_timestamp: 1_700_000_000,
                canonical_end: None,
                remote_epochs: Vec::new(),
                txs: vec![transfer(signer, to1, 0, 100), transfer(signer, to2, 1, 50)],
            },
            ReplayBlock {
                block_number: 2,
                l2_timestamp: 1_700_000_001,
                canonical_end: None,
                remote_epochs: Vec::new(),
                txs: vec![transfer(signer, to1, 2, 25)],
            },
        ]
    }

    /// The live chain of `two_blocks` with an epoch before each block:
    /// an empty epoch takes one slot before block 1, and an epoch with
    /// two deposits takes three before block 2. The payload carries
    /// neither, only each block's end index and origin.
    fn two_blocks_after_epochs(
        signer: &PrivateKeySigner,
        to1: Address,
        to2: Address,
    ) -> Vec<ReplayBlock> {
        let ends = [
            CanonicalEnd {
                end_tx_idx: 3,
                l1_origin: 40,
            },
            CanonicalEnd {
                end_tx_idx: 7,
                l1_origin: 41,
            },
        ];
        two_blocks(signer, to1, to2)
            .into_iter()
            .zip(ends)
            .map(|(block, end)| ReplayBlock {
                canonical_end: Some(end),
                ..block
            })
            .collect()
    }

    /// The rebuilt cursor, headers and receipt positions are the live
    /// chain's, not a count of the replayed items, and the root is the
    /// same with and without the field.
    #[test]
    fn a_payload_cursor_gives_the_live_positions_and_the_same_root() {
        let signer = PrivateKeySigner::random();
        let to1 = address!("00000000000000000000000000000000000A0001");
        let to2 = address!("00000000000000000000000000000000000A0002");
        let genesis = genesis_for(signer.address());

        let (_plain_dir, plain_env) = fresh_env();
        let plain = replay_blocks(
            plain_env,
            CHAIN_ID,
            &genesis,
            &[],
            two_blocks(&signer, to1, to2),
        )
        .unwrap();
        assert_eq!(plain.head_end_tx_idx, None);

        let (_dir, env) = fresh_env();
        let blocks = two_blocks_after_epochs(&signer, to1, to2);
        let last_tx = blocks[1].txs[0].tx_hash;
        let outcome = replay_blocks(env.clone(), CHAIN_ID, &genesis, &[], blocks).unwrap();

        assert_eq!(outcome.state_root, plain.state_root);
        assert_eq!(outcome.head_end_tx_idx, Some(7));
        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.end_tx_position().unwrap(), BPosition::from_index(7));
        let point = kardamom_state::read_recovery_point(&env).unwrap();
        assert_eq!(point.last_fsynced_b_position, BPosition::from_index(7));
        // Block 2 is slots 3..7: the epoch marker and two deposits, then
        // its one transaction in the last slot.
        assert_eq!(
            snap.get_tx_position(last_tx).unwrap(),
            Some(BPosition::from_index(6))
        );
    }

    #[test]
    fn a_cursor_with_no_room_for_the_blocks_items_is_refused() {
        let signer = PrivateKeySigner::random();
        let to = address!("00000000000000000000000000000000000A0001");
        let mut blocks = two_blocks(&signer, to, to);
        blocks[0].canonical_end = Some(CanonicalEnd {
            end_tx_idx: 1,
            l1_origin: 0,
        });
        let (_dir, env) = fresh_env();
        let err =
            replay_blocks(env, CHAIN_ID, &genesis_for(signer.address()), &[], blocks).unwrap_err();
        assert!(
            matches!(
                err,
                ReplayError::CursorRegress {
                    block_number: 1,
                    ..
                }
            ),
            "{err}"
        );
    }

    #[test]
    fn reconstructs_state_and_balances_from_ordered_blocks() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to1 = address!("00000000000000000000000000000000000A0001");
        let to2 = address!("00000000000000000000000000000000000A0002");

        let (_dir, env) = fresh_env();
        let outcome = replay_blocks(
            env.clone(),
            CHAIN_ID,
            &genesis_for(from),
            &[],
            two_blocks(&signer, to1, to2),
        )
        .unwrap();

        assert_eq!(outcome.head_block, 2);
        assert_eq!(outcome.blocks_applied, 2);
        assert_eq!(outcome.txs_applied, 3);
        assert_ne!(outcome.state_root, empty_root());

        // Balances reflect the exact transfers. Gas price is 0, so there is no fee burn.
        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.block_number(), 2);
        assert_eq!(snap.basic(to1).unwrap().unwrap().1, U256::from(125u64)); // 100 + 25
        assert_eq!(snap.basic(to2).unwrap().unwrap().1, U256::from(50u64));
        let (from_nonce, from_balance, _) = snap.basic(from).unwrap().unwrap();
        assert_eq!(from_nonce, 3);
        assert_eq!(
            from_balance,
            U256::from(1_000_000_000_000_000_000u128 - 175u128)
        );
        // The snapshot's committed root matches the reported one.
        assert_eq!(snap.state_root().unwrap(), Some(outcome.state_root));
    }

    #[test]
    fn reconstruction_is_deterministic() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let to1 = address!("00000000000000000000000000000000000B0001");
        let to2 = address!("00000000000000000000000000000000000B0002");

        let (_d1, env1) = fresh_env();
        let (_d2, env2) = fresh_env();
        let r1 = replay_blocks(
            env1,
            CHAIN_ID,
            &genesis_for(from),
            &[],
            two_blocks(&signer, to1, to2),
        )
        .unwrap();
        let r2 = replay_blocks(
            env2,
            CHAIN_ID,
            &genesis_for(from),
            &[],
            two_blocks(&signer, to1, to2),
        )
        .unwrap();

        // Same inputs give a byte-identical reconstructed root. This is the
        // property the DA round-trip depends on.
        assert_eq!(r1.state_root, r2.state_root);
        assert_eq!(r1.head_block, r2.head_block);
    }

    #[test]
    fn empty_block_stream_yields_genesis_root() {
        let signer = PrivateKeySigner::random();
        let from = signer.address();
        let (_dir, env) = fresh_env();
        let outcome =
            replay_blocks(env.clone(), CHAIN_ID, &genesis_for(from), &[], Vec::new()).unwrap();
        assert_eq!(outcome.head_block, 0);
        assert_eq!(outcome.blocks_applied, 0);
        // With no blocks, the root is the seeded genesis root.
        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.state_root().unwrap(), Some(outcome.state_root));
    }
}
