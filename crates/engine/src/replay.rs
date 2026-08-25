//! Offline re-execution of an already-ordered L2 block stream.
//!
//! Reconstructs L2 state — and its canonical Ethereum MPT world-state root —
//! from ordered transactions alone, with no Aeron substrate. This is the
//! inverse of a live executor run: the DA-recovery path (`rebuild-from-L1`)
//! decodes the blobs the batcher posted back into ordered blocks and drives
//! them through here to rebuild a byte-identical state DB. It reuses the exact
//! per-tx execution ([`execute_tx`]), delta accumulation, and trie-aware state
//! writer the live executor/validator use, so the reconstructed state root
//! matches the canonical chain's.
//!
//! ## Scope
//!
//! L2 transactions only. Deposits (L1-originated) are not carried in the DA
//! payload — the batcher's `MultiArchiveReader` skips `DepositRef`s — so a
//! block range that contains deposits cannot be reconstructed to a
//! byte-identical root from blobs alone; that requires re-deriving deposits
//! from L1 events (the `da_watcher` path) and interleaving them in canonical
//! order, a documented follow-up. For deposit-free ranges (the common case,
//! and everything the load harness produces) the reconstructed root is exact.

use alloy_primitives::B256;
use kardamom_state::{StateEnv, StateWriter, TrieMode, seed_genesis};
use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, CodeEntry, SnapshotSource, TxEnvelope,
};

use crate::actor::{StateWriterQueue, StateWriterSignal};
use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::exec_types::TxIndex;
use crate::executor::execute_tx;
use crate::persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};

/// One block to re-execute: boundary metadata + its ordered transactions.
///
/// The transaction list is the block's canonical order (as recovered from the
/// DA payload). `sender`/`tx_hash` on each [`TxEnvelope`] are trusted exactly
/// as they are on the hot path — the proxy stamped them at the system boundary
/// and they were preserved through the DA round-trip.
#[derive(Clone, Debug)]
pub struct ReplayBlock {
    pub block_number: u64,
    pub l2_timestamp: u64,
    pub txs: Vec<TxEnvelope>,
}

/// Result of a reconstruction run.
#[derive(Clone, Copy, Debug)]
pub struct ReplayOutcome {
    /// Highest block re-executed (0 if no blocks were applied).
    pub head_block: u64,
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
    /// The writer committed but persisted no state root — it was spawned with
    /// `TrieMode::Off`. `replay_blocks` always uses `Incremental`, so this only
    /// fires if the invariant is broken.
    #[error("reconstruction produced no state root")]
    NoStateRoot,
}

/// Running counters threaded through [`drive_blocks`].
#[derive(Default)]
struct Counters {
    head: u64,
    blocks_applied: u64,
    txs_applied: u64,
    /// Executor-local sanity counter (mirrors the live tx_ordering reader).
    tx_idx: u64,
    /// Synthetic canonical position. Neither this nor `tx_idx` feed the state
    /// trie (only accounts/storage/code writes do), so any monotonic sequence
    /// yields the same reconstructed root; we keep them consistent so the
    /// per-tx receipts we build are internally coherent.
    global_pos: u64,
}

/// Re-execute `blocks` (in canonical order) into the state DB at `env`, seeding
/// `genesis_accounts` / `genesis_code` first.
///
/// `env` should be a fresh state DB (a from-scratch reconstruction). Seeding is
/// idempotent — an already-seeded env is left untouched — so re-running against
/// a partially-built DB is safe as long as the genesis matches. Returns the
/// head block and its canonical state root; the state DB at `env` is left fully
/// populated on success.
///
/// `chain_id` must match the chain being reconstructed (it feeds every tx's
/// `CfgEnv`); a mismatch silently produces a different — wrong — root.
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
    // snapshot, so block 1 already has accounts to debit.
    seed_genesis(&env, genesis_accounts, genesis_code)?;

    let handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental)?;

    let mut counters = Counters::default();
    // The writer adapters live inside this helper scope: everything holding a
    // delta sender drops at its end, which is what lets the writer thread
    // exit — `handle.shutdown()` below joins it and would deadlock while any
    // sender were still alive.
    let (drive, root) = {
        let mut queue = MdbxWriterQueue::new(handle.delta_tx.clone());
        let mut signal = MdbxWriterSignal::new(handle.snapshot_rx.clone());
        let source = MdbxSnapshotSource::new(handle.snapshot_rx.clone());

        let drive = drive_blocks(
            &mut queue,
            &mut signal,
            &source,
            chain_id,
            blocks,
            &mut counters,
        );

        // Read the final root while the drive succeeded and before tearing
        // down.
        let root = match &drive {
            Ok(()) => source
                .snapshot_after(counters.head)
                .state_root()
                .map_err(ReplayError::from)
                .and_then(|o| o.ok_or(ReplayError::NoStateRoot)),
            Err(_) => Ok(B256::ZERO), // unused; `drive?` below returns first
        };
        (drive, root)
    };
    let shutdown = handle.shutdown();

    drive?;
    shutdown?;
    let state_root = root?;

    Ok(ReplayOutcome {
        head_block: counters.head,
        blocks_applied: counters.blocks_applied,
        txs_applied: counters.txs_applied,
        state_root,
    })
}

/// Inner loop: execute each block's txs, submit the block delta, wait for the
/// durable commit. Borrows the adapters so the caller can always tear the
/// writer down afterwards regardless of outcome.
fn drive_blocks<I>(
    queue: &mut MdbxWriterQueue,
    signal: &mut MdbxWriterSignal,
    source: &MdbxSnapshotSource,
    chain_id: u64,
    blocks: I,
    counters: &mut Counters,
) -> Result<(), ReplayError>
where
    I: IntoIterator<Item = ReplayBlock>,
{
    for block in blocks {
        // Snapshot after the previously-committed block (genesis before the
        // first). `wait_committed` below keeps the published snapshot anchored
        // at `counters.head`, so this is exactly the pre-block state view.
        let snapshot = source.snapshot_after(counters.head);
        let exec_env = ExecEnv {
            chain_id,
            block_number: block.block_number,
            l2_timestamp: block.l2_timestamp,
        };

        let mut delta = PendingDelta::new();
        let mut block_receipts = Vec::with_capacity(block.txs.len());
        let mut cumulative_gas = 0u64;
        for (i, tx) in block.txs.iter().enumerate() {
            let tx_position = BPosition::from_index(counters.global_pos);
            // Replay executes one durably-committed block at a time against
            // its own committed snapshot — no pipelined parent layer.
            let (receipt, ws) = execute_tx(
                &snapshot,
                None,
                &delta,
                exec_env,
                TxIndex(counters.tx_idx),
                tx_position,
                tx,
                i as u64,
                cumulative_gas,
                None,
            )?;
            delta.apply(ws);
            cumulative_gas += receipt.gas_used;
            block_receipts.push(receipt);
            counters.tx_idx += 1;
            counters.global_pos += 1;
            counters.txs_applied += 1;
        }

        let block_delta = delta.finalize(block.block_number, block_receipts);
        let boundary = BlockBoundary {
            block_number: block.block_number,
            end_tx_idx: BPosition::from_index(counters.global_pos),
            l2_timestamp: block.l2_timestamp,
            // Offline replay reconstructs from the DA payload, which does not
            // carry the origin yet (KAR2 adds it — see the deposit-derivation
            // spec). Zero here is honest: reconstruction currently derives no
            // deposits, so no block it builds has an epoch to point at.
            l1_origin: 0,
        };
        queue.submit(boundary, block_delta)?;
        signal.wait_committed(block.block_number)?;
        counters.head = block.block_number;
        counters.blocks_applied += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{
        Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
    };
    use alloy_signer_local::PrivateKeySigner;
    use bytes::Bytes;
    use kardamom_state::{Durability, StateEnvBuilder, StateSnapshot, empty_root};
    use kardamom_types::StateDatabase;
    use revm::primitives::KECCAK_EMPTY;

    const CHAIN_ID: u64 = 1;

    /// Build a signed legacy transfer wrapped as a `kardamom_types::TxEnvelope`,
    /// exactly as the proxy would hand it downstream (sender + tx_hash stamped).
    fn transfer(signer: &PrivateKeySigner, to: Address, nonce: u64, value: u64) -> TxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(CHAIN_ID),
            nonce,
            gas_price: 0,
            gas_limit: 21_000,
            to: APTxKind::Call(to),
            value: U256::from(value),
            input: AlloyBytes::new(),
        };
        let sig = signer.sign_transaction_sync(&mut tx).unwrap();
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        TxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
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
                txs: vec![transfer(signer, to1, 0, 100), transfer(signer, to2, 1, 50)],
            },
            ReplayBlock {
                block_number: 2,
                l2_timestamp: 1_700_000_001,
                txs: vec![transfer(signer, to1, 2, 25)],
            },
        ]
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

        // Balances reflect the exact transfers (gas_price 0 ⇒ no fee burn).
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

        // Same inputs ⇒ byte-identical reconstructed root. This is the property
        // the DA round-trip relies on.
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
