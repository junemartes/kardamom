//! Shared test helpers. These open an env and a writer, and build deltas.
//!
//! Each integration test imports this module with `mod common;`. Each
//! test binary uses only a subset of the helpers, so
//! `#![allow(dead_code)]` keeps clippy quiet.

#![allow(
    dead_code,
    reason = "each of this module's several integration-test and bench binaries uses only a \
              subset of these helpers; the rest are live in at least one other binary"
)]

use std::ops::RangeInclusive;
use std::path::Path;

use alloy_primitives::{Address, B256, U256};
use kardamom_state::env::Durability;
use kardamom_state::{
    StateEnv, StateEnvBuilder, StateSnapshot, StateWriter, WriteBatch, WriterHandle,
};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta, Receipt, StorageChange};

/// Open a fresh tempdir-backed env. Returns the tempdir guard, so the
/// caller controls its lifetime.
pub(crate) fn temp_env() -> (tempfile::TempDir, StateEnv) {
    let dir = tempfile::tempdir().unwrap();
    let env = open_env(dir.path());
    (dir, env)
}

/// Open an env at an existing path: a tempdir kept alive across a
/// close-and-reopen test, or any other directory a caller already holds.
pub(crate) fn open_env(dir: &Path) -> StateEnv {
    StateEnvBuilder::new(dir)
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap()
}

/// Spawn a fresh writer over a tempdir-backed env. Returns the tempdir
/// guard, so the caller controls its lifetime. Drop it after the writer.
pub(crate) fn open_tmp_writer() -> (tempfile::TempDir, WriterHandle) {
    let (dir, env) = temp_env();
    let writer = StateWriter::spawn(env).unwrap();
    (dir, writer)
}

pub(crate) fn bpos(block: u64) -> BPosition {
    let term_offset = i32::try_from(
        block
            .checked_mul(1024)
            .expect("test block numbers are far under u64::MAX / 1024"),
    )
    .expect("test block numbers fit i32 after the *1024 offset");
    BPosition {
        term_id: 0,
        term_offset,
    }
}

/// Build a simple per-block delta: one account mutation, one storage
/// slot, and one receipt with a deterministic `tx_hash`.
///
/// The upstream `AccountChange` shape is `{ address, nonce, balance,
/// code_hash }`, with no `storage_root` field. The v0 executor does not
/// maintain MPT roots.
pub(crate) fn simple_delta(
    block: u64,
    addr: Address,
    balance: u64,
    slot_idx: u64,
    slot_value: u64,
) -> WriteBatch {
    let end_pos = bpos(block);
    // Deterministic per-block tx_hash so tx_hash_index tests can look it up.
    let mut hash_bytes = [0u8; 32];
    hash_bytes[24..].copy_from_slice(&block.to_be_bytes());
    let tx_hash = B256::from(hash_bytes);
    let mut slot_bytes = [0u8; 32];
    slot_bytes[24..].copy_from_slice(&slot_idx.to_be_bytes());
    let slot_key = B256::from(slot_bytes);

    let boundary = BlockBoundary {
        block_number: block,
        end_tx_idx: end_pos,
        l2_timestamp: 1_700_000_000 + block,
        l1_origin: 0,
    };
    let delta = BlockDelta {
        block_number: block,
        accounts: vec![AccountChange {
            address: addr,
            nonce: block,
            balance: U256::from(balance),
            code_hash: B256::ZERO,
        }],
        storage: vec![StorageChange {
            address: addr,
            key: slot_key,
            value: U256::from(slot_value),
        }],
        code: Vec::new(),
        receipts: vec![Receipt {
            tx_idx: end_pos,
            tx_hash,
            status: true,
            gas_used: 21_000,
            logs: vec![],
            write_set_hash: B256::ZERO,
            ..Default::default()
        }],
    };
    WriteBatch::new(boundary, delta)
}

/// Send one [`simple_delta`] block and drain the snapshot it produces.
/// This is every test's "commit a block, and get its post-state view"
/// step, over `WriteBatch::send` plus `snapshot_rx.recv`.
pub(crate) fn commit_block(
    w: &WriterHandle,
    block: u64,
    addr: Address,
    balance: u64,
    slot_idx: u64,
    slot_value: u64,
) -> StateSnapshot {
    w.delta_tx
        .send(simple_delta(block, addr, balance, slot_idx, slot_value))
        .unwrap();
    w.snapshot_rx.recv().unwrap()
}

/// Commit every block in `blocks`, each with balance `1000 + block` and
/// slot 7 set to `block * 100` — [`commit_block`]'s deterministic
/// per-block values, the shape most range-of-blocks tests want. Returns
/// the last block's snapshot.
///
/// # Panics
///
/// Panics if `blocks` is empty.
pub(crate) fn commit_range(
    w: &WriterHandle,
    blocks: RangeInclusive<u64>,
    addr: Address,
) -> StateSnapshot {
    let mut last = None;
    for block in blocks {
        last = Some(commit_block(w, block, addr, 1000 + block, 7, block * 100));
    }
    last.expect("commit_range requires a non-empty range")
}

/// Produce the B256 storage-slot key for slot index `idx` matching
/// [`simple_delta`].
pub(crate) fn slot_key(idx: u64) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&idx.to_be_bytes());
    B256::from(bytes)
}
