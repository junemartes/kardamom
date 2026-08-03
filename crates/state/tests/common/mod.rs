//! Shared test helpers: open an env+writer, build deltas.
//!
//! Each integration test imports this via `mod common;`, but each test binary
//! only uses a subset of helpers — `#![allow(dead_code)]` keeps clippy quiet.

#![allow(dead_code)]

use alloy_primitives::{Address, B256, U256};
use kardamom_state::env::Durability;
use kardamom_state::{StateEnvBuilder, StateWriter, WriteBatch, WriterHandle};
use kardamom_types::{AccountChange, BPosition, BlockBoundary, BlockDelta, Receipt, StorageChange};

/// Spawn a fresh writer over a tempdir-backed env. Returns the tempdir guard
/// so the caller controls its lifetime (drop it after the writer).
pub fn open_tmp_writer() -> (tempfile::TempDir, WriterHandle) {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let writer = StateWriter::spawn(env).unwrap();
    (dir, writer)
}

pub fn bpos(block: u64) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: (block * 1024) as i32,
    }
}

/// Build a simple per-block delta: one account mutation + one storage slot +
/// one receipt with a deterministic tx_hash. Used by Tasks 17-21.
///
/// Per upstream `AccountChange` shape: `{ address, nonce, balance, code_hash }` —
/// no storage_root field (v0 executor does not maintain MPT roots per).
pub fn simple_delta(
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

/// Produce the B256 storage-slot key for slot index `idx` matching
/// [`simple_delta`].
pub fn slot_key(idx: u64) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[24..].copy_from_slice(&idx.to_be_bytes());
    B256::from(bytes)
}
