//! Cold-start recovery (§5).
//!
//! On startup the writer:
//! 1. opens (or creates) the env (`StateEnvBuilder::open`),
//! 2. reads the meta cursors via [`read_recovery_point`],
//! 3. opens an initial snapshot,
//! 4. emits a [`RecoveryPoint`] that tells the executor where to resume
//!    reading B from.
//!
//! Recovery itself is read-only — no replay logic lives in this crate; the
//! executor consumes B from `recovery_point.last_fsynced_b_position` and
//! re-derives any blocks the writer never got to commit.

use kardamom_types::BPosition;

use std::ops::ControlFlow;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION,
    read_meta_b_position, read_meta_u64,
};
use crate::schema::{
    HeaderValue, TABLE_HEADERS, TABLE_META, decode_header_value, encode_block_key, for_each_row,
};

/// Cursors read out of the `meta` table at startup. The writer uses this to
/// hand the executor a resume point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    pub last_committed_block: u64,
    pub last_committed_end_tx_position: BPosition,
    pub last_fsynced_b_position: BPosition,
    /// The committed block's boundary `l2_timestamp` (from its `headers` row).
    /// The exec thread seeds its block-timestamp state from this on a
    /// resume-from-cursor: block N+1's txs execute with boundary N's timestamp,
    /// and a resumed replica no longer sees boundary N — deriving it from
    /// anything else would diverge from the replicas that never restarted.
    /// 0 when nothing is committed yet (fresh DB / genesis-only).
    pub last_committed_l2_timestamp: u64,
}

pub fn read_recovery_point(env: &StateEnv) -> Result<RecoveryPoint, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;

    let last_committed_block = read_meta_u64(&txn, meta, KEY_LAST_COMMITTED_BLOCK)?.unwrap_or(0);
    let last_committed_end_tx_position =
        read_meta_b_position(&txn, meta, KEY_LAST_COMMITTED_END_TX_POSITION)?
            .unwrap_or(BPosition::ZERO);
    let last_fsynced_b_position =
        read_meta_b_position(&txn, meta, KEY_LAST_FSYNCED_B_POSITION)?.unwrap_or(BPosition::ZERO);
    // The committed block's header row is written in the same txn as the meta
    // cursors, so present-cursor/absent-header means a corrupt env — surface
    // it rather than defaulting (a wrong timestamp silently diverges state).
    let last_committed_l2_timestamp = if last_committed_block > 0 {
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        match txn.get::<Vec<u8>>(headers.dbi(), &encode_block_key(last_committed_block))? {
            Some(b) => decode_header_value(&b)?.l2_timestamp,
            None => {
                return Err(StateError::Recovery(format!(
                    "meta cursor says block {last_committed_block} committed but its headers row is missing"
                )));
            }
        }
    } else {
        0
    };

    Ok(RecoveryPoint {
        last_committed_block,
        last_committed_end_tx_position,
        last_fsynced_b_position,
        last_committed_l2_timestamp,
    })
}

/// Whether the env carries a populated trie (hashed mirror + stored nodes).
/// An EXECUTOR checkpoint has neither — its writer runs trie-off — so a
/// validator adopting one must [`bootstrap_trie_from_state`] before spawning
/// its trie-aware writer (issue #143). Probed as "hashed_accounts non-empty
/// OR accounts table empty" (an empty env legitimately has no trie yet;
/// genesis seeding builds it).
pub fn has_trie(env: &StateEnv) -> Result<bool, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let accounts_db = txn.open_db(Some(crate::schema::TABLE_ACCOUNTS))?;
    let mut cur = txn.cursor(accounts_db)?;
    if cur.first::<Vec<u8>, Vec<u8>>()?.is_none() {
        return Ok(true); // nothing to mirror; genesis seed will build it
    }
    let hashed_db = txn.open_db(Some(crate::schema::TABLE_HASHED_ACCOUNTS))?;
    let mut cur = txn.cursor(hashed_db)?;
    Ok(cur.first::<Vec<u8>, Vec<u8>>()?.is_some())
}

/// Build the hashed-state mirror + account/storage tries from the PLAIN state
/// tables, returning the world-state root — the one-time adoption step for a
/// state image produced by a trie-off writer (an executor checkpoint fetched
/// by the validator's replay-unavailable fallback, issue #143).
///
/// Mechanically this is the genesis seeding path generalized: every account,
/// storage slot and code entry is folded into one synthetic [`BlockDelta`]
/// and pushed through [`crate::trie::update_for_block`] — the SAME function
/// that maintains the trie incrementally, so the resulting root is
/// byte-identical to one grown block-by-block (pinned by the
/// `incremental_equals_full_rebuild` equivalence test). Runs in one RW txn:
/// crash-safe (a torn bootstrap aborts wholesale and reruns on next start),
/// idempotent (rerunning on a populated mirror upserts the same rows).
pub fn bootstrap_trie_from_state(env: &StateEnv) -> Result<alloy_primitives::B256, StateError> {
    use crate::schema::{
        TABLE_ACCOUNTS, TABLE_CODE, TABLE_STORAGE, decode_account_value, decode_storage_value,
    };
    use alloy_primitives::{Address, B256, U256};
    use signet_libmdbx::WriteFlags;

    let txn = env.raw().begin_rw_sync()?;

    let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS))?;
    let mut accounts = Vec::new();
    for_each_row(&txn, accounts_db, |k, v| {
        if k.len() != 20 {
            return Err(StateError::Recovery(format!(
                "accounts key of length {} during trie bootstrap",
                k.len()
            )));
        }
        let a = decode_account_value(&v)?;
        accounts.push(kardamom_types::AccountChange {
            address: Address::from_slice(&k),
            nonce: a.nonce,
            balance: a.balance,
            code_hash: a.code_hash,
        });
        Ok(ControlFlow::Continue(()))
    })?;

    let storage_db = txn.open_db(Some(TABLE_STORAGE))?;
    let mut storage = Vec::new();
    for_each_row(&txn, storage_db, |k, v| {
        if k.len() != 52 {
            return Err(StateError::Recovery(format!(
                "storage key of length {} during trie bootstrap",
                k.len()
            )));
        }
        let value: U256 = decode_storage_value(&v)?;
        storage.push(kardamom_types::StorageChange {
            address: Address::from_slice(&k[..20]),
            key: B256::from_slice(&k[20..]),
            value,
        });
        Ok(ControlFlow::Continue(()))
    })?;

    let code_db = txn.open_db(Some(TABLE_CODE))?;
    let mut code = Vec::new();
    for_each_row(&txn, code_db, |k, v| {
        code.push(kardamom_types::CodeEntry {
            code_hash: B256::from_slice(&k),
            code: v.into(),
        });
        Ok(ControlFlow::Continue(()))
    })?;

    let delta = kardamom_types::BlockDelta {
        block_number: 0,
        accounts,
        storage,
        code,
        receipts: Vec::new(),
    };
    let trie_tables = crate::trie::TrieTables::open(&txn)?;
    let root = crate::trie::update_for_block(&txn, &trie_tables, &delta)?;
    let meta = txn.open_db(Some(TABLE_META))?;
    txn.put(
        meta,
        crate::meta::KEY_STATE_ROOT,
        crate::meta::encode_b256(root),
        WriteFlags::UPSERT,
    )?;
    txn.commit()?;
    Ok(root)
}

/// Every persisted block header, in block order.
///
/// Read-only scan of `headers`; used by verification tooling and the
/// chain-semantics suite to assert properties that span the whole chain (the
/// L1-origin sequence, boundary alignment) rather than a single block.
/// Nothing serves headers over RPC, so this is the only way to observe them.
pub fn read_all_headers(env: &StateEnv) -> Result<Vec<(u64, HeaderValue)>, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let headers = txn.open_db(Some(TABLE_HEADERS))?;
    let mut out = Vec::new();
    // Keys are block numbers big-endian, so mdbx's byte order IS block order.
    for_each_row(&txn, headers, |k, v| {
        if k.len() != 8 {
            return Err(StateError::BadEncoding {
                table: TABLE_HEADERS,
                expected: 8,
                got: k.len(),
            });
        }
        let block_number = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
        out.push((block_number, decode_header_value(&v)?));
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(out)
}

#[cfg(test)]
#[path = "tests.rs"]
mod trie_bootstrap_tests;
