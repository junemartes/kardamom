//! Cold-start recovery (spec section 5).
//!
//! On startup, the writer does this:
//!
//! 1. Open, or create, the env (`StateEnvBuilder::open`).
//! 2. Read the meta cursors with [`read_recovery_point`].
//! 3. Open an initial snapshot.
//! 4. Emit a [`RecoveryPoint`] that tells the executor where to resume
//!    reading B from.
//!
//! Recovery itself is read-only. No replay logic lives in this crate. The
//! executor reads B starting at `recovery_point.last_fsynced_b_position`,
//! and re-derives any blocks the writer never committed.

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

/// Cursors read from the `meta` table at startup. The writer uses these to
/// give the executor a resume point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryPoint {
    pub last_committed_block: u64,
    pub last_committed_end_tx_position: BPosition,
    pub last_fsynced_b_position: BPosition,
    /// The committed block's boundary `l2_timestamp`, from its `headers` row.
    ///
    /// On a resume from cursor, the executor thread seeds its
    /// block-timestamp state from this value. Block N+1's transactions
    /// execute with boundary N's timestamp, and a resumed replica no
    /// longer sees boundary N. Deriving the timestamp any other way would
    /// diverge from replicas that never restarted.
    ///
    /// This is 0 when nothing is committed yet, on a fresh DB or a
    /// genesis-only DB.
    pub last_committed_l2_timestamp: u64,
}

/// # Errors
///
/// Returns [`StateError::Recovery`] if the meta cursors say a block is
/// committed but its header row is missing, and [`StateError`] if the
/// read transaction or a table read fails.
pub fn read_recovery_point(env: &StateEnv) -> Result<RecoveryPoint, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;

    // An absent cursor means genesis: nothing has committed or fsynced
    // yet, so the zero defaults below are correct recovery points, not
    // missing-data sentinels.
    let last_committed_block = read_meta_u64(&txn, meta, KEY_LAST_COMMITTED_BLOCK)?.unwrap_or(0);
    let last_committed_end_tx_position =
        read_meta_b_position(&txn, meta, KEY_LAST_COMMITTED_END_TX_POSITION)?
            .unwrap_or(BPosition::ZERO);
    let last_fsynced_b_position =
        read_meta_b_position(&txn, meta, KEY_LAST_FSYNCED_B_POSITION)?.unwrap_or(BPosition::ZERO);
    let last_committed_l2_timestamp = if last_committed_block > 0 {
        read_committed_l2_timestamp(&txn, last_committed_block)?
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

/// The L2 timestamp of block `block`, read from its header row.
///
/// The committed block's header row is written in the same transaction as
/// the meta cursors. So a present cursor with an absent header means a
/// corrupt env. This reports that error instead of defaulting: a wrong
/// timestamp would silently diverge state.
///
/// # Errors
///
/// Returns [`StateError::Recovery`] if the header row is missing, and
/// [`StateError`] if the table read or the decode fails.
fn read_committed_l2_timestamp<K: signet_libmdbx::TransactionKind>(
    txn: &signet_libmdbx::tx::Tx<K>,
    block: u64,
) -> Result<u64, StateError> {
    let headers = txn.open_db(Some(TABLE_HEADERS))?;
    let Some(b) = txn.get::<Vec<u8>>(headers.dbi(), &encode_block_key(block))? else {
        return Err(StateError::Recovery(format!(
            "meta cursor says block {block} committed but its headers row is missing"
        )));
    };
    Ok(decode_header_value(&b)?.l2_timestamp)
}

/// Whether the env carries a populated trie: a hashed mirror plus stored
/// nodes.
///
/// An executor checkpoint has neither, because its writer runs with the
/// trie off. So a validator that adopts one must call
/// [`bootstrap_trie_from_state`] before it spawns its trie-aware writer.
///
/// This checks whether `hashed_accounts` is non-empty, or the `accounts`
/// table is empty. An empty env legitimately has no trie yet; genesis
/// seeding builds it.
///
/// # Errors
///
/// Returns [`StateError`] if the read transaction or a table read fails.
pub fn has_trie(env: &StateEnv) -> Result<bool, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let accounts_db = txn.open_db(Some(crate::schema::TABLE_ACCOUNTS))?;
    let mut cur = txn.cursor(accounts_db)?;
    if cur.first::<Vec<u8>, Vec<u8>>()?.is_none() {
        // Nothing to mirror yet. Genesis seeding will build the trie.
        return Ok(true);
    }
    let hashed_db = txn.open_db(Some(crate::schema::TABLE_HASHED_ACCOUNTS))?;
    let mut cur = txn.cursor(hashed_db)?;
    Ok(cur.first::<Vec<u8>, Vec<u8>>()?.is_some())
}

/// Build the hashed-state mirror and the account and storage tries from
/// the plain state tables. Returns the world-state root.
///
/// This is the one-time adoption step for a state image produced by a
/// trie-off writer, such as an executor checkpoint fetched by the
/// validator's replay-unavailable fallback.
///
/// This is the genesis seeding path, generalized. Every account, storage
/// slot, and code entry is folded into one synthetic [`BlockDelta`] and
/// passed through [`crate::trie::update_for_block`]. This is the same
/// function that maintains the trie incrementally, so the resulting root
/// is byte-identical to one grown block by block. The
/// `incremental_equals_full_rebuild` test pins this equivalence.
///
/// This runs in one read-write transaction. It is crash-safe: a torn
/// bootstrap aborts entirely and reruns on the next start. It is
/// idempotent: rerunning it on a populated mirror upserts the same rows.
///
/// # Errors
///
/// Returns [`StateError`] if the transaction, a table read, or the trie
/// update fails.
pub fn bootstrap_trie_from_state(env: &StateEnv) -> Result<alloy_primitives::B256, StateError> {
    let txn = env.raw().begin_rw_sync()?;

    let bootstrap = TrieBootstrap::open(&txn)?;
    let accounts = bootstrap.read_all_accounts()?;
    let storage = bootstrap.read_all_storage()?;
    let code = bootstrap.read_all_code()?;

    let delta = kardamom_types::BlockDelta {
        block_number: 0,
        accounts,
        storage,
        code,
        receipts: Vec::new(),
    };
    let root = crate::trie::commit_trie_root(&txn, &delta)?;
    txn.commit()?;
    Ok(root)
}

/// The plain-state tables [`bootstrap_trie_from_state`] reads from, opened
/// once on one read-write transaction.
struct TrieBootstrap<'a> {
    txn: &'a signet_libmdbx::tx::aliases::RwTxSync,
    accounts_db: signet_libmdbx::Database,
    storage_db: signet_libmdbx::Database,
    code_db: signet_libmdbx::Database,
}

impl<'a> TrieBootstrap<'a> {
    fn open(txn: &'a signet_libmdbx::tx::aliases::RwTxSync) -> Result<Self, StateError> {
        use crate::schema::{TABLE_ACCOUNTS, TABLE_CODE, TABLE_STORAGE};
        Ok(Self {
            accounts_db: txn.open_db(Some(TABLE_ACCOUNTS))?,
            storage_db: txn.open_db(Some(TABLE_STORAGE))?,
            code_db: txn.open_db(Some(TABLE_CODE))?,
            txn,
        })
    }

    /// Walk `db`, decoding each row with `decode`. This is the one shared
    /// "walk a table, decode each row, push" body behind
    /// `read_all_accounts`/`read_all_storage`/`read_all_code`.
    fn collect_rows<T>(
        &self,
        db: signet_libmdbx::Database,
        mut decode: impl FnMut(&[u8], &[u8]) -> Result<T, StateError>,
    ) -> Result<Vec<T>, StateError> {
        let mut out = Vec::new();
        for_each_row(self.txn, db, |k, v| {
            out.push(decode(&k, &v)?);
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(out)
    }

    /// Read every row of `accounts` into the `AccountChange` shape
    /// [`kardamom_types::BlockDelta`] carries.
    fn read_all_accounts(&self) -> Result<Vec<kardamom_types::AccountChange>, StateError> {
        use crate::schema::decode_account_value;
        use alloy_primitives::Address;

        self.collect_rows(self.accounts_db, |k, v| {
            if k.len() != 20 {
                return Err(StateError::Recovery(format!(
                    "accounts key of length {} during trie bootstrap",
                    k.len()
                )));
            }
            let a = decode_account_value(v)?;
            Ok(kardamom_types::AccountChange {
                address: Address::from_slice(k),
                nonce: a.nonce,
                balance: a.balance,
                code_hash: a.code_hash,
            })
        })
    }

    /// Read every row of `storage` into the `StorageChange` shape
    /// [`kardamom_types::BlockDelta`] carries.
    fn read_all_storage(&self) -> Result<Vec<kardamom_types::StorageChange>, StateError> {
        use crate::schema::decode_storage_value;
        use alloy_primitives::{Address, B256, U256};

        self.collect_rows(self.storage_db, |k, v| {
            if k.len() != 52 {
                return Err(StateError::Recovery(format!(
                    "storage key of length {} during trie bootstrap",
                    k.len()
                )));
            }
            let value: U256 = decode_storage_value(v)?;
            Ok(kardamom_types::StorageChange {
                address: Address::from_slice(&k[..20]),
                key: B256::from_slice(&k[20..]),
                value,
            })
        })
    }

    /// Read every row of `code` into the `CodeEntry` shape
    /// [`kardamom_types::BlockDelta`] carries.
    fn read_all_code(&self) -> Result<Vec<kardamom_types::CodeEntry>, StateError> {
        use alloy_primitives::B256;

        self.collect_rows(self.code_db, |k, v| {
            Ok(kardamom_types::CodeEntry {
                code_hash: B256::from_slice(k),
                code: bytes::Bytes::copy_from_slice(v),
            })
        })
    }
}

/// Every persisted block header, in block order.
///
/// This is a read-only scan of `headers`. Verification tooling and the
/// chain-semantics suite use it to check properties that span the whole
/// chain, such as the L1-origin sequence or boundary alignment, rather
/// than a single block. Nothing serves headers over RPC, so this is the
/// only way to observe them.
///
/// # Errors
///
/// Returns [`StateError::BadEncoding`] if a stored key or value is not
/// the header table's fixed width, and [`StateError`] if the read
/// transaction fails.
pub fn read_all_headers(env: &StateEnv) -> Result<Vec<(u64, HeaderValue)>, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let headers = txn.open_db(Some(TABLE_HEADERS))?;
    let mut out = Vec::new();
    // Keys are block numbers in big-endian order, so mdbx's byte order is
    // block order.
    for_each_row(&txn, headers, |k, v| {
        let block_number = u64::from_be_bytes(*crate::meta::fixed::<8>(TABLE_HEADERS, &k)?);
        out.push((block_number, decode_header_value(&v)?));
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(out)
}

#[cfg(test)]
#[path = "tests.rs"]
mod trie_bootstrap_tests;
