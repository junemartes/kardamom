//! mdbx read helpers for the incremental trie: fetch stored branch nodes by
//! path, and collect hashed leaves under a nibble prefix in ascending key order.
//!
//! Generic over the transaction kind (`Tx<K>`) so the same code serves the
//! writer's RW txn (reads must see the just-written hashed-mirror rows) and
//! read-only callers. Keys for stored nodes are the **unpacked** nibble path
//! (one nibble per byte) so lexicographic mdbx order == trie order and no two
//! distinct paths collide; storage tries prepend the 32-byte `account_hash`.

use alloy_primitives::{B256, U256};
use alloy_trie::{BranchNodeCompact, Nibbles};
use signet_libmdbx::Database;
use signet_libmdbx::tx::aliases::RwTxSync;

use super::AccountTrieParts;
use super::node::decode_branch_node;
use crate::error::StateError;

const HASHED_ACCT: &str = "hashed_accounts";
const HASHED_STOR: &str = "hashed_storage";

/// Stored-node key = `[account_hash(32)?] ++ nibble_path(one byte per nibble)`.
fn node_key(account_hash: Option<&B256>, path: &Nibbles) -> Vec<u8> {
    let nibs = path.to_vec();
    match account_hash {
        Some(a) => {
            let mut k = Vec::with_capacity(32 + nibs.len());
            k.extend_from_slice(a.as_slice());
            k.extend_from_slice(&nibs);
            k
        }
        None => nibs,
    }
}

/// Fetch the stored branch node at `path` (exact), if present.
pub(crate) fn get_branch_node(
    tx: &RwTxSync,
    db: Database,
    account_hash: Option<&B256>,
    path: &Nibbles,
) -> Result<Option<BranchNodeCompact>, StateError> {
    match tx.get::<Vec<u8>>(db.dbi(), &node_key(account_hash, path))? {
        Some(bytes) => Ok(Some(decode_branch_node(&bytes)?)),
        None => Ok(None),
    }
}

/// Decode a `hashed_accounts` value (`nonce ++ balance ++ code_hash ++ storage_root`).
fn decode_account_leaf(b: &[u8]) -> Result<AccountTrieParts, StateError> {
    if b.len() != 104 {
        return Err(StateError::BadEncoding {
            table: HASHED_ACCT,
            expected: 104,
            got: b.len(),
        });
    }
    let nonce = u64::from_be_bytes(b[0..8].try_into().unwrap());
    let balance = U256::from_be_slice(&b[8..40]);
    let code_hash = B256::from_slice(&b[40..72]);
    let storage_root = B256::from_slice(&b[72..104]);
    Ok(AccountTrieParts {
        nonce,
        balance,
        code_hash,
        storage_root,
    })
}

/// Exact `hashed_accounts` lookup by `keccak(addr)`.
pub(crate) fn get_hashed_account(
    tx: &RwTxSync,
    db: Database,
    account_hash: &B256,
) -> Result<Option<AccountTrieParts>, StateError> {
    match tx.get::<Vec<u8>>(db.dbi(), account_hash.as_slice())? {
        Some(b) => Ok(Some(decode_account_leaf(&b)?)),
        None => Ok(None),
    }
}

/// Encode a `hashed_accounts` value.
pub(crate) fn encode_account_leaf(p: &AccountTrieParts) -> [u8; 104] {
    let mut out = [0u8; 104];
    out[0..8].copy_from_slice(&p.nonce.to_be_bytes());
    out[8..40].copy_from_slice(&p.balance.to_be_bytes::<32>());
    out[40..72].copy_from_slice(p.code_hash.as_slice());
    out[72..104].copy_from_slice(p.storage_root.as_slice());
    out
}

/// The 32-byte key whose first `prefix.len()` nibbles equal `prefix`, rest zero —
/// the cursor seek start for "leaves under prefix".
fn padded_key(prefix: &Nibbles) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, n) in prefix.to_vec().into_iter().enumerate() {
        if i >= 64 {
            break;
        }
        if i % 2 == 0 {
            out[i / 2] |= n << 4;
        } else {
            out[i / 2] |= n;
        }
    }
    out
}

/// True iff the 32-byte hashed key's first `prefix.len()` nibbles equal `prefix`.
fn key_starts_with(key: &B256, prefix: &Nibbles) -> bool {
    Nibbles::unpack(key.as_slice()).starts_with(prefix)
}

/// Collect `(keccak(addr), AccountTrieParts)` for every hashed account whose key
/// is under `prefix`, in ascending key order.
pub(crate) fn collect_hashed_accounts_under(
    tx: &RwTxSync,
    db: Database,
    prefix: &Nibbles,
) -> Result<Vec<(B256, AccountTrieParts)>, StateError> {
    let start = padded_key(prefix);
    let mut out = Vec::new();
    let mut cur = tx.cursor(db)?;
    let mut item = cur.set_range::<Vec<u8>, Vec<u8>>(&start)?;
    while let Some((k, v)) = item {
        if k.len() != 32 {
            break;
        }
        let key = B256::from_slice(&k);
        if !key_starts_with(&key, prefix) {
            break;
        }
        out.push((key, decode_account_leaf(&v)?));
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    Ok(out)
}

/// Collect `(keccak(slot), value)` for one account's hashed storage under
/// `prefix`, ascending. Keys in `hashed_storage` are `account_hash ++ keccak(slot)`.
pub(crate) fn collect_hashed_storage_under(
    tx: &RwTxSync,
    db: Database,
    account_hash: &B256,
    prefix: &Nibbles,
) -> Result<Vec<(B256, U256)>, StateError> {
    let mut start = Vec::with_capacity(64);
    start.extend_from_slice(account_hash.as_slice());
    start.extend_from_slice(&padded_key(prefix));
    let mut out = Vec::new();
    let mut cur = tx.cursor(db)?;
    let mut item = cur.set_range::<Vec<u8>, Vec<u8>>(&start)?;
    while let Some((k, v)) = item {
        if k.len() != 64 || &k[0..32] != account_hash.as_slice() {
            break;
        }
        let slot_hash = B256::from_slice(&k[32..64]);
        if !key_starts_with(&slot_hash, prefix) {
            break;
        }
        if v.len() != 32 {
            return Err(StateError::BadEncoding {
                table: HASHED_STOR,
                expected: 32,
                got: v.len(),
            });
        }
        out.push((slot_hash, U256::from_be_slice(&v)));
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    Ok(out)
}
