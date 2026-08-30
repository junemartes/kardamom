//! The per-table checks that make up [`sweep`](super::sweep).
//!
//! Each check walks one table, or one coherent group for the receipt
//! index. It appends findings to the shared [`IntegrityReport`], and
//! increases the report's row counts. `sweep` calls these checks in a
//! fixed order. Within each check, the iteration order matches the
//! table's key order.

use std::ops::ControlFlow;

use alloy_primitives::B256;
use alloy_trie::KECCAK_EMPTY;
use kardamom_types::BPosition;

use signet_libmdbx::Database;
use signet_libmdbx::tx::aliases::RwTxSync;

use crate::error::StateError;
use crate::meta::{
    KEY_GENESIS_APPLIED, KEY_GENESIS_DIGEST, KEY_LAST_COMMITTED_BLOCK,
    KEY_LAST_COMMITTED_END_TX_POSITION, KEY_SCHEMA_VERSION, KEY_STATE_ROOT, SCHEMA_VERSION,
    decode_b_position, decode_b256, decode_u32, decode_u64,
};
use crate::schema::{
    TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_RECEIPTS, TABLE_STORAGE, TABLE_TX_HASH_INDEX,
    decode_account_value, decode_header_value, decode_receipt_value, decode_storage_value,
    decode_tx_hash_value, encode_code_key, encode_tx_hash_key, for_each_row,
};
use crate::trie::{TrieTables, rebuild_root};

use super::IntegrityReport;

fn get_meta(txn: &RwTxSync, meta: Database, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
    Ok(txn.get::<Vec<u8>>(meta.dbi(), key)?)
}

fn problem(p: String, r: &mut IntegrityReport) {
    r.problems.push(p);
}

/// Checks meta: schema version, genesis, and cursors.
///
/// Returns the decoded `last_committed_end_tx_position` cursor. The
/// headers and receipts checks cross-reference this value.
pub(super) fn check_meta(
    txn: &RwTxSync,
    meta: Database,
    r: &mut IntegrityReport,
) -> Result<Option<BPosition>, StateError> {
    match get_meta(txn, meta, KEY_SCHEMA_VERSION)? {
        Some(b) => match decode_u32(&b) {
            Ok(v) if v == SCHEMA_VERSION => {}
            Ok(v) => problem(
                format!("schema_version {v} != expected {SCHEMA_VERSION}"),
                r,
            ),
            Err(e) => problem(format!("schema_version undecodable: {e}"), r),
        },
        None => problem("schema_version missing".into(), r),
    }
    let genesis_applied = get_meta(txn, meta, KEY_GENESIS_APPLIED)?.is_some();
    if !genesis_applied {
        problem("genesis_applied flag missing (DB never seeded)".into(), r);
    } else if get_meta(txn, meta, KEY_GENESIS_DIGEST)?.is_none() {
        problem("genesis seeded but genesis_digest missing".into(), r);
    }
    r.last_committed_block = match get_meta(txn, meta, KEY_LAST_COMMITTED_BLOCK)? {
        Some(b) => decode_u64(&b).unwrap_or_else(|e| {
            r.problems
                .push(format!("last_committed_block undecodable: {e}"));
            0
        }),
        None => 0,
    };
    let meta_end_tx = match get_meta(txn, meta, KEY_LAST_COMMITTED_END_TX_POSITION)? {
        Some(b) => match decode_b_position(&b) {
            Ok(p) => Some(p),
            Err(e) => {
                problem(
                    format!("last_committed_end_tx_position undecodable: {e}"),
                    r,
                );
                None
            }
        },
        None => None,
    };
    Ok(meta_end_tx)
}

/// Checks headers: every row decodes, and keys are dense up to the meta
/// cursor.
pub(super) fn check_headers(
    txn: &RwTxSync,
    meta_end_tx: Option<BPosition>,
    r: &mut IntegrityReport,
) -> Result<(), StateError> {
    let headers_db = txn.open_db(Some(TABLE_HEADERS))?;
    let mut prev_block: Option<u64> = None;
    let mut first_block: Option<u64> = None;
    let mut last_header_end_tx = None;
    for_each_row(txn, headers_db, |k, v| {
        if k.len() != 8 {
            problem(format!("headers key of length {} (expected 8)", k.len()), r);
            return Ok(ControlFlow::Break(()));
        }
        let block = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
        match decode_header_value(&v) {
            Ok(h) => last_header_end_tx = Some(h.end_tx_idx),
            Err(e) => problem(format!("headers[{block}] undecodable: {e}"), r),
        }
        if let Some(p) = prev_block
            && block != p + 1
        {
            problem(format!("headers gap: {p} -> {block}"), r);
        }
        first_block.get_or_insert(block);
        prev_block = Some(block);
        r.headers += 1;
        Ok(ControlFlow::Continue(()))
    })?;
    if let Some(first) = first_block
        && first > 1
    {
        problem(format!("headers start at {first} (expected 0 or 1)"), r);
    }
    if let Some(last) = prev_block
        && last != r.last_committed_block
    {
        problem(
            format!(
                "last header {last} != meta last_committed_block {}",
                r.last_committed_block
            ),
            r,
        );
    }
    if r.last_committed_block > 0 && r.headers == 0 {
        problem("meta cursor set but headers table empty".into(), r);
    }
    if let (Some(h), Some(m)) = (last_header_end_tx, meta_end_tx)
        && h != m
    {
        problem(
            format!("last header end_tx_idx {h:?} != meta cursor {m:?}"),
            r,
        );
    }
    Ok(())
}

/// Checks receipts: every row decodes, and the index round-trips both ways.
pub(super) fn check_receipts_index(
    txn: &RwTxSync,
    meta_end_tx: Option<BPosition>,
    r: &mut IntegrityReport,
) -> Result<(), StateError> {
    let receipts_db = txn.open_db(Some(TABLE_RECEIPTS))?;
    let tx_hash_db = txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
    for_each_row(txn, receipts_db, |k, v| {
        match (decode_b_position(&k), decode_receipt_value(&v)) {
            (Ok(pos), Ok(receipt)) => {
                if receipt.tx_idx != pos {
                    problem(
                        format!("receipts[{pos:?}] carries tx_idx {:?}", receipt.tx_idx),
                        r,
                    );
                }
                // Index must map this receipt's hash back to this position.
                match txn.get::<Vec<u8>>(tx_hash_db.dbi(), &encode_tx_hash_key(receipt.tx_hash))? {
                    Some(b) => match decode_tx_hash_value(&b) {
                        Ok(p) if p == pos => {}
                        Ok(p) => problem(
                            format!(
                                "tx_hash_index[{}] -> {p:?}, receipt sits at {pos:?}",
                                receipt.tx_hash
                            ),
                            r,
                        ),
                        Err(e) => problem(format!("tx_hash_index[{}]: {e}", receipt.tx_hash), r),
                    },
                    None => problem(
                        format!("receipt {:?} missing from tx_hash_index", receipt.tx_hash),
                        r,
                    ),
                }
                if let Some(m) = meta_end_tx
                    && pos > m
                {
                    problem(format!("receipt at {pos:?} beyond meta cursor {m:?}"), r);
                }
            }
            (Err(e), _) => problem(format!("receipts key: {e}"), r),
            (_, Err(e)) => problem(format!("receipts value at {:02x?}: {e}", &k[..]), r),
        }
        r.receipts += 1;
        Ok(ControlFlow::Continue(()))
    })?;
    // Check the reverse direction too: every index entry must point at
    // an existing receipt. Counts alone would let dangling entries hide
    // behind missing ones.
    let mut index_entries = 0u64;
    for_each_row(txn, tx_hash_db, |k, v| {
        index_entries += 1;
        match decode_tx_hash_value(&v) {
            Ok(pos) => {
                if txn
                    .get::<Vec<u8>>(receipts_db.dbi(), &crate::meta::encode_b_position(pos))?
                    .is_none()
                {
                    problem(
                        format!(
                            "tx_hash_index entry {:02x?} -> {pos:?} has no receipt",
                            &k[..4]
                        ),
                        r,
                    );
                }
            }
            Err(e) => problem(format!("tx_hash_index value: {e}"), r),
        }
        Ok(ControlFlow::Continue(()))
    })?;
    if index_entries != r.receipts {
        problem(
            format!(
                "tx_hash_index has {index_entries} entries, receipts has {}",
                r.receipts
            ),
            r,
        );
    }
    Ok(())
}

/// Checks accounts: rows decode, and declared code exists.
pub(super) fn check_accounts(txn: &RwTxSync, r: &mut IntegrityReport) -> Result<(), StateError> {
    let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS))?;
    let code_db = txn.open_db(Some(TABLE_CODE))?;
    for_each_row(txn, accounts_db, |k, v| {
        match decode_account_value(&v) {
            Ok(a) => {
                if a.code_hash != B256::ZERO
                    && a.code_hash != KECCAK_EMPTY
                    && txn
                        .get::<Vec<u8>>(code_db.dbi(), &encode_code_key(a.code_hash))?
                        .is_none()
                {
                    problem(
                        format!(
                            "account {:02x?} declares missing code {}",
                            &k[..4],
                            a.code_hash
                        ),
                        r,
                    );
                }
            }
            Err(e) => problem(format!("accounts value at {:02x?}: {e}", &k[..4]), r),
        }
        r.accounts += 1;
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}

/// Checks storage: values decode.
pub(super) fn check_storage(txn: &RwTxSync, r: &mut IntegrityReport) -> Result<(), StateError> {
    let storage_db = txn.open_db(Some(TABLE_STORAGE))?;
    for_each_row(txn, storage_db, |k, v| {
        if k.len() != 52 {
            problem(
                format!("storage key of length {} (expected 52)", k.len()),
                r,
            );
        } else if let Err(e) = decode_storage_value(&v) {
            problem(format!("storage value at {:02x?}: {e}", &k[..4]), r);
        }
        r.storage_slots += 1;
        Ok(ControlFlow::Continue(()))
    })?;
    Ok(())
}

/// Checks the trie: the persisted root must reproduce from the trie tables.
pub(super) fn check_trie(
    txn: &RwTxSync,
    meta: Database,
    r: &mut IntegrityReport,
) -> Result<(), StateError> {
    r.state_root = match get_meta(txn, meta, KEY_STATE_ROOT)? {
        Some(b) => match decode_b256(&b) {
            Ok(root) => Some(root),
            Err(e) => {
                problem(format!("state_root undecodable: {e}"), r);
                None
            }
        },
        None => None, // A plain (executor) writer has no root to verify.
    };
    if let Some(stored) = r.state_root {
        let tables = TrieTables::open(txn)?;
        let rebuilt = rebuild_root(txn, &tables)?;
        r.rebuilt_root = Some(rebuilt);
        if rebuilt != stored {
            problem(
                format!("trie rebuild {rebuilt} != stored state_root {stored}"),
                r,
            );
        }
    }
    Ok(())
}
