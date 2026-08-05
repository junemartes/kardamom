//! Offline integrity checks over a state DB, and a deep table-level
//! comparison between two of them.
//!
//! Built for the chain-semantics e2e suite
//! (`docs/agents/chain-semantics-e2e-suite-spec.md`, S6/S9): after a run,
//! [`sweep`] proves a single DB is internally coherent (every row decodes,
//! the header chain is dense, the receipt index is bijective, the persisted
//! trie root reproduces from the trie tables), and [`deep_compare`] proves an
//! executor DB and a validator DB hold byte-identical chain state. Both are
//! read-only in effect; they open RW transactions only because the trie
//! rebuild oracle requires one, and never write.
//!
//! Also exposed operationally via the `kardamom-statecheck` binary.

use alloy_primitives::B256;

use signet_libmdbx::Database;
use signet_libmdbx::tx::aliases::RwTxSync;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_GENESIS_APPLIED, KEY_GENESIS_DIGEST, KEY_LAST_COMMITTED_BLOCK,
    KEY_LAST_COMMITTED_END_TX_POSITION, KEY_SCHEMA_VERSION, KEY_STATE_ROOT, SCHEMA_VERSION,
    decode_b_position, decode_b256, decode_u32, decode_u64,
};
use crate::schema::{
    TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META, TABLE_RECEIPTS, TABLE_STORAGE,
    TABLE_TX_HASH_INDEX, decode_account_value, decode_header_value, decode_receipt_value,
    decode_storage_value, decode_tx_hash_value, encode_code_key, encode_tx_hash_key,
};
use crate::trie::{TrieTables, rebuild_root};

/// keccak256 of the empty byte string — the "account has no code" sentinel
/// alongside the engine's `B256::ZERO` convention.
const KECCAK_EMPTY: B256 = B256::new([
    0xc5, 0xd2, 0x46, 0x01, 0x86, 0xf7, 0x23, 0x3c, 0x92, 0x7e, 0x7d, 0xb2, 0xdc, 0xc7, 0x03, 0xc0,
    0xe5, 0x00, 0xb6, 0x53, 0xca, 0x82, 0x27, 0x3b, 0x7b, 0xfa, 0xd8, 0x04, 0x5d, 0x85, 0xa4, 0x70,
]);

/// Outcome of a [`sweep`]. `problems` empty ⇔ the DB passed every check.
///
/// Note on `state_root`: `seed_genesis` stores one on EVERY database, so a
/// plain-writer (executor) DB carries the genesis root frozen at block 0
/// while its chain advances — only the trie-aware (validator) writer keeps
/// it live. The rebuild check below therefore validates the genesis-era trie
/// on executor DBs and the current trie on validator DBs; both are internally
/// consistent, which is what a sweep can assert about one database alone.
#[derive(Debug, Default)]
pub struct IntegrityReport {
    pub last_committed_block: u64,
    pub headers: u64,
    pub receipts: u64,
    pub accounts: u64,
    pub storage_slots: u64,
    /// `meta[state_root]` when present (trie-aware writers only).
    pub state_root: Option<B256>,
    /// Root recomputed from the trie tables when a stored root exists.
    pub rebuilt_root: Option<B256>,
    pub problems: Vec<String>,
}

impl IntegrityReport {
    pub fn is_clean(&self) -> bool {
        self.problems.is_empty()
    }
}

fn get_meta(txn: &RwTxSync, meta: Database, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
    Ok(txn.get::<Vec<u8>>(meta.dbi(), key)?)
}

/// Run the full invariant sweep over one state DB.
pub fn sweep(env: &StateEnv) -> Result<IntegrityReport, StateError> {
    let txn = env.raw().begin_rw_sync()?;
    let mut r = IntegrityReport::default();
    let problem = |p: String, r: &mut IntegrityReport| r.problems.push(p);

    let meta = txn.open_db(Some(TABLE_META))?;

    // --- meta: schema version, genesis, cursors ---------------------------
    match get_meta(&txn, meta, KEY_SCHEMA_VERSION)? {
        Some(b) => match decode_u32(&b) {
            Ok(v) if v == SCHEMA_VERSION => {}
            Ok(v) => problem(
                format!("schema_version {v} != expected {SCHEMA_VERSION}"),
                &mut r,
            ),
            Err(e) => problem(format!("schema_version undecodable: {e}"), &mut r),
        },
        None => problem("schema_version missing".into(), &mut r),
    }
    let genesis_applied = get_meta(&txn, meta, KEY_GENESIS_APPLIED)?.is_some();
    if !genesis_applied {
        problem(
            "genesis_applied flag missing (DB never seeded)".into(),
            &mut r,
        );
    } else if get_meta(&txn, meta, KEY_GENESIS_DIGEST)?.is_none() {
        problem("genesis seeded but genesis_digest missing".into(), &mut r);
    }
    r.last_committed_block = match get_meta(&txn, meta, KEY_LAST_COMMITTED_BLOCK)? {
        Some(b) => decode_u64(&b).unwrap_or_else(|e| {
            r.problems
                .push(format!("last_committed_block undecodable: {e}"));
            0
        }),
        None => 0,
    };
    let meta_end_tx = match get_meta(&txn, meta, KEY_LAST_COMMITTED_END_TX_POSITION)? {
        Some(b) => match decode_b_position(&b) {
            Ok(p) => Some(p),
            Err(e) => {
                problem(
                    format!("last_committed_end_tx_position undecodable: {e}"),
                    &mut r,
                );
                None
            }
        },
        None => None,
    };

    // --- headers: every row decodes; keys dense up to the meta cursor -----
    let headers_db = txn.open_db(Some(TABLE_HEADERS))?;
    let mut cur = txn.cursor(headers_db)?;
    let mut prev_block: Option<u64> = None;
    let mut first_block: Option<u64> = None;
    let mut last_header_end_tx = None;
    let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((k, v)) = item {
        if k.len() != 8 {
            problem(
                format!("headers key of length {} (expected 8)", k.len()),
                &mut r,
            );
            break;
        }
        let block = u64::from_be_bytes(k[..8].try_into().expect("8 bytes"));
        match decode_header_value(&v) {
            Ok(h) => last_header_end_tx = Some(h.end_tx_idx),
            Err(e) => problem(format!("headers[{block}] undecodable: {e}"), &mut r),
        }
        if let Some(p) = prev_block
            && block != p + 1
        {
            problem(format!("headers gap: {p} -> {block}"), &mut r);
        }
        first_block.get_or_insert(block);
        prev_block = Some(block);
        r.headers += 1;
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    if let Some(first) = first_block
        && first > 1
    {
        problem(
            format!("headers start at {first} (expected 0 or 1)"),
            &mut r,
        );
    }
    if let Some(last) = prev_block
        && last != r.last_committed_block
    {
        problem(
            format!(
                "last header {last} != meta last_committed_block {}",
                r.last_committed_block
            ),
            &mut r,
        );
    }
    if r.last_committed_block > 0 && r.headers == 0 {
        problem("meta cursor set but headers table empty".into(), &mut r);
    }
    if let (Some(h), Some(m)) = (last_header_end_tx, meta_end_tx)
        && h != m
    {
        problem(
            format!("last header end_tx_idx {h:?} != meta cursor {m:?}"),
            &mut r,
        );
    }

    // --- receipts: every row decodes; index round-trips both ways ---------
    let receipts_db = txn.open_db(Some(TABLE_RECEIPTS))?;
    let tx_hash_db = txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
    let mut cur = txn.cursor(receipts_db)?;
    let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((k, v)) = item {
        match (decode_b_position(&k), decode_receipt_value(&v)) {
            (Ok(pos), Ok(receipt)) => {
                if receipt.tx_idx != pos {
                    problem(
                        format!("receipts[{pos:?}] carries tx_idx {:?}", receipt.tx_idx),
                        &mut r,
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
                            &mut r,
                        ),
                        Err(e) => {
                            problem(format!("tx_hash_index[{}]: {e}", receipt.tx_hash), &mut r)
                        }
                    },
                    None => problem(
                        format!("receipt {:?} missing from tx_hash_index", receipt.tx_hash),
                        &mut r,
                    ),
                }
                if let Some(m) = meta_end_tx
                    && pos > m
                {
                    problem(
                        format!("receipt at {pos:?} beyond meta cursor {m:?}"),
                        &mut r,
                    );
                }
            }
            (Err(e), _) => problem(format!("receipts key: {e}"), &mut r),
            (_, Err(e)) => problem(format!("receipts value at {:02x?}: {e}", &k[..]), &mut r),
        }
        r.receipts += 1;
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    // Reverse direction: every index entry points at an existing receipt
    // (counts alone would let dangling entries hide behind missing ones).
    let mut cur = txn.cursor(tx_hash_db)?;
    let mut index_entries = 0u64;
    let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((k, v)) = item {
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
                        &mut r,
                    );
                }
            }
            Err(e) => problem(format!("tx_hash_index value: {e}"), &mut r),
        }
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }
    if index_entries != r.receipts {
        problem(
            format!(
                "tx_hash_index has {index_entries} entries, receipts has {}",
                r.receipts
            ),
            &mut r,
        );
    }

    // --- accounts: rows decode; declared code exists -----------------------
    let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS))?;
    let code_db = txn.open_db(Some(TABLE_CODE))?;
    let mut cur = txn.cursor(accounts_db)?;
    let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((k, v)) = item {
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
                        &mut r,
                    );
                }
            }
            Err(e) => problem(format!("accounts value at {:02x?}: {e}", &k[..4]), &mut r),
        }
        r.accounts += 1;
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }

    // --- storage: values decode --------------------------------------------
    let storage_db = txn.open_db(Some(TABLE_STORAGE))?;
    let mut cur = txn.cursor(storage_db)?;
    let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
    while let Some((k, v)) = item {
        if k.len() != 52 {
            problem(
                format!("storage key of length {} (expected 52)", k.len()),
                &mut r,
            );
        } else if let Err(e) = decode_storage_value(&v) {
            problem(format!("storage value at {:02x?}: {e}", &k[..4]), &mut r);
        }
        r.storage_slots += 1;
        item = cur.next::<Vec<u8>, Vec<u8>>()?;
    }

    // --- trie: the persisted root must reproduce from the trie tables ------
    r.state_root = match get_meta(&txn, meta, KEY_STATE_ROOT)? {
        Some(b) => match decode_b256(&b) {
            Ok(root) => Some(root),
            Err(e) => {
                problem(format!("state_root undecodable: {e}"), &mut r);
                None
            }
        },
        None => None, // plain (executor) writer — no root to verify
    };
    if let Some(stored) = r.state_root {
        let tables = TrieTables::open(&txn)?;
        let rebuilt = rebuild_root(&txn, &tables)?;
        r.rebuilt_root = Some(rebuilt);
        if rebuilt != stored {
            problem(
                format!("trie rebuild {rebuilt} != stored state_root {stored}"),
                &mut r,
            );
        }
    }

    Ok(r)
}

/// Byte-level comparison of the chain-state tables of two DBs (an executor's
/// and a validator's, canonically). Returns human-readable differences —
/// empty means the two databases hold identical chain state. Trie tables and
/// per-node meta (fsync watermark, state_root) are deliberately excluded:
/// only the validator maintains a trie, and the sweep verifies it.
pub fn deep_compare(a: &StateEnv, b: &StateEnv) -> Result<Vec<String>, StateError> {
    const SHARED_TABLES: &[&str] = &[
        TABLE_ACCOUNTS,
        TABLE_STORAGE,
        TABLE_CODE,
        TABLE_HEADERS,
        TABLE_RECEIPTS,
        TABLE_TX_HASH_INDEX,
    ];
    const MAX_DIFFS_PER_TABLE: usize = 8;

    let ta = a.raw().begin_rw_sync()?;
    let tb = b.raw().begin_rw_sync()?;
    let mut diffs = Vec::new();

    for table in SHARED_TABLES {
        let da = ta.open_db(Some(table))?;
        let db = tb.open_db(Some(table))?;
        let mut ca = ta.cursor(da)?;
        let mut cb = tb.cursor(db)?;
        let mut ia = ca.first::<Vec<u8>, Vec<u8>>()?;
        let mut ib = cb.first::<Vec<u8>, Vec<u8>>()?;
        let mut table_diffs = 0usize;
        while table_diffs < MAX_DIFFS_PER_TABLE {
            match (&ia, &ib) {
                (None, None) => break,
                (Some((ka, va)), Some((kb, vb))) => {
                    if ka != kb {
                        diffs.push(format!(
                            "{table}: key mismatch a={:02x?} b={:02x?}",
                            &ka[..ka.len().min(8)],
                            &kb[..kb.len().min(8)]
                        ));
                        table_diffs += 1;
                        // Advance the smaller side to resynchronize.
                        if ka < kb {
                            ia = ca.next::<Vec<u8>, Vec<u8>>()?;
                        } else {
                            ib = cb.next::<Vec<u8>, Vec<u8>>()?;
                        }
                        continue;
                    }
                    if va != vb {
                        // Byte lengths alone say nothing about WHAT diverged —
                        // "224 vs 224 bytes" is the shape a fixed-width field
                        // mismatch takes, and chasing it from CI logs is
                        // guesswork. Receipts decode, so name the fields.
                        let detail = if *table == TABLE_RECEIPTS {
                            receipt_field_diff(va, vb)
                        } else {
                            None
                        };
                        match detail {
                            Some(d) => {
                                diffs.push(format!("{table}[{:02x?}]: {d}", &ka[..ka.len().min(8)]))
                            }
                            None => diffs.push(format!(
                                "{table}[{:02x?}]: values differ ({} vs {} bytes)",
                                &ka[..ka.len().min(8)],
                                va.len(),
                                vb.len()
                            )),
                        }
                        table_diffs += 1;
                    }
                    ia = ca.next::<Vec<u8>, Vec<u8>>()?;
                    ib = cb.next::<Vec<u8>, Vec<u8>>()?;
                }
                (Some((ka, _)), None) => {
                    diffs.push(format!(
                        "{table}: extra key in a: {:02x?}",
                        &ka[..ka.len().min(8)]
                    ));
                    table_diffs += 1;
                    ia = ca.next::<Vec<u8>, Vec<u8>>()?;
                }
                (None, Some((kb, _))) => {
                    diffs.push(format!(
                        "{table}: extra key in b: {:02x?}",
                        &kb[..kb.len().min(8)]
                    ));
                    table_diffs += 1;
                    ib = cb.next::<Vec<u8>, Vec<u8>>()?;
                }
            }
        }
        if table_diffs >= MAX_DIFFS_PER_TABLE {
            diffs.push(format!("{table}: further diffs truncated"));
        }
    }

    // Shared meta cursors must agree (per-node keys — fsync watermark,
    // state_root — excluded by design).
    let ma = ta.open_db(Some(TABLE_META))?;
    let mb = tb.open_db(Some(TABLE_META))?;
    for key in [
        KEY_LAST_COMMITTED_BLOCK,
        KEY_LAST_COMMITTED_END_TX_POSITION,
        KEY_GENESIS_DIGEST,
        KEY_SCHEMA_VERSION,
    ] {
        let va = ta.get::<Vec<u8>>(ma.dbi(), key)?;
        let vb = tb.get::<Vec<u8>>(mb.dbi(), key)?;
        if va != vb {
            diffs.push(format!(
                "meta[{}]: {:02x?} vs {:02x?}",
                String::from_utf8_lossy(key),
                va.as_deref().map(|v| &v[..v.len().min(8)]),
                vb.as_deref().map(|v| &v[..v.len().min(8)])
            ));
        }
    }

    Ok(diffs)
}

/// Field-level diff of two encoded receipts, for the deep-compare report.
///
/// Returns `None` when either side does not decode — the caller then falls
/// back to the byte-length message, which is still true, just uninformative.
fn receipt_field_diff(a: &[u8], b: &[u8]) -> Option<String> {
    let ra = decode_receipt_value(a).ok()?;
    let rb = decode_receipt_value(b).ok()?;
    let mut fields: Vec<String> = Vec::new();
    macro_rules! cmp {
        ($f:ident) => {
            if ra.$f != rb.$f {
                fields.push(format!("{}: {:?} vs {:?}", stringify!($f), ra.$f, rb.$f));
            }
        };
    }
    cmp!(tx_idx);
    cmp!(tx_type);
    cmp!(tx_hash);
    cmp!(status);
    cmp!(gas_used);
    cmp!(write_set_hash);
    cmp!(nonce);
    cmp!(from);
    cmp!(to);
    cmp!(contract_address);
    cmp!(effective_gas_price);
    cmp!(block_number);
    cmp!(transaction_index);
    if ra.logs != rb.logs {
        fields.push(format!(
            "logs: {} vs {} entries",
            ra.logs.len(),
            rb.logs.len()
        ));
    }
    if fields.is_empty() {
        // Decoded equal but the bytes differ: an encoding difference, which
        // is itself worth saying out loud rather than reporting as "no diff".
        return Some(format!(
            "receipts decode EQUAL but encode differently ({} vs {} bytes) — an \
             encoding-level divergence",
            a.len(),
            b.len()
        ));
    }
    Some(format!("receipt fields differ — {}", fields.join("; ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, U256};
    use kardamom_types::{BPosition, BlockBoundary, BlockDelta, Receipt};
    use signet_libmdbx::WriteFlags;

    use crate::env::{Durability, StateEnvBuilder};
    use crate::writer::{StateWriter, TrieMode, WriteBatch};

    /// Build a small 2-block chain (with receipts) through the trie-aware
    /// writer into `dir`, seeded with a genesis account.
    fn build_db(dir: &std::path::Path) {
        let env = StateEnvBuilder::new(dir)
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        let genesis_accounts = vec![kardamom_types::AccountChange {
            address: Address::from([0xAA; 20]),
            nonce: 0,
            balance: U256::from(1_000_000u64),
            code_hash: B256::ZERO,
        }];
        crate::genesis::seed_genesis(&env, &genesis_accounts, &[]).unwrap();
        let handle = StateWriter::spawn_with_trie(env, TrieMode::Incremental).unwrap();
        for block in 1..=2u64 {
            let receipt = Receipt {
                tx_idx: BPosition::from_index(block),
                tx_hash: B256::from(U256::from(0xBEEF00 + block)),
                status: true,
                gas_used: 21_000,
                write_set_hash: B256::from(U256::from(7u64)),
                ..Default::default()
            };
            let delta = BlockDelta {
                block_number: block,
                accounts: vec![kardamom_types::AccountChange {
                    address: Address::from([0xAA; 20]),
                    nonce: block,
                    balance: U256::from(1_000_000 - block * 10),
                    code_hash: B256::ZERO,
                }],
                storage: vec![],
                code: vec![],
                receipts: vec![receipt],
            };
            let boundary = BlockBoundary {
                block_number: block,
                end_tx_idx: BPosition::from_index(block),
                l2_timestamp: 1_700_000_000 + block,
                l1_origin: 0,
            };
            handle
                .delta_tx
                .send(WriteBatch::new(boundary, delta))
                .unwrap();
        }
        handle.shutdown().unwrap();
    }

    fn open(dir: &std::path::Path) -> StateEnv {
        StateEnvBuilder::new(dir)
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap()
    }

    #[test]
    fn sweep_is_clean_on_a_healthy_db() {
        let dir = tempfile::tempdir().unwrap();
        build_db(dir.path());
        let r = sweep(&open(dir.path())).unwrap();
        assert!(r.is_clean(), "problems: {:?}", r.problems);
        assert_eq!(r.last_committed_block, 2);
        assert_eq!(r.receipts, 2);
        assert!(r.state_root.is_some(), "trie writer persists a root");
        assert_eq!(r.state_root, r.rebuilt_root);
    }

    #[test]
    fn sweep_detects_a_flipped_receipt_byte() {
        let dir = tempfile::tempdir().unwrap();
        build_db(dir.path());
        // Corrupt one receipt value in place (what disk rot / a torn write
        // would look like at the row level).
        {
            let env = open(dir.path());
            let txn = env.raw().begin_rw_sync().unwrap();
            let db = txn.open_db(Some(TABLE_RECEIPTS)).unwrap();
            let (k, mut v) = {
                let mut cur = txn.cursor(db).unwrap();
                cur.first::<Vec<u8>, Vec<u8>>().unwrap().unwrap()
            };
            let last = v.len() - 1;
            v[last] ^= 0xFF;
            v.truncate(v.len() - 3); // also shear the tail so rkyv must reject
            txn.put(db, &k, &v, WriteFlags::UPSERT).unwrap();
            txn.commit().unwrap();
        }
        let r = sweep(&open(dir.path())).unwrap();
        assert!(!r.is_clean(), "sweep must flag the corrupted receipt");
        assert!(
            r.problems.iter().any(|p| p.contains("receipts")),
            "problems: {:?}",
            r.problems
        );
    }

    #[test]
    fn sweep_detects_a_headers_gap() {
        let dir = tempfile::tempdir().unwrap();
        build_db(dir.path());
        {
            let env = open(dir.path());
            let txn = env.raw().begin_rw_sync().unwrap();
            let db = txn.open_db(Some(TABLE_HEADERS)).unwrap();
            txn.del(db, crate::schema::encode_block_key(1), None)
                .unwrap();
            txn.commit().unwrap();
        }
        let r = sweep(&open(dir.path())).unwrap();
        assert!(
            r.problems
                .iter()
                .any(|p| p.contains("gap") || p.contains("start at")),
            "problems: {:?}",
            r.problems
        );
    }

    #[test]
    fn deep_compare_identical_dbs_is_empty_and_divergent_is_not() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        build_db(a.path());
        build_db(b.path());
        let ea = open(a.path());
        let eb = open(b.path());
        assert!(deep_compare(&ea, &eb).unwrap().is_empty());
        drop(eb);
        // Perturb one account balance in b.
        {
            let eb = open(b.path());
            let txn = eb.raw().begin_rw_sync().unwrap();
            let db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
            let (k, v) = {
                let mut cur = txn.cursor(db).unwrap();
                cur.first::<Vec<u8>, Vec<u8>>().unwrap().unwrap()
            };
            let mut acct = decode_account_value(&v).unwrap();
            acct.balance += U256::from(1u64);
            txn.put(
                db,
                &k,
                crate::schema::encode_account_value(&acct),
                WriteFlags::UPSERT,
            )
            .unwrap();
            txn.commit().unwrap();
        }
        let eb = open(b.path());
        let diffs = deep_compare(&ea, &eb).unwrap();
        assert!(
            diffs.iter().any(|d| d.contains(TABLE_ACCOUNTS)),
            "diffs: {diffs:?}"
        );
    }
}
