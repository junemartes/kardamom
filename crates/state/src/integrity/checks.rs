//! The per-table checks that make up [`sweep`](super::sweep).
//!
//! [`Sweep`] owns the read-write transaction and the [`IntegrityReport`]
//! being built. Each check method walks one table, or one coherent group
//! for the receipt index. It appends findings to the report, and
//! increases the report's row counts. [`super::sweep`] calls these
//! methods in a fixed order. Within each check, the iteration order
//! matches the table's key order.

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
use crate::trie::TrieTables;

use super::IntegrityReport;

/// Running trackers `Sweep::check_header_row` updates and
/// `Sweep::check_header_chain_ends` reads once the walk over `headers`
/// finishes.
#[derive(Default)]
struct HeaderChainState {
    prev_block: Option<u64>,
    first_block: Option<u64>,
    last_header_end_tx: Option<BPosition>,
}

/// One invariant sweep over one state DB: the transaction it reads from,
/// and the report it is building. [`super::sweep`] runs every check
/// method on one `Sweep`, in a fixed order, then takes the finished
/// report with [`Sweep::finish`].
pub(super) struct Sweep<'a> {
    txn: &'a RwTxSync,
    meta: Database,
    r: IntegrityReport,
}

impl<'a> Sweep<'a> {
    pub(super) fn new(txn: &'a RwTxSync, meta: Database) -> Self {
        Self {
            txn,
            meta,
            r: IntegrityReport::default(),
        }
    }

    /// Take the finished report. Call this after every check method.
    pub(super) fn finish(self) -> IntegrityReport {
        self.r
    }

    fn get_meta(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StateError> {
        Ok(self.txn.get::<Vec<u8>>(self.meta.dbi(), key)?)
    }

    fn problem(&mut self, p: String) {
        self.r.problems.push(p);
    }

    /// Read `key`, decode it, and record a problem naming `name` if it is
    /// present but malformed. Returns `None` if the key is absent, or if
    /// decoding failed (the problem is already recorded in that case).
    fn decoded_meta<T>(
        &mut self,
        key: &[u8],
        name: &str,
        decode: fn(&[u8]) -> Result<T, StateError>,
    ) -> Result<Option<T>, StateError> {
        let Some(b) = self.get_meta(key)? else {
            return Ok(None);
        };
        match decode(&b) {
            Ok(v) => Ok(Some(v)),
            Err(e) => {
                self.problem(format!("{name} undecodable: {e}"));
                Ok(None)
            }
        }
    }

    /// Checks meta: schema version, genesis, and cursors.
    ///
    /// Returns the decoded `last_committed_end_tx_position` cursor. The
    /// headers and receipts checks cross-reference this value.
    pub(super) fn meta(&mut self) -> Result<Option<BPosition>, StateError> {
        match self.get_meta(KEY_SCHEMA_VERSION)? {
            Some(b) => match decode_u32(&b) {
                Ok(v) if v == SCHEMA_VERSION => {}
                Ok(v) => self.problem(format!("schema_version {v} != expected {SCHEMA_VERSION}")),
                Err(e) => self.problem(format!("schema_version undecodable: {e}")),
            },
            None => self.problem("schema_version missing".into()),
        }
        let genesis_applied = self.get_meta(KEY_GENESIS_APPLIED)?.is_some();
        if !genesis_applied {
            self.problem("genesis_applied flag missing (DB never seeded)".into());
        } else if self.get_meta(KEY_GENESIS_DIGEST)?.is_none() {
            self.problem("genesis seeded but genesis_digest missing".into());
        }
        self.r.last_committed_block = match self.get_meta(KEY_LAST_COMMITTED_BLOCK)? {
            Some(b) => decode_u64(&b).unwrap_or_else(|e| {
                self.r
                    .problems
                    .push(format!("last_committed_block undecodable: {e}"));
                0
            }),
            None => 0,
        };
        let meta_end_tx = self.decoded_meta(
            KEY_LAST_COMMITTED_END_TX_POSITION,
            "last_committed_end_tx_position",
            decode_b_position,
        )?;
        Ok(meta_end_tx)
    }

    /// Checks headers: every row decodes, and keys are dense up to the meta
    /// cursor.
    pub(super) fn headers(&mut self, meta_end_tx: Option<BPosition>) -> Result<(), StateError> {
        let headers_db = self.txn.open_db(Some(TABLE_HEADERS))?;
        let txn = self.txn;
        let mut state = HeaderChainState::default();
        for_each_row(txn, headers_db, |k, v| {
            Ok(self.check_header_row(&k, &v, &mut state))
        })?;
        self.check_header_chain_ends(&state, meta_end_tx);
        Ok(())
    }

    /// One `headers` row: it must decode, and its block number must be one
    /// past the previous row's.
    fn check_header_row(
        &mut self,
        k: &[u8],
        v: &[u8],
        state: &mut HeaderChainState,
    ) -> ControlFlow<()> {
        let Ok(&key) = <&[u8; 8]>::try_from(k) else {
            self.problem(format!("headers key of length {} (expected 8)", k.len()));
            return ControlFlow::Break(());
        };
        let block = u64::from_be_bytes(key);
        match decode_header_value(v) {
            Ok(h) => state.last_header_end_tx = Some(h.end_tx_idx),
            Err(e) => self.problem(format!("headers[{block}] undecodable: {e}")),
        }
        if let Some(p) = state.prev_block {
            match p.checked_add(1) {
                Some(expected) if block != expected => {
                    self.problem(format!("headers gap: {p} -> {block}"));
                }
                Some(_) => {}
                None => self.problem(format!(
                    "headers key {p} has no successor block number (u64 overflow)"
                )),
            }
        }
        state.first_block.get_or_insert(block);
        state.prev_block = Some(block);
        self.r.headers += 1;
        ControlFlow::Continue(())
    }

    /// Checks the properties that only hold once the whole `headers` table has
    /// been walked: the chain starts at 0 or 1, ends at the meta cursor, and is
    /// non-empty whenever the meta cursor says blocks are committed.
    fn check_header_chain_ends(
        &mut self,
        state: &HeaderChainState,
        meta_end_tx: Option<BPosition>,
    ) {
        if let Some(first) = state.first_block
            && first > 1
        {
            self.problem(format!("headers start at {first} (expected 0 or 1)"));
        }
        if let Some(last) = state.prev_block
            && last != self.r.last_committed_block
        {
            self.problem(format!(
                "last header {last} != meta last_committed_block {}",
                self.r.last_committed_block
            ));
        }
        if self.r.last_committed_block > 0 && self.r.headers == 0 {
            self.problem("meta cursor set but headers table empty".into());
        }
        if let (Some(h), Some(m)) = (state.last_header_end_tx, meta_end_tx)
            && h != m
        {
            self.problem(format!("last header end_tx_idx {h:?} != meta cursor {m:?}"));
        }
    }

    /// Checks receipts: every row decodes, and the index round-trips both ways.
    pub(super) fn receipts_index(
        &mut self,
        meta_end_tx: Option<BPosition>,
    ) -> Result<(), StateError> {
        let receipts_db = self.txn.open_db(Some(TABLE_RECEIPTS))?;
        let tx_hash_db = self.txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
        let txn = self.txn;
        for_each_row(txn, receipts_db, |k, v| {
            self.check_receipt_row(tx_hash_db, &k, &v, meta_end_tx)?;
            self.r.receipts += 1;
            Ok(ControlFlow::Continue(()))
        })?;
        // Check the reverse direction too: every index entry must point at
        // an existing receipt. Counts alone would let dangling entries hide
        // behind missing ones.
        let index_entries = self.check_index_rows(receipts_db, tx_hash_db)?;
        if index_entries != self.r.receipts {
            self.problem(format!(
                "tx_hash_index has {index_entries} entries, receipts has {}",
                self.r.receipts
            ));
        }
        Ok(())
    }

    /// One `receipts` row: it must decode, carry its own position, and have a
    /// matching `tx_hash_index` entry that maps back to the same position.
    fn check_receipt_row(
        &mut self,
        tx_hash_db: Database,
        key: &[u8],
        value: &[u8],
        meta_end_tx: Option<BPosition>,
    ) -> Result<(), StateError> {
        match (decode_b_position(key), decode_receipt_value(value)) {
            (Ok(pos), Ok(receipt)) => {
                if receipt.tx_idx != pos {
                    self.problem(format!(
                        "receipts[{pos:?}] carries tx_idx {:?}",
                        receipt.tx_idx
                    ));
                }
                // Index must map this receipt's hash back to this position.
                match self
                    .txn
                    .get::<Vec<u8>>(tx_hash_db.dbi(), &encode_tx_hash_key(receipt.tx_hash))?
                {
                    Some(index_bytes) => match decode_tx_hash_value(&index_bytes) {
                        Ok(indexed_pos) if indexed_pos == pos => {}
                        Ok(indexed_pos) => self.problem(format!(
                            "tx_hash_index[{}] -> {indexed_pos:?}, receipt sits at {pos:?}",
                            receipt.tx_hash
                        )),
                        Err(e) => self.problem(format!("tx_hash_index[{}]: {e}", receipt.tx_hash)),
                    },
                    None => self.problem(format!(
                        "receipt {:?} missing from tx_hash_index",
                        receipt.tx_hash
                    )),
                }
                if let Some(m) = meta_end_tx
                    && pos > m
                {
                    self.problem(format!("receipt at {pos:?} beyond meta cursor {m:?}"));
                }
            }
            (Err(e), _) => self.problem(format!("receipts key: {e}")),
            (_, Err(e)) => self.problem(format!("receipts value at {key:02x?}: {e}")),
        }
        Ok(())
    }

    /// The reverse direction: every `tx_hash_index` entry must point at an
    /// existing receipt. Returns the number of index rows seen.
    fn check_index_rows(
        &mut self,
        receipts_db: Database,
        tx_hash_db: Database,
    ) -> Result<u64, StateError> {
        let mut index_entries = 0u64;
        let txn = self.txn;
        for_each_row(txn, tx_hash_db, |k, v| {
            index_entries += 1;
            match decode_tx_hash_value(&v) {
                Ok(pos) => {
                    if txn
                        .get::<Vec<u8>>(receipts_db.dbi(), &crate::meta::encode_b_position(pos))?
                        .is_none()
                    {
                        self.problem(format!(
                            "tx_hash_index entry {:02x?} -> {pos:?} has no receipt",
                            super::head(&k)
                        ));
                    }
                }
                Err(e) => self.problem(format!("tx_hash_index value: {e}")),
            }
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(index_entries)
    }

    /// Checks accounts: rows decode, and declared code exists.
    pub(super) fn accounts(&mut self) -> Result<(), StateError> {
        let accounts_db = self.txn.open_db(Some(TABLE_ACCOUNTS))?;
        let code_db = self.txn.open_db(Some(TABLE_CODE))?;
        let txn = self.txn;
        for_each_row(txn, accounts_db, |k, v| {
            match decode_account_value(&v) {
                Ok(a) => {
                    if a.code_hash != B256::ZERO
                        && a.code_hash != KECCAK_EMPTY
                        && txn
                            .get::<Vec<u8>>(code_db.dbi(), &encode_code_key(a.code_hash))?
                            .is_none()
                    {
                        self.problem(format!(
                            "account {:02x?} declares missing code {}",
                            super::head(&k),
                            a.code_hash
                        ));
                    }
                }
                Err(e) => self.problem(format!("accounts value at {:02x?}: {e}", super::head(&k))),
            }
            self.r.accounts += 1;
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(())
    }

    /// Checks storage: values decode.
    pub(super) fn storage(&mut self) -> Result<(), StateError> {
        let storage_db = self.txn.open_db(Some(TABLE_STORAGE))?;
        let txn = self.txn;
        for_each_row(txn, storage_db, |k, v| {
            if k.len() != 52 {
                self.problem(format!("storage key of length {} (expected 52)", k.len()));
            } else if let Err(e) = decode_storage_value(&v) {
                self.problem(format!("storage value at {:02x?}: {e}", super::head(&k)));
            }
            self.r.storage_slots += 1;
            Ok(ControlFlow::Continue(()))
        })?;
        Ok(())
    }

    /// Checks the trie: the persisted root must reproduce from the trie tables.
    pub(super) fn trie(&mut self) -> Result<(), StateError> {
        // `None` covers both an absent key (a plain, executor writer has no
        // root to verify) and a present-but-malformed one (already recorded
        // as a problem by `decoded_meta`).
        self.r.state_root = self.decoded_meta(KEY_STATE_ROOT, "state_root", decode_b256)?;
        if let Some(stored) = self.r.state_root {
            let tables = TrieTables::open(self.txn)?;
            let rebuilt = tables.rebuild_root(self.txn)?;
            self.r.rebuilt_root = Some(rebuilt);
            if rebuilt != stored {
                self.problem(format!(
                    "trie rebuild {rebuilt} != stored state_root {stored}"
                ));
            }
        }
        Ok(())
    }
}
