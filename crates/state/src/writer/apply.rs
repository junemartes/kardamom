//! [`StateWriter::apply`]: persists one write batch inside a single mdbx
//! read-write transaction.

use std::time::{Duration, Instant};

use signet_libmdbx::tx::aliases::RwTxSync;
use signet_libmdbx::{Database, WriteFlags};
use tracing::error;

use kardamom_types::{BlockBoundary, BlockDelta};

use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION,
    KEY_STATE_ROOT, encode_b_position, encode_b256, encode_u64,
};
use crate::schema::{
    HeaderValue, TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META, TABLE_RECEIPTS,
    TABLE_STORAGE, TABLE_TX_HASH_INDEX, encode_block_key, encode_header_value,
    encode_receipt_value, encode_storage_key, encode_storage_value, encode_tx_hash_key,
    encode_tx_hash_value,
};
use crate::trie;

use super::{StateWriter, TrieMode, WriteBatch};

/// Per-section stopwatches for one `apply` call, reported when
/// `KARDAMOM_WRITER_TIMING` is set.
struct ApplyTimings {
    open: Duration,
    storage: Duration,
    accounts: Duration,
    receipts: Duration,
    commit: Duration,
}

impl ApplyTimings {
    fn report(&self, batch: &WriteBatch) {
        eprintln!(
            "writer apply block {}: open {:?} storage {:?} accounts {:?} receipts {:?} commit {:?} (n: sto {} acc {} rcpt {})",
            batch.boundary.block_number,
            self.open,
            self.storage,
            self.accounts,
            self.receipts,
            self.commit,
            batch.delta.storage.len(),
            batch.delta.accounts.len(),
            batch.delta.receipts.len(),
        );
    }
}

/// One write batch's tables and per-section stopwatch, open on one mdbx
/// read-write transaction. Each step method writes one section, then
/// returns `self` so [`StateWriter::apply`] can chain the whole batch as
/// one expression: `writer.storage(delta)?.accounts(delta)?...`.
struct BatchWriter<'a> {
    txn: &'a RwTxSync,
    accounts: Database,
    storage: Database,
    code: Database,
    headers: Database,
    receipts: Database,
    tx_hash_index: Database,
    meta: Database,
    trie_mode: TrieMode,
    timing: ApplyTimings,
}

impl<'a> BatchWriter<'a> {
    /// Open every table this batch touches, timed as the batch's `open`
    /// section.
    fn open(txn: &'a RwTxSync, trie_mode: TrieMode) -> Result<Self, StateError> {
        let t0 = Instant::now();
        let accounts = txn.open_db(Some(TABLE_ACCOUNTS))?;
        let storage = txn.open_db(Some(TABLE_STORAGE))?;
        let code = txn.open_db(Some(TABLE_CODE))?;
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        let receipts = txn.open_db(Some(TABLE_RECEIPTS))?;
        let tx_hash_index = txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
        let meta = txn.open_db(Some(TABLE_META))?;
        Ok(Self {
            txn,
            accounts,
            storage,
            code,
            headers,
            receipts,
            tx_hash_index,
            meta,
            trie_mode,
            timing: ApplyTimings {
                open: t0.elapsed(),
                storage: Duration::ZERO,
                accounts: Duration::ZERO,
                receipts: Duration::ZERO,
                commit: Duration::ZERO,
            },
        })
    }

    /// Write every storage-slot change with a cursor.
    ///
    /// Upstream `StorageChange.key` is `B256`, matching the `StateDatabase`
    /// trait signature. The executor writes every slot as an absolute value.
    /// There are no tombstones: writing `U256::ZERO` means the slot is now
    /// zero.
    ///
    /// This uses a cursor with sorted input. `BlockDelta` vectors come from
    /// `BTreeMap` iteration, so keys ascend. A cursor upsert then walks down
    /// the tree from its previous position, instead of from the root. In one
    /// measurement, this was the difference between a writer that keeps pace
    /// with the execution pipeline and one running twice as slow.
    ///
    /// Called before accounts, so the trie-aware path can read an account's
    /// current slots when it recomputes that account's `storage_root`.
    fn storage(mut self, delta: &BlockDelta) -> Result<Self, StateError> {
        let t = Instant::now();
        let mut cur = self.txn.cursor(self.storage)?;
        for change in &delta.storage {
            let key = encode_storage_key(change.address, change.key);
            cur.put(
                &key,
                &encode_storage_value(change.value),
                WriteFlags::UPSERT,
            )?;
        }
        self.timing.storage = t.elapsed();
        Ok(self)
    }

    /// Write every account change with a cursor.
    ///
    /// The `accounts` table feeds revm reads: nonce, balance, and `code_hash`.
    /// It does not carry a meaningful `storage_root`. The state trie keeps the
    /// canonical per-account storage root in `hashed_accounts` (see
    /// `crate::trie`). This always persists `storage_root` as ZERO here,
    /// regardless of trie mode.
    fn accounts(mut self, delta: &BlockDelta) -> Result<Self, StateError> {
        let t = Instant::now();
        crate::schema::write_accounts(self.txn, self.accounts, &delta.accounts)?;
        self.timing.accounts = t.elapsed();
        Ok(self)
    }

    /// Write every new code entry. Code is content-addressed, so a key that
    /// already exists carries the same bytes; `write_code` skips the
    /// redundant write.
    fn code(self, delta: &BlockDelta) -> Result<Self, StateError> {
        crate::schema::write_code(self.txn, self.code, &delta.code)?;
        Ok(self)
    }

    /// Write this block's header row.
    fn header(self, boundary: &BlockBoundary) -> Result<Self, StateError> {
        let header = HeaderValue {
            end_tx_idx: boundary.end_tx_idx,
            l2_timestamp: boundary.l2_timestamp,
            l1_origin: boundary.l1_origin,
        };
        self.txn.put(
            self.headers,
            encode_block_key(boundary.block_number),
            encode_header_value(&header),
            WriteFlags::UPSERT,
        )?;
        Ok(self)
    }

    /// Write every receipt, then its `tx_hash_index` entry.
    ///
    /// This lets a caller serve `eth_getTransactionReceipt(hash)` with two
    /// reads: `StateDatabase::get_tx_position(hash)`, then
    /// `StateDatabase::get_receipt(pos)`.
    fn receipts_and_index(mut self, delta: &BlockDelta) -> Result<Self, StateError> {
        let t = Instant::now();
        // Receipts arrive in ascending BPosition order, so use a cursor.
        // One pass writes each receipt and collects its hash-index entry,
        // instead of a second walk over `delta.receipts` just to build `hk`.
        let mut cur = self.txn.cursor(self.receipts)?;
        let mut hk: Vec<([u8; 32], [u8; 8])> = Vec::with_capacity(delta.receipts.len());
        for r in &delta.receipts {
            let pos_key = encode_b_position(r.tx_idx);
            cur.put(&pos_key, &encode_receipt_value(r), WriteFlags::UPSERT)?;
            hk.push((
                encode_tx_hash_key(r.tx_hash),
                encode_tx_hash_value(r.tx_idx),
            ));
        }
        // The hash index's keys are random. Sort them first, so the cursor
        // gets the same locality benefit.
        hk.sort_unstable_by_key(|e| e.0);
        let mut cur = self.txn.cursor(self.tx_hash_index)?;
        for (k, v) in &hk {
            cur.put(k, v, WriteFlags::UPSERT)?;
        }
        self.timing.receipts = t.elapsed();
        Ok(self)
    }

    /// Write the block-level durable cursors, last so a reader never sees a
    /// cursor advance past data the same transaction has not yet committed.
    fn meta_cursors(self, boundary: &BlockBoundary) -> Result<Self, StateError> {
        self.txn.put(
            self.meta,
            KEY_LAST_COMMITTED_BLOCK,
            encode_u64(boundary.block_number),
            WriteFlags::UPSERT,
        )?;
        self.txn.put(
            self.meta,
            KEY_LAST_COMMITTED_END_TX_POSITION,
            encode_b_position(boundary.end_tx_idx),
            WriteFlags::UPSERT,
        )?;
        self.txn.put(
            self.meta,
            KEY_LAST_FSYNCED_B_POSITION,
            encode_b_position(boundary.end_tx_idx),
            WriteFlags::UPSERT,
        )?;
        Ok(self)
    }

    /// Advance the canonical Ethereum MPT world-state root incrementally, and
    /// persist it in the same transaction, when `trie_mode` is not `Off`. This
    /// makes the root advance atomically with the state. `ShadowCheck` also
    /// rebuilds the root from scratch at a sampling interval, and stops the
    /// writer on a mismatch, as a canary for walker bugs.
    fn advance_state_root(
        self,
        boundary: &BlockBoundary,
        delta: &BlockDelta,
    ) -> Result<Self, StateError> {
        if self.trie_mode == TrieMode::Off {
            return Ok(self);
        }
        let tables = trie::TrieTables::open(self.txn)?;
        let root = tables.update_for_block(self.txn, delta)?;
        // `every_n` is never 0 here: `StateWriter::spawn_with_trie` refuses to
        // construct a writer with `ShadowCheck { every_n: 0 }`.
        if let TrieMode::ShadowCheck { every_n } = self.trie_mode
            && boundary.block_number.is_multiple_of(every_n)
        {
            let rebuilt = tables.rebuild_root(self.txn)?;
            metrics::counter!("kardamom_state_trie_shadow_checks_total").increment(1);
            if rebuilt != root {
                metrics::counter!("kardamom_state_trie_shadow_mismatch_total").increment(1);
                error!(
                    block = boundary.block_number,
                    %root, %rebuilt, "trie shadow-check MISMATCH — halting writer"
                );
                return Err(StateError::ShadowMismatch {
                    block: boundary.block_number,
                    incremental: root,
                    rebuilt,
                });
            }
        }
        self.txn.put(
            self.meta,
            KEY_STATE_ROOT,
            encode_b256(root),
            WriteFlags::UPSERT,
        )?;
        Ok(self)
    }

    /// End the batch's table borrows and hand back its timing breakdown, so
    /// the caller can commit the owned transaction and time that commit.
    fn finish(self) -> ApplyTimings {
        self.timing
    }
}

impl StateWriter {
    pub(super) fn apply(&self, batch: &WriteBatch) -> Result<(), StateError> {
        let want_timing = std::env::var_os("KARDAMOM_WRITER_TIMING").is_some();
        let txn = self.env.raw().begin_rw_sync()?;

        let mut timing = BatchWriter::open(&txn, self.trie_mode)?
            .storage(&batch.delta)?
            .accounts(&batch.delta)?
            .code(&batch.delta)?
            .header(&batch.boundary)?
            .receipts_and_index(&batch.delta)?
            .meta_cursors(&batch.boundary)?
            .advance_state_root(&batch.boundary, &batch.delta)?
            .finish();

        let t_commit = Instant::now();
        txn.commit()?;
        timing.commit = t_commit.elapsed();

        if want_timing {
            timing.report(batch);
        }
        Ok(())
    }
}
