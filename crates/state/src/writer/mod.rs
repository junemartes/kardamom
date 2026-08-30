//! A single writer thread. It drains the [`WriteBatch`] channel, commits
//! one mdbx read-write transaction per block boundary, and publishes new
//! snapshots through the snapshot-swap channel.
//!
//! ## Coordination with the executor
//!
//! The executor's `submit(boundary: BlockBoundary, delta: BlockDelta)` is
//! the producer. This writer is the consumer. This crate accepts the pair
//! as a [`WriteBatch`], so the cursor persisted in the `meta` table
//! tracks all three values:
//!
//! - `last_committed_block` = `boundary.block_number`
//! - `last_committed_end_tx_position` = `boundary.end_tx_idx`
//! - `last_fsynced_b_position` = `boundary.end_tx_idx`. These are the
//!   same value, because the boundary's `end_tx_idx` is the last B
//!   position the executor committed through.
//!
//! `BlockDelta` lives in `kardamom-types`. This crate never redefines it.

use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender};
use kardamom_types::{BlockBoundary, BlockDelta};
use signet_libmdbx::{WriteFlags, sys::EnvironmentKind};
use tracing::{debug, error, info, warn};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION,
    KEY_SCHEMA_VERSION, KEY_STATE_ROOT, SCHEMA_VERSION, encode_b_position, encode_b256, encode_u32,
    encode_u64,
};
use crate::schema::{
    AccountValue, HeaderValue, TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META,
    TABLE_RECEIPTS, TABLE_STORAGE, TABLE_TX_HASH_INDEX, encode_account_key, encode_account_value,
    encode_block_key, encode_code_key, encode_header_value, encode_receipt_value,
    encode_storage_key, encode_storage_value, encode_tx_hash_key, encode_tx_hash_value,
};
use crate::snapshot::StateSnapshot;
use crate::swap::{SnapshotHandle, SnapshotReceiver, channel as swap_channel};
use crate::trie;
use alloy_primitives::B256;

/// One block's worth of state changes, submitted to the writer.
///
/// This pairs the boundary marker with its delta. The writer then
/// persists, in a single atomic mdbx commit:
///
/// - The block-level cursors.
/// - The per-key state mutations.
/// - The per-transaction receipts.
#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub boundary: BlockBoundary,
    pub delta: BlockDelta,
}

impl WriteBatch {
    pub fn new(boundary: BlockBoundary, delta: BlockDelta) -> Self {
        Self { boundary, delta }
    }

    /// The worst-case encoded size, used by the writer to budget the mdbx
    /// transaction. This is a heuristic, not an exact value.
    pub fn approx_size_bytes(&self) -> usize {
        let acct = self.delta.accounts.len() * (20 + 96);
        let stor = self.delta.storage.len() * (52 + 32);
        let code: usize = self.delta.code.iter().map(|c| 32 + c.code.len()).sum();
        let receipts: usize = self.delta.receipts.len() * (8 + 256);
        let tx_index: usize = self.delta.receipts.len() * (32 + 8);
        let header = 8 + 20;
        acct + stor + code + receipts + tx_index + header
    }
}

/// The handle returned by [`StateWriter::spawn`]. Drop it to stop the
/// writer thread. This closes the delta sender, and the thread joins on
/// its next loop iteration.
pub struct WriterHandle {
    pub delta_tx: Sender<WriteBatch>,
    pub snapshot_rx: SnapshotReceiver,
    join: Option<JoinHandle<Result<(), StateError>>>,
}

impl WriterHandle {
    /// Stop the writer and wait for its thread to exit. Returns the writer's
    /// final result.
    pub fn shutdown(mut self) -> Result<(), StateError> {
        drop(self.delta_tx);
        match self.join.take() {
            Some(j) => j.join().expect("writer thread panicked"),
            None => Ok(()),
        }
    }
}

/// How the writer maintains the state-root trie.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrieMode {
    /// No trie. The sequencer-side executor uses this; v0 emits no
    /// state-root commitment.
    Off,
    /// A node-incremental trie. This persists the canonical MPT root per
    /// block.
    Incremental,
    /// Incremental, plus a full-rebuild shadow-check every `every_n`
    /// blocks. This check stops the writer on a mismatch, as a canary
    /// against walker bugs.
    ShadowCheck { every_n: u64 },
}

/// The single-writer state thread. It owns the only read-write mdbx
/// transaction at a time.
pub struct StateWriter {
    env: StateEnv,
    delta_rx: Receiver<WriteBatch>,
    snapshot_handle: SnapshotHandle,
    /// The trie maintenance mode. `Off` for the executor. `Incremental` or
    /// `ShadowCheck` for the validator: each block commit then advances
    /// the canonical Ethereum MPT world-state root (see [`crate::trie`])
    /// inside the same atomic transaction.
    trie_mode: TrieMode,
}

impl StateWriter {
    /// Spawn the plain writer (no state-root trie) on a dedicated OS thread.
    /// This is the sequencer-side executor's backend.
    pub fn spawn(env: StateEnv) -> Result<WriterHandle, StateError> {
        Self::spawn_inner(env, TrieMode::Off)
    }

    /// Spawn the trie-aware writer with the given [`TrieMode`]. Each block
    /// commit then advances the Ethereum MPT state root inside the same
    /// atomic transaction. The validator uses this.
    pub fn spawn_with_trie(env: StateEnv, mode: TrieMode) -> Result<WriterHandle, StateError> {
        Self::spawn_inner(env, mode)
    }

    fn spawn_inner(env: StateEnv, trie_mode: TrieMode) -> Result<WriterHandle, StateError> {
        // This channel is bounded, HORIZON_BLOCKS deep. If the writer falls
        // behind by more than the version horizon, the executor blocks
        // here. At that point, the snapshot the executor holds is about
        // to become invalid anyway, so blocking is the correct fail-fast
        // behavior.
        let (delta_tx, delta_rx) =
            crossbeam_channel::bounded(crate::geometry::HORIZON_BLOCKS as usize);
        let (snapshot_handle, snapshot_rx) = swap_channel();

        // Write the schema-version meta key on first start (and verify it on
        // subsequent starts).
        ensure_schema_version(&env)?;

        // Publish an initial snapshot at the current cursors.
        let initial = StateSnapshot::open(&env)?;
        snapshot_handle.publish(initial);

        let writer = StateWriter {
            env: env.clone(),
            delta_rx,
            snapshot_handle: snapshot_handle.clone(),
            trie_mode,
        };

        let join = thread::Builder::new()
            .name("kardamom-state-writer".into())
            .spawn(move || writer.run())?;

        Ok(WriterHandle {
            delta_tx,
            snapshot_rx,
            join: Some(join),
        })
    }

    fn run(self) -> Result<(), StateError> {
        info!(
            path = %self.env.path().display(),
            env_kind = ?self.env.raw().env_kind(),
            "state writer started"
        );
        // Confirm the env kind is what we expect. This crate uses the
        // no-write-map mode by default. That is the safest choice for
        // arbitrary kernels, and it is signet-libmdbx's default.
        assert!(matches!(
            self.env.raw().env_kind(),
            EnvironmentKind::Default | EnvironmentKind::WriteMap
        ));
        loop {
            let batch = match self.delta_rx.recv() {
                Ok(b) => b,
                Err(_) => {
                    info!("delta channel closed; writer shutting down");
                    return Ok(());
                }
            };
            let block = batch.boundary.block_number;
            let size = batch.approx_size_bytes();
            debug!(block, size_bytes = size, "applying block delta");
            if let Err(e) = self.apply(&batch) {
                // Report this clearly on both channels: tracing for
                // production, and stderr unconditionally. A halted state
                // writer strands every consumer of the snapshot channel.
                // They block instead of erroring, so a silent failure
                // here is hard to find.
                eprintln!("kardamom-state-writer HALTING: block {block} apply failed: {e}");
                error!(block, error = %e, "block apply failed; halting writer");
                return Err(e);
            }
            // Publish the snapshot after this block. `SnapshotHandle::publish`
            // drops the old snapshot, which releases its read-only transaction.
            match StateSnapshot::open(&self.env) {
                Ok(snap) => self.snapshot_handle.publish(snap),
                Err(e) => {
                    eprintln!(
                        "kardamom-state-writer HALTING: snapshot open failed after block {block}: {e}"
                    );
                    warn!(block, error = %e, "snapshot open failed after commit");
                    return Err(e);
                }
            }
        }
    }

    fn apply(&self, batch: &WriteBatch) -> Result<(), StateError> {
        let timing = std::env::var_os("KARDAMOM_WRITER_TIMING").is_some();
        let t0 = std::time::Instant::now();
        let txn = self.env.raw().begin_rw_sync()?;

        let accounts = txn.open_db(Some(TABLE_ACCOUNTS))?;
        let storage = txn.open_db(Some(TABLE_STORAGE))?;
        let code = txn.open_db(Some(TABLE_CODE))?;
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        let receipts = txn.open_db(Some(TABLE_RECEIPTS))?;
        let tx_hash_index = txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
        let meta = txn.open_db(Some(TABLE_META))?;
        let t_open = t0.elapsed();

        // --- storage ---
        // Write storage first, so the trie-aware path can read an
        // account's current slots when it recomputes that account's
        // storage_root.
        //
        // Upstream `StorageChange.key` is `B256`, matching the
        // `StateDatabase` trait signature. The executor writes every slot
        // as an absolute value. There are no tombstones: writing
        // `U256::ZERO` means the slot is now zero.
        //
        // This uses a cursor with sorted input. `BlockDelta` vectors come
        // from `BTreeMap` iteration, so keys ascend. A cursor upsert then
        // walks down the tree from its previous position, instead of
        // from the root. In one measurement, this was the difference
        // between a writer that keeps pace with the execution pipeline
        // and one running twice as slow.
        let t1 = std::time::Instant::now();
        {
            let mut cur = txn.cursor(storage)?;
            for change in &batch.delta.storage {
                let key = encode_storage_key(change.address, change.key);
                cur.put(
                    &key,
                    &encode_storage_value(change.value),
                    WriteFlags::UPSERT,
                )?;
            }
        }
        let t_storage = t1.elapsed();

        // --- accounts ---
        // The `accounts` table feeds revm reads: nonce, balance, and
        // code_hash. It does not carry a meaningful `storage_root`. The
        // state trie keeps the canonical per-account storage root in
        // `hashed_accounts` (see `crate::trie`). This code always
        // persists `storage_root` as ZERO here, regardless of trie mode.
        let t2 = std::time::Instant::now();
        {
            let mut cur = txn.cursor(accounts)?;
            for change in &batch.delta.accounts {
                let key = encode_account_key(change.address);
                let v = AccountValue {
                    nonce: change.nonce,
                    balance: change.balance,
                    code_hash: change.code_hash,
                    storage_root: B256::ZERO,
                };
                cur.put(&key, &encode_account_value(&v), WriteFlags::UPSERT)?;
            }
        }
        let t_accounts = t2.elapsed();

        // --- code ---
        for entry in &batch.delta.code {
            let key = encode_code_key(entry.code_hash);
            // Code is content-addressed. NO_OVERWRITE skips a redundant write.
            match txn.put(code, key, &entry.code, WriteFlags::NO_OVERWRITE) {
                Ok(()) => {}
                Err(signet_libmdbx::MdbxError::KeyExist) => {} // Duplicate code. Fine.
                Err(e) => return Err(e.into()),
            }
        }

        // --- headers ---
        let header = HeaderValue {
            end_tx_idx: batch.boundary.end_tx_idx,
            l2_timestamp: batch.boundary.l2_timestamp,
            l1_origin: batch.boundary.l1_origin,
        };
        txn.put(
            headers,
            encode_block_key(batch.boundary.block_number),
            encode_header_value(&header),
            WriteFlags::UPSERT,
        )?;

        // --- receipts and tx_hash_index ---
        // For each receipt, write the receipt at its BPosition key, and
        // set tx_hash_index[receipt.tx_hash] = receipt.tx_idx. This lets
        // a caller serve eth_getTransactionReceipt(hash) with two reads:
        // StateDatabase::get_tx_position(hash), then
        // StateDatabase::get_receipt(pos).
        let t3 = std::time::Instant::now();
        {
            // Receipts arrive in ascending BPosition order, so use a cursor.
            let mut cur = txn.cursor(receipts)?;
            for r in &batch.delta.receipts {
                let pos_key = encode_b_position(r.tx_idx);
                cur.put(&pos_key, &encode_receipt_value(r), WriteFlags::UPSERT)?;
            }
            // The hash index's keys are random. Sort them first, so the
            // cursor gets the same locality benefit.
            let mut hk: Vec<([u8; 32], [u8; 8])> = batch
                .delta
                .receipts
                .iter()
                .map(|r| {
                    (
                        encode_tx_hash_key(r.tx_hash),
                        encode_tx_hash_value(r.tx_idx),
                    )
                })
                .collect();
            hk.sort_unstable_by_key(|e| e.0);
            let mut cur = txn.cursor(tx_hash_index)?;
            for (k, v) in &hk {
                cur.put(k, v, WriteFlags::UPSERT)?;
            }
        }
        let t_receipts = t3.elapsed();

        // --- meta cursors (last) ---
        txn.put(
            meta,
            KEY_LAST_COMMITTED_BLOCK,
            encode_u64(batch.boundary.block_number),
            WriteFlags::UPSERT,
        )?;
        txn.put(
            meta,
            KEY_LAST_COMMITTED_END_TX_POSITION,
            encode_b_position(batch.boundary.end_tx_idx),
            WriteFlags::UPSERT,
        )?;
        txn.put(
            meta,
            KEY_LAST_FSYNCED_B_POSITION,
            encode_b_position(batch.boundary.end_tx_idx),
            WriteFlags::UPSERT,
        )?;

        // --- state root (trie-aware only) ---
        // Advance the canonical Ethereum MPT world-state root
        // incrementally, and persist it in the same transaction. This
        // makes the root advance atomically with the state. `ShadowCheck`
        // also rebuilds the root from scratch at a sampling interval, and
        // stops the writer on a mismatch, as a canary for walker bugs.
        if self.trie_mode != TrieMode::Off {
            let tables = trie::TrieTables::open(&txn)?;
            let root = trie::update_for_block(&txn, &tables, &batch.delta)?;
            if let TrieMode::ShadowCheck { every_n } = self.trie_mode
                && every_n != 0
                && batch.boundary.block_number.is_multiple_of(every_n)
            {
                let rebuilt = trie::rebuild_root(&txn, &tables)?;
                metrics::counter!("kardamom_state_trie_shadow_checks_total").increment(1);
                if rebuilt != root {
                    metrics::counter!("kardamom_state_trie_shadow_mismatch_total").increment(1);
                    error!(
                        block = batch.boundary.block_number,
                        %root, %rebuilt, "trie shadow-check MISMATCH — halting writer"
                    );
                    return Err(StateError::ShadowMismatch {
                        block: batch.boundary.block_number,
                        incremental: root,
                        rebuilt,
                    });
                }
            }
            txn.put(meta, KEY_STATE_ROOT, encode_b256(root), WriteFlags::UPSERT)?;
        }

        let t4 = std::time::Instant::now();
        txn.commit()?;
        if timing {
            eprintln!(
                "writer apply block {}: open {:?} storage {:?} accounts {:?} receipts {:?} commit {:?} (n: sto {} acc {} rcpt {})",
                batch.boundary.block_number,
                t_open,
                t_storage,
                t_accounts,
                t_receipts,
                t4.elapsed(),
                batch.delta.storage.len(),
                batch.delta.accounts.len(),
                batch.delta.receipts.len(),
            );
        }
        Ok(())
    }
}

fn ensure_schema_version(env: &StateEnv) -> Result<(), StateError> {
    let txn = env.raw().begin_rw_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    match crate::meta::read_meta_u32(&txn, meta, KEY_SCHEMA_VERSION)? {
        None => {
            txn.put(
                meta,
                KEY_SCHEMA_VERSION,
                encode_u32(SCHEMA_VERSION),
                WriteFlags::UPSERT,
            )?;
        }
        Some(on_disk) => {
            if on_disk != SCHEMA_VERSION {
                drop(txn);
                return Err(StateError::Recovery(format!(
                    "schema version mismatch: on-disk={on_disk}, code={SCHEMA_VERSION}"
                )));
            }
        }
    }
    txn.commit()?;
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod trie_writer_tests;
