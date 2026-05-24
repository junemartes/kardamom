//! Single writer thread: drains the [`WriteBatch`] channel, commits one mdbx
//! RW txn per block boundary, publishes new snapshots through the snapshot-
//! swap channel.
//!
//! ## Coordination with S4 executor
//!
//! S4's executor `submit(boundary: BlockBoundary, delta: BlockDelta)` is the
//! producer; this writer is the consumer. We accept the pair as
//! [`WriteBatch`] so the cursor we persist in the `meta` table tracks both
//! - `last_committed_block` = `boundary.block_number`
//! - `last_committed_end_tx_position` = `boundary.end_tx_idx`
//! - `last_fsynced_b_position` = `boundary.end_tx_idx` (same; the boundary
//!   `end_tx_idx` IS the last B position the executor committed through).
//!
//! `BlockDelta` lives in `kardamom-types` per S0 D-Sh1 — we never redefine it.

use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender};
use kardamom_types::{BlockBoundary, BlockDelta};
use signet_libmdbx::{WriteFlags, sys::EnvironmentKind};
use tracing::{debug, error, info, warn};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION, KEY_LAST_FSYNCED_B_POSITION,
    KEY_SCHEMA_VERSION, SCHEMA_VERSION, encode_b_position, encode_u32, encode_u64,
};
use crate::schema::{
    AccountValue, HeaderValue, TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META,
    TABLE_RECEIPTS, TABLE_STORAGE, TABLE_TX_HASH_INDEX, encode_account_key, encode_account_value,
    encode_b_position_key, encode_block_key, encode_code_key, encode_header_value,
    encode_receipt_value, encode_storage_key, encode_storage_value, encode_tx_hash_key,
    encode_tx_hash_value,
};
use crate::snapshot::StateSnapshot;
use crate::swap::{SnapshotHandle, SnapshotReceiver, channel as swap_channel};
use alloy_primitives::B256;

/// One block's worth of state changes submitted to the writer.
///
/// Pairs the boundary marker with its delta so the writer can persist all of
/// (a) the block-level cursors, (b) the per-key state mutations, and
/// (c) the per-tx receipts in a single atomic mdbx commit.
#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub boundary: BlockBoundary,
    pub delta: BlockDelta,
}

impl WriteBatch {
    pub fn new(boundary: BlockBoundary, delta: BlockDelta) -> Self {
        Self { boundary, delta }
    }

    /// Worst-case encoded size used by the writer to budget the mdbx txn.
    /// Heuristic only — not exact.
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

/// Handle returned by [`StateWriter::spawn`]. Drop to stop the writer thread
/// (which closes the delta sender and joins on the next loop iteration).
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

/// Single-writer state thread. Owns the only RW mdbx txn at a time.
pub struct StateWriter {
    env: StateEnv,
    delta_rx: Receiver<WriteBatch>,
    snapshot_handle: SnapshotHandle,
}

impl StateWriter {
    /// Spawn the writer on a dedicated OS thread.
    pub fn spawn(env: StateEnv) -> Result<WriterHandle, StateError> {
        // Bounded channel: HORIZON_BLOCKS deep. If the writer falls behind by
        // more than the version horizon, the executor will block here — at
        // which point the snapshot it holds is about to be invalidated anyway,
        // so blocking is the correct fail-fast.
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
        // Confirm the env kind is what we expect — we use the no-write-map
        // mode by default (the safest choice for arbitrary kernels and the
        // signet-libmdbx default).
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
                error!(block, error = %e, "block apply failed; halting writer");
                return Err(e);
            }
            // Publish the post-N snapshot. Old snapshot is dropped inside
            // SnapshotHandle::publish, which releases its RO txn.
            match StateSnapshot::open(&self.env) {
                Ok(snap) => self.snapshot_handle.publish(snap),
                Err(e) => {
                    warn!(block, error = %e, "snapshot open failed after commit");
                    return Err(e);
                }
            }
        }
    }

    fn apply(&self, batch: &WriteBatch) -> Result<(), StateError> {
        let txn = self.env.raw().begin_rw_sync()?;

        let accounts = txn.open_db(Some(TABLE_ACCOUNTS))?;
        let storage = txn.open_db(Some(TABLE_STORAGE))?;
        let code = txn.open_db(Some(TABLE_CODE))?;
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        let receipts = txn.open_db(Some(TABLE_RECEIPTS))?;
        let tx_hash_index = txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
        let meta = txn.open_db(Some(TABLE_META))?;

        // --- accounts ---
        // Upstream AccountChange = { address, nonce, balance, code_hash } — no
        // storage_root (v0 executor does not maintain per-account MPT roots,
        // see D-Sh11). We persist storage_root = B256::ZERO at v0; future
        // validator subsystems can recompute roots offline.
        for change in &batch.delta.accounts {
            let key = encode_account_key(change.address);
            let v = AccountValue {
                nonce: change.nonce,
                balance: change.balance,
                code_hash: change.code_hash,
                storage_root: B256::ZERO,
            };
            txn.put(accounts, key, encode_account_value(&v), WriteFlags::UPSERT)?;
        }

        // --- storage ---
        // Upstream StorageChange.key is B256 (matches the StateDatabase trait
        // signature). v0 executor writes every slot as an absolute value;
        // there are no tombstones at v0 — writing U256::ZERO means "slot
        // value is now zero".
        for change in &batch.delta.storage {
            let key = encode_storage_key(change.address, change.key);
            txn.put(
                storage,
                key,
                encode_storage_value(change.value),
                WriteFlags::UPSERT,
            )?;
        }

        // --- code ---
        for entry in &batch.delta.code {
            let key = encode_code_key(entry.code_hash);
            // code is content-addressed; NO_OVERWRITE saves a write.
            match txn.put(code, key, &entry.code, WriteFlags::NO_OVERWRITE) {
                Ok(()) => {}
                Err(signet_libmdbx::MdbxError::KeyExist) => {} // duplicate code, fine
                Err(e) => return Err(e.into()),
            }
        }

        // --- headers (D-Sh11: no state_root_commitment) ---
        let header = HeaderValue {
            end_tx_idx: batch.boundary.end_tx_idx,
            l2_timestamp: batch.boundary.l2_timestamp,
        };
        txn.put(
            headers,
            encode_block_key(batch.boundary.block_number),
            encode_header_value(&header),
            WriteFlags::UPSERT,
        )?;

        // --- receipts + tx_hash_index (D-Sh4) ---
        // For each receipt: write the receipt at its BPosition key, AND
        // populate tx_hash_index[receipt.tx_hash] = receipt.tx_idx so the S1
        // proxy can serve eth_getTransactionReceipt(hash) via two reads:
        //   StateDatabase::get_tx_position(hash) → StateDatabase::get_receipt(pos)
        for r in &batch.delta.receipts {
            let pos_key = encode_b_position_key(r.tx_idx);
            txn.put(
                receipts,
                pos_key,
                encode_receipt_value(r),
                WriteFlags::UPSERT,
            )?;
            txn.put(
                tx_hash_index,
                encode_tx_hash_key(r.tx_hash),
                encode_tx_hash_value(r.tx_idx),
                WriteFlags::UPSERT,
            )?;
        }

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

        txn.commit()?;
        Ok(())
    }
}

fn ensure_schema_version(env: &StateEnv) -> Result<(), StateError> {
    let txn = env.raw().begin_rw_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    match txn.get::<Vec<u8>>(meta.dbi(), KEY_SCHEMA_VERSION)? {
        None => {
            txn.put(
                meta,
                KEY_SCHEMA_VERSION,
                encode_u32(SCHEMA_VERSION),
                WriteFlags::UPSERT,
            )?;
        }
        Some(bytes) => {
            let on_disk = crate::meta::decode_u32(&bytes)?;
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
