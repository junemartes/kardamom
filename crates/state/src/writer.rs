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
//! `BlockDelta` lives in `kardamom-types` — we never redefine it.

use std::collections::{BTreeMap, BTreeSet};
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
    TABLE_RECEIPTS, TABLE_STORAGE, TABLE_TX_HASH_INDEX, decode_account_value, decode_storage_value,
    encode_account_key, encode_account_value, encode_block_key, encode_code_key,
    encode_header_value, encode_receipt_value, encode_storage_key, encode_storage_value,
    encode_tx_hash_key, encode_tx_hash_value,
};
use crate::snapshot::StateSnapshot;
use crate::swap::{SnapshotHandle, SnapshotReceiver, channel as swap_channel};
use crate::trie::{self, AccountTrieParts};
use alloy_primitives::{Address, B256, U256};

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
    /// When true, every block commit also maintains per-account `storage_root`s
    /// and computes the canonical Ethereum MPT world-state root, persisting it
    /// to `meta[KEY_STATE_ROOT]` in the same txn. Used by the validator; the
    /// sequencer-side executor leaves this off (v0: no state-root commitment).
    compute_trie: bool,
}

impl StateWriter {
    /// Spawn the plain writer (no state-root trie) on a dedicated OS thread.
    /// This is the sequencer-side executor's backend — behaviour is unchanged
    /// from before the trie support landed.
    pub fn spawn(env: StateEnv) -> Result<WriterHandle, StateError> {
        Self::spawn_inner(env, false)
    }

    /// Spawn the trie-aware writer: in addition to persisting state, each block
    /// commit advances the Ethereum MPT state root (see [`crate::trie`]) inside
    /// the same atomic txn. Used by the validator.
    pub fn spawn_with_trie(env: StateEnv) -> Result<WriterHandle, StateError> {
        Self::spawn_inner(env, true)
    }

    fn spawn_inner(env: StateEnv, compute_trie: bool) -> Result<WriterHandle, StateError> {
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
            compute_trie,
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

        // --- storage (written first so the trie-aware path can read an
        // account's current slots when recomputing its storage_root) ---
        // Upstream StorageChange.key is B256 (matches the StateDatabase trait
        // signature). The executor writes every slot as an absolute value;
        // there are no tombstones — writing U256::ZERO means "slot is now zero".
        for change in &batch.delta.storage {
            let key = encode_storage_key(change.address, change.key);
            txn.put(
                storage,
                key,
                encode_storage_value(change.value),
                WriteFlags::UPSERT,
            )?;
        }

        // --- accounts ---
        if self.compute_trie {
            // Trie-aware: every account touched this block (a basic-field change
            // OR a storage change) gets its storage_root recomputed from its
            // current slots and persisted, so the world-state root is canonical.
            let mut basics: BTreeMap<Address, (u64, U256, B256)> = BTreeMap::new();
            for c in &batch.delta.accounts {
                basics.insert(c.address, (c.nonce, c.balance, c.code_hash));
            }
            let mut touched: BTreeSet<Address> = basics.keys().copied().collect();
            for s in &batch.delta.storage {
                touched.insert(s.address);
            }
            for addr in touched {
                let prefix = addr.into_array();
                // Recompute this account's storage_root from all its slots.
                let storage_root = {
                    let mut cur = txn.cursor(storage)?;
                    let mut slots: Vec<(B256, U256)> = Vec::new();
                    let mut item = cur.set_range::<Vec<u8>, Vec<u8>>(&prefix)?;
                    while let Some((k, v)) = item {
                        if k.len() < 52 || k[..20] != prefix[..] {
                            break;
                        }
                        slots.push((B256::from_slice(&k[20..52]), decode_storage_value(&v)?));
                        item = cur.next::<Vec<u8>, Vec<u8>>()?;
                    }
                    trie::storage_root(slots)
                };
                // Basic fields: from the delta if the account changed this block,
                // otherwise carry forward the existing record (storage-only change).
                let (nonce, balance, code_hash) = match basics.get(&addr) {
                    Some(&b) => b,
                    None => match txn.get::<Vec<u8>>(accounts.dbi(), &prefix)? {
                        Some(bytes) => {
                            let v = decode_account_value(&bytes)?;
                            (v.nonce, v.balance, v.code_hash)
                        }
                        None => (0, U256::ZERO, B256::ZERO),
                    },
                };
                let v = AccountValue {
                    nonce,
                    balance,
                    code_hash,
                    storage_root,
                };
                txn.put(
                    accounts,
                    encode_account_key(addr),
                    encode_account_value(&v),
                    WriteFlags::UPSERT,
                )?;
            }
        } else {
            // Plain path (executor): AccountChange carries no storage_root; the
            // v0 executor does not maintain per-account MPT roots, so persist
            // storage_root = B256::ZERO.
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

        // --- headers ---
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

        // --- receipts + tx_hash_index ---
        // For each receipt: write the receipt at its BPosition key, AND
        // populate tx_hash_index[receipt.tx_hash] = receipt.tx_idx so the S1
        // proxy can serve eth_getTransactionReceipt(hash) via two reads:
        //   StateDatabase::get_tx_position(hash) → StateDatabase::get_receipt(pos)
        for r in &batch.delta.receipts {
            let pos_key = encode_b_position(r.tx_idx);
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

        // --- state root (trie-aware only) ---
        // Canonical Ethereum MPT world-state root over the full account set,
        // persisted in the same txn so the root advances atomically with state.
        if self.compute_trie {
            let root = {
                let mut cur = txn.cursor(accounts)?;
                let mut all: Vec<(Address, AccountTrieParts)> = Vec::new();
                let mut item = cur.first::<Vec<u8>, Vec<u8>>()?;
                while let Some((k, v)) = item {
                    let av = decode_account_value(&v)?;
                    all.push((
                        Address::from_slice(&k),
                        AccountTrieParts {
                            nonce: av.nonce,
                            balance: av.balance,
                            code_hash: av.code_hash,
                            storage_root: av.storage_root,
                        },
                    ));
                    item = cur.next::<Vec<u8>, Vec<u8>>()?;
                }
                trie::state_root(all)
            };
            txn.put(meta, KEY_STATE_ROOT, encode_b256(root), WriteFlags::UPSERT)?;
        }

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

#[cfg(test)]
mod trie_writer_tests {
    use super::*;
    use crate::env::{Durability, StateEnvBuilder};
    use crate::trie;
    use alloy_primitives::{Address, B256, U256};
    use kardamom_types::{AccountChange, BPosition, StorageChange};
    use std::collections::BTreeMap;

    fn acct(addr: u8, nonce: u64, balance: u64) -> AccountChange {
        AccountChange {
            address: Address::from([addr; 20]),
            nonce,
            balance: U256::from(balance),
            code_hash: B256::ZERO,
        }
    }

    fn slot(addr: u8, key: u8, value: u64) -> StorageChange {
        StorageChange {
            address: Address::from([addr; 20]),
            key: B256::from(U256::from(key)),
            value: U256::from(value),
        }
    }

    fn boundary(block: u64) -> BlockBoundary {
        BlockBoundary {
            block_number: block,
            end_tx_idx: BPosition::from_index(block),
            l2_timestamp: 1_700_000_000 + block,
        }
    }

    /// Independent oracle: compute the canonical state root from a pure in-memory
    /// model of the accounts + slots, recomputing storage roots from scratch.
    fn model_root(
        accounts: &BTreeMap<Address, (u64, U256, B256)>,
        storage: &BTreeMap<Address, BTreeMap<B256, U256>>,
    ) -> B256 {
        trie::state_root(accounts.iter().map(|(addr, &(nonce, balance, code_hash))| {
            let sroot = storage
                .get(addr)
                .map(|slots| trie::storage_root(slots.iter().map(|(k, v)| (*k, *v))))
                .unwrap_or_else(trie::empty_root);
            (
                *addr,
                trie::AccountTrieParts {
                    nonce,
                    balance,
                    code_hash,
                    storage_root: sroot,
                },
            )
        }))
    }

    #[test]
    fn trie_writer_root_matches_model_and_persists() {
        let dir = tempfile::tempdir().unwrap();

        // Build a 3-block scenario and a parallel model of the final state.
        let blocks = vec![
            BlockDelta {
                block_number: 1,
                accounts: vec![acct(0x11, 1, 1000), acct(0x22, 0, 500)],
                storage: vec![slot(0x11, 1, 7), slot(0x11, 2, 9)],
                code: vec![],
                receipts: vec![],
            },
            BlockDelta {
                block_number: 2,
                // 0x11 changes a slot (storage-only: NOT in accounts), 0x22 gets funds.
                accounts: vec![acct(0x22, 1, 800)],
                storage: vec![slot(0x11, 1, 0), slot(0x33, 5, 42)], // zero-out one slot, add account 0x33's slot
                code: vec![],
                receipts: vec![],
            },
            BlockDelta {
                block_number: 3,
                accounts: vec![acct(0x33, 2, 0), acct(0x11, 2, 1000)],
                storage: vec![slot(0x11, 2, 99)],
                code: vec![],
                receipts: vec![],
            },
        ];

        let mut model_accts: BTreeMap<Address, (u64, U256, B256)> = BTreeMap::new();
        let mut model_stor: BTreeMap<Address, BTreeMap<B256, U256>> = BTreeMap::new();

        // Submit all blocks, then shut down — `shutdown()` drains every queued
        // batch and joins, so on return block 3 is durably committed.
        {
            let env = StateEnvBuilder::new(dir.path())
                .durability(Durability::SafeNoSync)
                .open()
                .unwrap();
            let handle = StateWriter::spawn_with_trie(env).unwrap();
            for delta in &blocks {
                for s in &delta.storage {
                    model_stor
                        .entry(s.address)
                        .or_default()
                        .insert(s.key, s.value);
                }
                for a in &delta.accounts {
                    model_accts.insert(a.address, (a.nonce, a.balance, a.code_hash));
                }
                handle
                    .delta_tx
                    .send(WriteBatch::new(boundary(delta.block_number), delta.clone()))
                    .unwrap();
            }
            handle.shutdown().unwrap();
        }

        // Reopen and verify: the persisted root equals an independent rebuild of
        // the model, is non-empty, and survived the restart.
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.block_number(), 3);
        let persisted = snap.state_root().unwrap().expect("trie writer set a root");
        assert_eq!(persisted, model_root(&model_accts, &model_stor));
        assert_ne!(persisted, trie::empty_root());
    }
}
