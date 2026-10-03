//! A read-only snapshot: a long-lived mdbx read-only transaction that backs
//! `StateDatabase`.
//!
//! The mdbx read-only transaction is the MVCC anchor. While one stays alive,
//! the mdbx freelist will not reuse the pages it can reach. See
//! `geometry::HORIZON_BLOCKS` for the limit the writer enforces.
//!
//! The snapshot is `Clone` and cheap to share. The inner transaction lives
//! in an `Arc<SnapshotInner>`, so multiple consumers (the executor, the RPC
//! server) can read the same MVCC view without each opening a new
//! transaction slot.

use std::ops::ControlFlow;
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{BPosition, Receipt, StateDatabase};
use signet_libmdbx::tx::aliases::RoTxSync;
use signet_libmdbx::{Database, Environment};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_LAST_COMMITTED_END_TX_POSITION, KEY_STATE_ROOT,
    encode_b_position, get_decoded, read_meta_b_position, read_meta_b256, read_meta_u64,
};
use crate::schema::{
    TABLE_ACCOUNTS, TABLE_CODE, TABLE_HEADERS, TABLE_META, TABLE_RECEIPTS, TABLE_STORAGE,
    TABLE_TX_HASH_INDEX, decode_account_value, decode_header_value, decode_receipt_value,
    decode_storage_value, decode_tx_hash_value, encode_account_key, encode_block_key,
    encode_code_key, encode_storage_key, encode_tx_hash_key,
};
use crate::schema::{for_each_range, for_each_row};
use kardamom_types::receipt::TX_TYPE_DEPOSIT;

/// The references of one block: its boundary, and where the bytes of
/// each of its transactions are, in canonical order. The batcher rebuilds
/// the block's payload from them when the sealer no longer retains it.
/// This is the JSON of `kardamom_getBlockRefs`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockRefs {
    pub block_number: u64,
    /// The block's canonical end, as an index.
    pub end_tx_idx: u64,
    pub l1_origin: u64,
    pub l2_timestamp: u64,
    pub refs: Vec<BlockTxRef>,
}

/// One transaction of a block: its hash, its canonical position, and its
/// `TxRef` on the `tx_data` archive. Deposits are not listed; a payload
/// never carries them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockTxRef {
    pub tx_hash: B256,
    /// The canonical position, as an index.
    pub tx_idx: u64,
    pub shard_id: u8,
    pub session_id: i32,
    /// The archive position, as [`BPosition::as_index`] packs it.
    pub position: u64,
}

/// An MVCC snapshot of the state DB at exactly one block boundary.
///
/// It holds the underlying read-only transaction for its full lifetime.
/// Drop the snapshot to release it. The writer's horizon check then knows
/// it can reclaim older pages.
#[derive(Clone)]
pub struct StateSnapshot {
    inner: Arc<SnapshotInner>,
}

struct SnapshotInner {
    txn: RoTxSync,
    block_number: u64,
    // DBI handles are cached at open time. `Database` is `Copy` (a u32 plus
    // flags), so a struct read replaces the per-call `txn.open_db(...)`
    // round trip.
    accounts_db: Database,
    storage_db: Database,
    code_db: Database,
    receipts_db: Database,
    tx_hash_db: Database,
    // Keep a strong reference to the env, so it stays alive for as long as
    // the snapshot's read-only transaction does.
    env: Arc<Environment>,
}

impl StateSnapshot {
    /// Open a fresh snapshot anchored at the writer's current
    /// `last_committed_block` cursor.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if the read-only transaction or a table open
    /// fails.
    pub fn open(env: &StateEnv) -> Result<Self, StateError> {
        Self::open_on(env.env.clone())
    }

    /// Runs [`Self::open`] from the raw environment handle. This is the
    /// shared body of `open` and [`StateDatabase::fork_view`]. A fork
    /// creates its sibling transaction from the env the snapshot already
    /// keeps alive.
    fn open_on(env: Arc<Environment>) -> Result<Self, StateError> {
        let txn = env.begin_ro_sync()?;
        let meta = txn.open_db(Some(TABLE_META))?;
        // An absent key means genesis: no block has committed yet, so 0 is
        // the correct block number, not a missing-data sentinel.
        let block_number = read_meta_u64(&txn, meta, KEY_LAST_COMMITTED_BLOCK)?.unwrap_or(0);
        let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS))?;
        let storage_db = txn.open_db(Some(TABLE_STORAGE))?;
        let code_db = txn.open_db(Some(TABLE_CODE))?;
        let receipts_db = txn.open_db(Some(TABLE_RECEIPTS))?;
        let tx_hash_db = txn.open_db(Some(TABLE_TX_HASH_INDEX))?;
        Ok(Self {
            inner: Arc::new(SnapshotInner {
                txn,
                block_number,
                accounts_db,
                storage_db,
                code_db,
                receipts_db,
                tx_hash_db,
                env,
            }),
        })
    }

    /// The references of block `number`, or `None` for a block this
    /// snapshot has not committed.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if a read fails, or [`StateError::Recovery`]
    /// when the block cannot be rebuilt from references: a transaction
    /// without one (a row written before the reference existed), or a
    /// cross-chain message, whose remote-epoch record is in no archive.
    pub fn block_refs(&self, number: u64) -> Result<Option<BlockRefs>, StateError> {
        let txn = &self.inner.txn;
        let headers = txn.open_db(Some(TABLE_HEADERS))?;
        let Some(header) =
            get_decoded(txn, headers, &encode_block_key(number), decode_header_value)?
        else {
            return Ok(None);
        };
        // The block's records sit between the previous block's end and its
        // own. The chain's first block starts at position zero.
        let start = match number.checked_sub(1).filter(|n| *n > 0) {
            Some(previous) => {
                get_decoded(
                    txn,
                    headers,
                    &encode_block_key(previous),
                    decode_header_value,
                )?
                .ok_or_else(|| {
                    StateError::Recovery(format!(
                        "block {number} has no predecessor header; its start is unknown"
                    ))
                })?
                .end_tx_idx
            }
            None => BPosition::ZERO,
        };
        let mut refs = Vec::new();
        for_each_range(
            txn,
            self.inner.receipts_db,
            &encode_b_position(start),
            &encode_b_position(header.end_tx_idx),
            |_, v| {
                let receipt = decode_receipt_value(&v)?;
                refs.extend(self.tx_ref_of(&receipt)?);
                Ok(ControlFlow::Continue(()))
            },
        )?;
        Ok(Some(BlockRefs {
            block_number: number,
            end_tx_idx: header.end_tx_idx.as_index(),
            l1_origin: header.l1_origin,
            l2_timestamp: header.l2_timestamp,
            refs,
        }))
    }

    /// The reference of `receipt`'s transaction: `None` for a deposit,
    /// which no payload carries; an error for any other transaction
    /// without one.
    fn tx_ref_of(&self, receipt: &Receipt) -> Result<Option<BlockTxRef>, StateError> {
        let row = get_decoded(
            &self.inner.txn,
            self.inner.tx_hash_db,
            &encode_tx_hash_key(receipt.tx_hash),
            decode_tx_hash_value,
        )?;
        match (row.and_then(|r| r.data), receipt.tx_type) {
            (Some(data), _) => Ok(Some(BlockTxRef {
                tx_hash: receipt.tx_hash,
                tx_idx: receipt.tx_idx.as_index(),
                shard_id: data.shard_id,
                session_id: data.session_id,
                position: data.position.as_index(),
            })),
            (None, TX_TYPE_DEPOSIT) => Ok(None),
            (None, tx_type) => Err(StateError::Recovery(format!(
                "transaction {} (type {tx_type:#x}) in block {} has no archive reference; the \
                 block cannot be rebuilt from references",
                receipt.tx_hash, receipt.block_number
            ))),
        }
    }

    /// The snapshot's pinned read-only transaction. This is the read view
    /// for trie walks. Proof generation anchors against exactly this state
    /// (spec sections 3b and 3c).
    #[must_use]
    pub fn ro_txn(&self) -> &RoTxSync {
        &self.inner.txn
    }

    /// Returns the block number this snapshot is anchored at.
    #[must_use]
    pub fn block_number(&self) -> u64 {
        self.inner.block_number
    }

    /// The canonical Ethereum MPT world-state root committed at this
    /// snapshot's block.
    ///
    /// This is `None` on databases written by the plain, non-trie executor
    /// writer, which does not maintain a state root. The trie-aware writer
    /// (`StateWriter::spawn_with_trie`) writes it. See [`crate::trie`].
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if the `meta` table open or read fails.
    pub fn state_root(&self) -> Result<Option<B256>, StateError> {
        let meta = self.inner.txn.open_db(Some(TABLE_META))?;
        read_meta_b256(&self.inner.txn, meta, KEY_STATE_ROOT)
    }

    /// The canonical end position of the last block committed at this
    /// snapshot. Zero at genesis.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if the `meta` table open or read fails.
    pub fn end_tx_position(&self) -> Result<BPosition, StateError> {
        let meta = self.inner.txn.open_db(Some(TABLE_META))?;
        Ok(
            read_meta_b_position(&self.inner.txn, meta, KEY_LAST_COMMITTED_END_TX_POSITION)?
                .unwrap_or(BPosition::ZERO),
        )
    }

    /// Walk every account in address order and call `f(address, nonce,
    /// balance)` for each. The state mirror's rebuild scans a checkpoint
    /// this way. `f` returns `Break` to stop early.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] on a cursor or decode failure, or the error
    /// `f` returns.
    pub fn for_each_account(
        &self,
        mut f: impl FnMut(Address, u64, U256) -> Result<ControlFlow<()>, StateError>,
    ) -> Result<(), StateError> {
        for_each_row(&self.inner.txn, self.inner.accounts_db, |key, value| {
            let account = decode_account_value(&value)?;
            f(Address::from_slice(&key), account.nonce, account.balance)
        })
    }
}

impl StateDatabase for StateSnapshot {
    type Error = StateError;

    /// Create a sibling snapshot with its own read-only transaction.
    ///
    /// mdbx serializes reads through a transaction's cursors. So, if W
    /// workers share one snapshot, their reads run serially. The Block-STM
    /// benchmarks measured this as slower than sequential execution at
    /// w=4. `PoolHandle::begin_block_per_worker` exists for the same reason.
    ///
    /// The fresh transaction anchors at the current committed block. The
    /// fork is returned only if that block still equals this snapshot's
    /// block. If the writer advanced while the fork was being created,
    /// which is common under load with the depth-K commit pipeline, this
    /// method returns `None`. The caller then shares `self` instead. This
    /// is correct, only serialized.
    ///
    /// `Clone` does not do this: cloning shares the inner transaction.
    fn fork_view(&self) -> Option<Self> {
        let fork = Self::open_on(self.inner.env.clone()).ok()?;
        (fork.inner.block_number == self.inner.block_number).then_some(fork)
    }

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        let key = encode_account_key(address);
        let v = get_decoded(
            &self.inner.txn,
            self.inner.accounts_db,
            &key,
            decode_account_value,
        )?;
        Ok(v.map(|v| (v.nonce, v.balance, v.code_hash)))
    }

    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        let composite = encode_storage_key(address, key);
        let v = get_decoded(
            &self.inner.txn,
            self.inner.storage_db,
            &composite,
            decode_storage_value,
        )?;
        Ok(v.unwrap_or(U256::ZERO))
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, Self::Error> {
        let key = encode_code_key(code_hash);
        match self
            .inner
            .txn
            .get::<Vec<u8>>(self.inner.code_db.dbi(), &key)?
        {
            None => Ok(Bytes::new()),
            Some(b) => Ok(Bytes::from(b)),
        }
    }

    /// Load a receipt by its canonical `BPosition`. Returns `None` if no
    /// receipt was committed at that position.
    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        let key = encode_b_position(pos);
        get_decoded(
            &self.inner.txn,
            self.inner.receipts_db,
            &key,
            decode_receipt_value,
        )
    }

    /// Look up a `BPosition` by transaction hash. This supports
    /// `eth_getTransactionReceipt`.
    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        let key = encode_tx_hash_key(tx_hash);
        Ok(get_decoded(
            &self.inner.txn,
            self.inner.tx_hash_db,
            &key,
            decode_tx_hash_value,
        )?
        .map(|v| v.tx_idx))
    }
}
