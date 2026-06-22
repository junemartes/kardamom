//! Read-only snapshot: long-lived mdbx RO txn that backs `StateDatabase`.
//!
//! The mdbx RO txn is the MVCC anchor. As long as one of these is alive, the
//! mdbx freelist will not reuse the pages reachable from that snapshot — see
//! `geometry::HORIZON_BLOCKS` for the bound the writer enforces.
//!
//! The snapshot is `Clone` and cheap to share — the inner txn is held in an
//! `Arc<SnapshotInner>` so multiple consumers (executor, RPC) can read against
//! the exact same MVCC view without each opening a new txn slot.

use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{BPosition, Receipt, StateDatabase};
use signet_libmdbx::tx::aliases::RoTxSync;
use signet_libmdbx::{Database, Environment};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_STATE_ROOT, decode_b256, decode_u64, encode_b_position,
};
use crate::schema::{
    TABLE_ACCOUNTS, TABLE_CODE, TABLE_META, TABLE_RECEIPTS, TABLE_STORAGE, TABLE_TX_HASH_INDEX,
    decode_account_value, decode_receipt_value, decode_storage_value, decode_tx_hash_value,
    encode_account_key, encode_code_key, encode_storage_key, encode_tx_hash_key,
};

/// MVCC snapshot of the state DB at exactly one block boundary.
///
/// Holds the underlying RO txn for its full lifetime. Drop it to release the
/// snapshot — which the writer's horizon check uses to know it can reclaim
/// older pages.
#[derive(Clone)]
pub struct StateSnapshot {
    inner: Arc<SnapshotInner>,
}

struct SnapshotInner {
    txn: RoTxSync,
    block_number: u64,
    // DBI handles cached at open time. `Database` is `Copy` (u32 + flags), so
    // the per-call `txn.open_db(...)` round-trip is replaced by a struct read.
    accounts_db: Database,
    storage_db: Database,
    code_db: Database,
    receipts_db: Database,
    tx_hash_db: Database,
    // Keep the env Arc alive so the env doesn't outlive the snapshot's RO txn.
    _env: Arc<Environment>,
}

impl StateSnapshot {
    /// Open a fresh snapshot anchored at the writer's current
    /// `last_committed_block` cursor.
    pub fn open(env: &StateEnv) -> Result<Self, StateError> {
        let txn = env.raw().begin_ro_sync()?;
        let meta = txn.open_db(Some(TABLE_META))?;
        let block_number = match txn.get::<Vec<u8>>(meta.dbi(), KEY_LAST_COMMITTED_BLOCK)? {
            Some(bytes) => decode_u64(&bytes)?,
            None => 0,
        };
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
                _env: env.env.clone(),
            }),
        })
    }

    /// Returns the block number this snapshot is anchored at.
    pub fn block_number(&self) -> u64 {
        self.inner.block_number
    }

    /// The canonical Ethereum MPT world-state root committed at this snapshot's
    /// block, or `None` on databases written by the plain (non-trie) executor
    /// writer (which does not maintain a state root). Written by the trie-aware
    /// writer (`StateWriter::spawn_with_trie`); see [`crate::trie`].
    pub fn state_root(&self) -> Result<Option<B256>, StateError> {
        let meta = self.inner.txn.open_db(Some(TABLE_META))?;
        match self.inner.txn.get::<Vec<u8>>(meta.dbi(), KEY_STATE_ROOT)? {
            None => Ok(None),
            Some(bytes) => decode_b256(&bytes).map(Some),
        }
    }
}

impl StateDatabase for StateSnapshot {
    type Error = StateError;

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        let key = encode_account_key(address);
        match self
            .inner
            .txn
            .get::<Vec<u8>>(self.inner.accounts_db.dbi(), &key)?
        {
            None => Ok(None),
            Some(bytes) => {
                let v = decode_account_value(&bytes)?;
                Ok(Some((v.nonce, v.balance, v.code_hash)))
            }
        }
    }

    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        let composite = encode_storage_key(address, key);
        match self
            .inner
            .txn
            .get::<Vec<u8>>(self.inner.storage_db.dbi(), &composite)?
        {
            None => Ok(U256::ZERO),
            Some(bytes) => decode_storage_value(&bytes),
        }
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

    ///: load a Receipt by its canonical BPosition. Returns None if no
    /// receipt was committed at that position.
    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        let key = encode_b_position(pos);
        match self
            .inner
            .txn
            .get::<Vec<u8>>(self.inner.receipts_db.dbi(), &key)?
        {
            None => Ok(None),
            Some(bytes) => decode_receipt_value(&bytes).map(Some),
        }
    }

    ///: tx_hash → BPosition lookup. Feeds S1 `eth_getTransactionReceipt`.
    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        let key = encode_tx_hash_key(tx_hash);
        match self
            .inner
            .txn
            .get::<Vec<u8>>(self.inner.tx_hash_db.dbi(), &key)?
        {
            None => Ok(None),
            Some(bytes) => decode_tx_hash_value(&bytes).map(Some),
        }
    }
}
