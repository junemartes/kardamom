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

use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{BPosition, Receipt, StateDatabase};
use signet_libmdbx::tx::aliases::RoTxSync;
use signet_libmdbx::{Database, Environment};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{
    KEY_LAST_COMMITTED_BLOCK, KEY_STATE_ROOT, encode_b_position, read_meta_b256, read_meta_u64,
};
use crate::schema::{
    TABLE_ACCOUNTS, TABLE_CODE, TABLE_META, TABLE_RECEIPTS, TABLE_STORAGE, TABLE_TX_HASH_INDEX,
    decode_account_value, decode_receipt_value, decode_storage_value, decode_tx_hash_value,
    encode_account_key, encode_code_key, encode_storage_key, encode_tx_hash_key,
};

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
    _env: Arc<Environment>,
}

impl StateSnapshot {
    /// Open a fresh snapshot anchored at the writer's current
    /// `last_committed_block` cursor.
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
                _env: env,
            }),
        })
    }

    /// The snapshot's pinned read-only transaction. This is the read view
    /// for trie walks. Proof generation anchors against exactly this state
    /// (spec sections 3b and 3c).
    pub fn ro_txn(&self) -> &RoTxSync {
        &self.inner.txn
    }

    /// Returns the block number this snapshot is anchored at.
    pub fn block_number(&self) -> u64 {
        self.inner.block_number
    }

    /// The canonical Ethereum MPT world-state root committed at this
    /// snapshot's block.
    ///
    /// This is `None` on databases written by the plain, non-trie executor
    /// writer, which does not maintain a state root. The trie-aware writer
    /// (`StateWriter::spawn_with_trie`) writes it. See [`crate::trie`].
    pub fn state_root(&self) -> Result<Option<B256>, StateError> {
        let meta = self.inner.txn.open_db(Some(TABLE_META))?;
        read_meta_b256(&self.inner.txn, meta, KEY_STATE_ROOT)
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
        let fork = Self::open_on(self.inner._env.clone()).ok()?;
        (fork.inner.block_number == self.inner.block_number).then_some(fork)
    }

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

    /// Load a receipt by its canonical `BPosition`. Returns `None` if no
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

    /// Look up a `BPosition` by transaction hash. This supports
    /// `eth_getTransactionReceipt`.
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
