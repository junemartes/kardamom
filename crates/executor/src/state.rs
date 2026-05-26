//! `MockStateDatabase`: in-memory fixture implementing the `StateDatabase`
//! trait defined in `kardamom-types`.
//!
//! The trait and `StateError` marker live in `kardamom-types`. No trait
//! definition lives in this crate.
//!
//! `MockStateDatabase` is backed by an `Arc<RwLock<MockInner>>` so a
//! `WriterApplyingQueue` and a `MutatingSnapshotSource` can share state for
//! multi-block integration tests. Each clone (and each snapshot the source
//! hands out) sees the *current* state at read time — the writer applies
//! deltas between blocks, so by the time the exec thread calls
//! `snapshot_after(N)` (after `wait_committed(N)` returns), the snapshot
//! already reflects block N's writes.
//!
//! For tests that don't drive multiple blocks, `StaticSnapshotSource` keeps
//! the previous behaviour of returning an immutable snapshot.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use types::{
    AccountChange, BPosition, BlockBoundary, BlockDelta, Receipt, SnapshotSource, StateDatabase,
    StateError, StorageChange,
};

use crate::actor::StateWriterQueue;
use crate::error::ExecutorError;

/// Error type for `MockStateDatabase`. The mock never actually errors — every
/// operation returns the in-memory value (or a default). The variant exists so
/// `StateDatabase::Error` is well-typed.
#[derive(Debug, thiserror::Error)]
pub enum MockStateError {
    #[error("mock state database error: {0}")]
    Other(String),
}

impl StateError for MockStateError {}

/// Test fixture. Cheap to clone (Arc-internal). Construct via
/// `MockStateDatabase::builder()`.
///
/// Reads acquire a read lock on the shared inner state. Snapshots returned
/// by `MutatingSnapshotSource` share the same lock — they observe whatever
/// writes the `WriterApplyingQueue` has applied at read time. v0 callers
/// only call `snapshot_after(N)` after `wait_committed(N)` has returned, so
/// the snapshot deterministically reflects block N's deltas.
#[derive(Debug, Default, Clone)]
pub struct MockStateDatabase {
    inner: Arc<RwLock<MockInner>>,
}

#[derive(Debug, Default)]
struct MockInner {
    /// Address → (nonce, balance, code_hash) — matches the wire shape of
    /// `StateDatabase::basic`.
    accounts: BTreeMap<Address, (u64, U256, B256)>,
    /// Storage keyed by (address, slot).
    storage: BTreeMap<(Address, B256), U256>,
    /// Code keyed by code_hash.
    code: BTreeMap<B256, Bytes>,
    /// Receipts by canonical position. Populated by tests that exercise the
    /// `get_receipt` path; defaults empty.
    receipts: BTreeMap<BPosition, Receipt>,
    /// tx_hash → BPosition index. Populated by tests; defaults empty.
    tx_index: BTreeMap<B256, BPosition>,
}

impl MockStateDatabase {
    pub fn builder() -> MockStateDatabaseBuilder {
        MockStateDatabaseBuilder::default()
    }

    /// Apply a finalized `BlockDelta` to the inner state. Used by
    /// `WriterApplyingQueue`. Receipts are also indexed by `tx_hash` so the
    /// `get_tx_position` / `get_receipt` paths work after writer commits.
    pub fn apply_block_delta(&self, delta: &BlockDelta) {
        let mut g = self.inner.write().expect("MockStateDatabase poisoned");
        for AccountChange {
            address,
            nonce,
            balance,
            code_hash,
        } in &delta.accounts
        {
            g.accounts.insert(*address, (*nonce, *balance, *code_hash));
        }
        for StorageChange {
            address,
            key,
            value,
        } in &delta.storage
        {
            g.storage.insert((*address, *key), *value);
        }
        for entry in &delta.code {
            g.code.insert(entry.code_hash, entry.code.clone());
        }
        for r in &delta.receipts {
            g.tx_index.insert(r.tx_hash, r.tx_idx);
            g.receipts.insert(r.tx_idx, r.clone());
        }
    }
}

#[derive(Debug, Default)]
pub struct MockStateDatabaseBuilder {
    accounts: BTreeMap<Address, (u64, U256, B256)>,
    storage: BTreeMap<(Address, B256), U256>,
    code: BTreeMap<B256, Bytes>,
    receipts: BTreeMap<BPosition, Receipt>,
    tx_index: BTreeMap<B256, BPosition>,
}

impl MockStateDatabaseBuilder {
    pub fn account(mut self, addr: Address, balance: U256, nonce: u64, code_hash: B256) -> Self {
        self.accounts.insert(addr, (nonce, balance, code_hash));
        self
    }

    pub fn storage(mut self, addr: Address, key: B256, value: U256) -> Self {
        self.storage.insert((addr, key), value);
        self
    }

    pub fn code(mut self, code_hash: B256, bytes: Bytes) -> Self {
        self.code.insert(code_hash, bytes);
        self
    }

    pub fn receipt(mut self, pos: BPosition, r: Receipt) -> Self {
        let tx_hash = r.tx_hash;
        self.tx_index.insert(tx_hash, pos);
        self.receipts.insert(pos, r);
        self
    }

    pub fn build(self) -> MockStateDatabase {
        MockStateDatabase {
            inner: Arc::new(RwLock::new(MockInner {
                accounts: self.accounts,
                storage: self.storage,
                code: self.code,
                receipts: self.receipts,
                tx_index: self.tx_index,
            })),
        }
    }
}

impl StateDatabase for MockStateDatabase {
    type Error = MockStateError;

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        let g = self.inner.read().expect("MockStateDatabase poisoned");
        Ok(g.accounts.get(&address).copied())
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, Self::Error> {
        let g = self.inner.read().expect("MockStateDatabase poisoned");
        Ok(g.code.get(&code_hash).cloned().unwrap_or_default())
    }

    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        let g = self.inner.read().expect("MockStateDatabase poisoned");
        Ok(g.storage
            .get(&(address, key))
            .copied()
            .unwrap_or(U256::ZERO))
    }

    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        let g = self.inner.read().expect("MockStateDatabase poisoned");
        Ok(g.receipts.get(&pos).cloned())
    }

    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        let g = self.inner.read().expect("MockStateDatabase poisoned");
        Ok(g.tx_index.get(&tx_hash).copied())
    }
}

/// Hermetic test fixture for `SnapshotSource`. Always hands back a clone of
/// the same `MockStateDatabase` regardless of `block_number`. Suitable for
/// single-block tests where the snapshot-swap loop never observes
/// block-N-vs-N+1 state differences.
///
/// For multi-block tests where later txs depend on earlier blocks' state,
/// use [`MutatingSnapshotSource`] paired with [`WriterApplyingQueue`].
#[derive(Debug, Clone)]
pub struct StaticSnapshotSource(pub MockStateDatabase);

impl SnapshotSource for StaticSnapshotSource {
    type Db = MockStateDatabase;

    fn snapshot_after(&self, _block_number: u64) -> Self::Db {
        self.0.clone()
    }
}

/// Multi-block-aware `SnapshotSource`. Hands back clones of a shared
/// `MockStateDatabase` whose state the [`WriterApplyingQueue`] mutates as
/// the exec thread closes blocks. Both must wrap the same
/// `MockStateDatabase` handle.
///
/// The clones share interior mutability via `Arc<RwLock<_>>`, so every
/// returned snapshot reads the *current* committed state — exactly the
/// post-commit view S6's libmdbx-backed `SnapshotSource` will produce in
/// production.
#[derive(Debug, Clone)]
pub struct MutatingSnapshotSource(pub MockStateDatabase);

impl SnapshotSource for MutatingSnapshotSource {
    type Db = MockStateDatabase;

    fn snapshot_after(&self, _block_number: u64) -> Self::Db {
        self.0.clone()
    }
}

/// State-writer queue that applies each submitted `BlockDelta` directly to
/// the shared `MockStateDatabase`. Pair with `MutatingSnapshotSource` so the
/// exec thread observes block N's writes when it opens its snapshot for
/// block N+1.
#[derive(Debug, Clone)]
pub struct WriterApplyingQueue {
    db: MockStateDatabase,
    /// Optional log of (BlockBoundary, BlockDelta) for tests that want to
    /// inspect what the writer received. Use `take_log()` to drain.
    log: Arc<RwLock<Vec<(BlockBoundary, BlockDelta)>>>,
}

impl WriterApplyingQueue {
    pub fn new(db: MockStateDatabase) -> Self {
        Self {
            db,
            log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn take_log(&self) -> Vec<(BlockBoundary, BlockDelta)> {
        std::mem::take(&mut self.log.write().expect("WriterApplyingQueue poisoned"))
    }
}

impl StateWriterQueue for WriterApplyingQueue {
    fn submit(&mut self, block: BlockBoundary, delta: BlockDelta) -> Result<(), ExecutorError> {
        self.db.apply_block_delta(&delta);
        self.log
            .write()
            .expect("WriterApplyingQueue poisoned")
            .push((block, delta));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_account_is_none() {
        let db = MockStateDatabase::default();
        assert_eq!(db.basic(Address::ZERO).unwrap(), None);
    }

    #[test]
    fn inserted_account_round_trips() {
        let addr = Address::from([1u8; 20]);
        let code_hash = B256::repeat_byte(0xAB);
        let db = MockStateDatabase::builder()
            .account(addr, U256::from(100u64), 7, code_hash)
            .build();
        let got = db.basic(addr).unwrap().unwrap();
        assert_eq!(got.0, 7);
        assert_eq!(got.1, U256::from(100u64));
        assert_eq!(got.2, code_hash);
    }

    #[test]
    fn missing_storage_returns_zero() {
        let db = MockStateDatabase::default();
        let v = db.storage(Address::ZERO, B256::ZERO).unwrap();
        assert_eq!(v, U256::ZERO);
    }

    #[test]
    fn snapshot_source_clones() {
        let addr = Address::from([1u8; 20]);
        let db = MockStateDatabase::builder()
            .account(addr, U256::from(1u64), 0, B256::ZERO)
            .build();
        let src = StaticSnapshotSource(db.clone());
        let snap = src.snapshot_after(7);
        assert!(snap.basic(addr).unwrap().is_some());
    }

    #[test]
    fn writer_applies_account_changes_and_snapshot_sees_them() {
        let addr = Address::from([1u8; 20]);
        let db = MockStateDatabase::builder()
            .account(addr, U256::from(1u64), 0, B256::ZERO)
            .build();
        let mut q = WriterApplyingQueue::new(db.clone());
        let src = MutatingSnapshotSource(db);

        let delta = BlockDelta {
            block_number: 1,
            accounts: vec![AccountChange {
                address: addr,
                nonce: 5,
                balance: U256::from(999u64),
                code_hash: B256::ZERO,
            }],
            storage: Vec::new(),
            code: Vec::new(),
            receipts: Vec::new(),
        };
        let boundary = BlockBoundary {
            block_number: 1,
            end_tx_idx: BPosition::ZERO,
            l2_timestamp: 0,
        };
        q.submit(boundary, delta).unwrap();

        let snap = src.snapshot_after(1);
        let (nonce, balance, _) = snap.basic(addr).unwrap().unwrap();
        assert_eq!(nonce, 5);
        assert_eq!(balance, U256::from(999u64));
    }
}
