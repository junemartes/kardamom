//! `MockStateDatabase`: an in-memory fixture that implements the
//! `StateDatabase` trait from `kardamom-types`. This module needs `std`
//! (`Arc` and `RwLock`), so guest builds compile without it.
//!
//! The trait and the `StateError` marker live in `kardamom-types`. This
//! crate defines no trait.
//!
//! `MockStateDatabase` is backed by an `Arc<RwLock<MockInner>>`, so a
//! `WriterApplyingQueue` (in `kardamom-engine`) and a
//! `MutatingSnapshotSource` can share state for multi-block integration
//! tests. Each clone, and each snapshot the source hands out, sees the
//! current state at read time. The writer applies deltas between blocks,
//! so by the time the exec thread calls `snapshot_after(N)` (after
//! `wait_committed(N)` returns), the snapshot already reflects block N's
//! writes.
//!
//! For tests that do not drive multiple blocks, `StaticSnapshotSource`
//! keeps the old behavior of returning an immutable snapshot.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{
    AccountChange, BPosition, BlockDelta, Receipt, SnapshotSource, StateDatabase, StateError,
    StorageChange,
};

/// Error type for `MockStateDatabase`. The mock never actually errors.
/// Every operation returns the in-memory value, or a default. This variant
/// exists so `StateDatabase::Error` has a concrete type.
#[derive(Debug, thiserror::Error)]
pub enum MockStateError {
    #[error("mock state database error: {0}")]
    Other(String),
}

impl StateError for MockStateError {}

/// Test fixture. Cheap to clone, since the inner state is an `Arc`.
/// Construct it with `MockStateDatabase::builder()`.
///
/// Reads take a read lock on the shared inner state. Snapshots returned by
/// `MutatingSnapshotSource` share the same lock, so they see whatever
/// writes the `WriterApplyingQueue` has applied at read time. Version-0
/// callers call `snapshot_after(N)` only after `wait_committed(N)`
/// returns, so the snapshot always reflects block N's deltas.
#[derive(Debug, Default, Clone)]
pub struct MockStateDatabase {
    inner: Arc<RwLock<MockInner>>,
}

#[derive(Debug, Default)]
struct MockInner {
    /// Address to (nonce, balance, code_hash). Matches the wire shape of
    /// `StateDatabase::basic`.
    accounts: BTreeMap<Address, (u64, U256, B256)>,
    /// Storage keyed by (address, slot).
    storage: BTreeMap<(Address, B256), U256>,
    /// Code keyed by code_hash.
    code: BTreeMap<B256, Bytes>,
    /// Receipts by canonical position. Tests that exercise the
    /// `get_receipt` path populate this. It is empty by default.
    receipts: BTreeMap<BPosition, Receipt>,
    /// Maps tx_hash to a `BPosition` index. Tests populate this. It is
    /// empty by default.
    tx_index: BTreeMap<B256, BPosition>,
}

impl MockStateDatabase {
    pub fn builder() -> MockStateDatabaseBuilder {
        MockStateDatabaseBuilder::default()
    }

    /// Apply a finalized `BlockDelta` to the inner state. The
    /// `WriterApplyingQueue` uses this. Receipts are also indexed by
    /// `tx_hash`, so the `get_tx_position` and `get_receipt` paths work
    /// after the writer commits.
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

    /// The mock has no per-view cursor to contend on, so a "fork" is just
    /// a plain clone that shares the inner map. This exists so tests
    /// exercise the forked code path that the mdbx snapshot takes in
    /// production.
    fn fork_view(&self) -> Option<Self> {
        Some(self.clone())
    }

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

/// Hermetic test fixture for `SnapshotSource`. It always hands back a
/// clone of the same `MockStateDatabase`, regardless of `block_number`.
/// Use it for single-block tests, where the snapshot-swap loop never sees
/// a difference between block N and block N+1 state.
///
/// For multi-block tests where later txs depend on earlier blocks' state,
/// use [`MutatingSnapshotSource`], paired with `kardamom-engine`'s
/// `WriterApplyingQueue`.
#[derive(Debug, Clone)]
pub struct StaticSnapshotSource(pub MockStateDatabase);

impl SnapshotSource for StaticSnapshotSource {
    type Db = MockStateDatabase;

    fn snapshot_after(&self, _block_number: u64) -> Self::Db {
        self.0.clone()
    }
}

/// A multi-block-aware `SnapshotSource`. It hands back clones of a shared
/// `MockStateDatabase`, whose state `kardamom-engine`'s
/// `WriterApplyingQueue` mutates as the exec thread closes blocks. Both
/// must wrap the same `MockStateDatabase` handle.
///
/// The clones share interior mutability through `Arc<RwLock<_>>`, so every
/// returned snapshot reads the current committed state. This is exactly
/// the post-commit view that the libmdbx-backed `SnapshotSource` will
/// produce in production.
#[derive(Debug, Clone)]
pub struct MutatingSnapshotSource(pub MockStateDatabase);

impl SnapshotSource for MutatingSnapshotSource {
    type Db = MockStateDatabase;

    fn snapshot_after(&self, _block_number: u64) -> Self::Db {
        self.0.clone()
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
}
