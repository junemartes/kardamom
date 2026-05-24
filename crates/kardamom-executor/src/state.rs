//! `MockStateDatabase`: in-memory fixture implementing the `StateDatabase`
//! trait defined in `kardamom-types` (per S0 D-Sh1).
//!
//! The trait and `StateError` marker live in `kardamom-types`. No trait
//! definition lives in this crate.
//!
//! `MockStateDatabase` is a `BTreeMap`-backed implementation used by every
//! integration test and bench in this plan; it supports cheap snapshot
//! cloning (Arc + persistent maps not needed at v0 scale).

use std::collections::BTreeMap;
use std::sync::Arc;

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;
use kardamom_types::{BPosition, Receipt, SnapshotSource, StateDatabase, StateError};

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
#[derive(Debug, Default, Clone)]
pub struct MockStateDatabase {
    inner: Arc<MockInner>,
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
            inner: Arc::new(MockInner {
                accounts: self.accounts,
                storage: self.storage,
                code: self.code,
                receipts: self.receipts,
                tx_index: self.tx_index,
            }),
        }
    }
}

impl StateDatabase for MockStateDatabase {
    type Error = MockStateError;

    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        Ok(self.inner.accounts.get(&address).copied())
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, Self::Error> {
        Ok(self.inner.code.get(&code_hash).cloned().unwrap_or_default())
    }

    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        Ok(self
            .inner
            .storage
            .get(&(address, key))
            .copied()
            .unwrap_or(U256::ZERO))
    }

    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        Ok(self.inner.receipts.get(&pos).cloned())
    }

    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        Ok(self.inner.tx_index.get(&tx_hash).copied())
    }
}

/// Hermetic test fixture for `SnapshotSource`. Always hands back a clone of
/// the same `MockStateDatabase` regardless of `block_number`; the executor
/// snapshot-swap loop doesn't observe block-N-vs-N+1 state differences in
/// pure-CPU tests.
#[derive(Debug, Clone)]
pub struct StaticSnapshotSource(pub MockStateDatabase);

impl SnapshotSource for StaticSnapshotSource {
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
