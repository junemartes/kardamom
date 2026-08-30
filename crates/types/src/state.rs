//! State-access traits. The `StateDatabase` trait is compatible with
//! `revm::Database` in spirit; this crate does not depend on revm. The
//! sequential executor consumes any implementor. The state writer ships
//! the libmdbx-backed one.

use alloy_primitives::{Address, B256, U256};
use bytes::Bytes;

use crate::position::BPosition;
use crate::receipt::Receipt;

/// Errors a state implementation may surface. Concrete crates wrap their own.
pub trait StateError: core::error::Error + Send + Sync + 'static {}

/// Read-only state access. A "snapshot" is a point-in-time view that does not
/// observe writes made by later blocks.
pub trait StateDatabase: Send + Sync {
    type Error: StateError;

    /// Returns `Some((nonce, balance, code_hash))` for an existing account, or
    /// `None` if the account does not exist.
    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error>;
    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error>;
    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, Self::Error>;

    /// Receipt lookup by canonical position.
    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error>;

    /// tx_hash to BPosition lookup. This is the `tx_hash_index` table the
    /// state writer maintains.
    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error>;

    /// Open an independent read view anchored at the same state as `self`.
    /// In mdbx this is a fresh read-only transaction at the same MVCC
    /// anchor. Sibling worker threads then read without contending on this
    /// view's backend cursor. Sharing one mdbx snapshot across W workers
    /// serializes their reads; the Block-STM benchmarks measured this as
    /// slower than running sequentially at w=4.
    ///
    /// Returns `None` when the backend cannot mint a new view, or cannot
    /// prove the fresh view anchors at the same state (for example, the
    /// writer advanced mid-mint). Callers then share `self` instead, which
    /// is always correct, only serialized. The blanket `&T` impl keeps this
    /// default, because it cannot return an owned `&T` from a mint.
    /// Parallel strategies hold a concrete `S`, so the default always means
    /// "share".
    fn fork_view(&self) -> Option<Self>
    where
        Self: Sized,
    {
        None
    }
}
/// A borrowed database is a database. This lets an [`ExecScope`]-style
/// owner hold either an owned snapshot (the executor's per-block scope) or
/// a borrow (a compat wrapper, or validator batches) behind one generic type.
impl<T: StateDatabase> StateDatabase for &T {
    type Error = T::Error;
    fn basic(&self, address: Address) -> Result<Option<(u64, U256, B256)>, Self::Error> {
        (**self).basic(address)
    }
    fn storage(&self, address: Address, key: B256) -> Result<U256, Self::Error> {
        (**self).storage(address, key)
    }
    fn code_by_hash(&self, code_hash: B256) -> Result<Bytes, Self::Error> {
        (**self).code_by_hash(code_hash)
    }
    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        (**self).get_receipt(pos)
    }
    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        (**self).get_tx_position(tx_hash)
    }
}

/// Source of fresh post-block state snapshots. The executor calls
/// [`SnapshotSource::snapshot_after`] when the state writer signals that a
/// block is durable.
pub trait SnapshotSource: Send + Sync {
    type Db: StateDatabase;

    fn snapshot_after(&self, block_number: u64) -> Self::Db;
}
