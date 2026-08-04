//! State-access traits. The `StateDatabase` trait is `revm::Database`-compatible
//! (in spirit; we do not depend on revm here). S4's executor consumes any
//! implementor; S6 ships the libmdbx-backed one.

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

    /// tx_hash → BPosition (the `tx_hash_index` table in S6).
    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error>;
}
/// Borrowed databases are databases: lets [`ExecScope`]-style owners hold
/// either an owned snapshot (executor per-block scope) or a borrow
/// (compat wrapper, validator batches) behind one generic.
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
