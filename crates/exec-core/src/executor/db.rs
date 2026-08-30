//! Snapshot-backed `revm::DatabaseRef` adapters, and the shared
//! view-composition primitive ([`seed_cache_layer`]). Both the tx path
//! ([`super::ExecScope`]) and the deposit path
//! ([`super::execute_deposit_tx`]) build on this.

use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::{Address, B256, U256};
use kardamom_types::StateDatabase;
use revm::database::{CacheDB, DatabaseRef};
use revm::primitives::KECCAK_EMPTY;
use revm::state::{AccountInfo, Bytecode};

use alloc::format;
use alloc::string::{String, ToString};

use crate::delta::PendingDelta;

/// A `revm::DatabaseRef` adapter for a `StateDatabase` snapshot. This is
/// read-only. Writes go through revm's per-tx state journal, returned by
/// `transact`.
pub struct SnapshotRef<'a, S: StateDatabase> {
    pub inner: &'a S,
}

/// An owned variant of [`SnapshotRef`]. [`super::ExecScope`] must be
/// storable across the actor's loop iterations, so it owns its snapshot.
/// (`S` can still be a `&T`, through the blanket `StateDatabase for &T`
/// impl.)
pub struct SnapshotDb<S: StateDatabase> {
    pub inner: S,
}

impl<S: StateDatabase> DatabaseRef for SnapshotDb<S> {
    type Error = StateRefError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        SnapshotRef { inner: &self.inner }.basic_ref(address)
    }
    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        SnapshotRef { inner: &self.inner }.code_by_hash_ref(code_hash)
    }
    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        SnapshotRef { inner: &self.inner }.storage_ref(address, index)
    }
    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        SnapshotRef { inner: &self.inner }.block_hash_ref(number)
    }
}

impl<S: StateDatabase> DatabaseRef for SnapshotRef<'_, S> {
    type Error = StateRefError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let a = self
            .inner
            .basic(address)
            .map_err(|e| StateRefError(e.to_string()))?;
        Ok(a.map(|(nonce, balance, code_hash)| AccountInfo {
            balance,
            nonce,
            // Genesis-seeded EOAs carry `code_hash = B256::ZERO` in the
            // state DB. Revm's CacheDB normalizes zero code hashes to
            // KECCAK_EMPTY when accounts pass through an execution scope
            // (`CacheDB::insert_contract`). Without normalizing here too,
            // the code_hash an account's write set carries would depend on
            // where it was read from: the mdbx snapshot gives ZERO, but the
            // intra-scope commit cache gives KECCAK_EMPTY. The executor
            // batches per block, and the validator batches per BAL chunk.
            // So a fresh account touched by two txs that straddle a
            // validator batch boundary, but share an executor block, could
            // produce a false receipt divergence and a validator
            // fail-stop. Both spellings of "no code" must hash the same.
            code_hash: if code_hash == B256::ZERO {
                KECCAK_EMPTY
            } else {
                code_hash
            },
            account_id: None,
            code: None,
        }))
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        if code_hash == KECCAK_EMPTY {
            return Ok(Bytecode::default());
        }
        let raw = self
            .inner
            .code_by_hash(code_hash)
            .map_err(|e| StateRefError(e.to_string()))?;
        if raw.is_empty() {
            return Ok(Bytecode::default());
        }
        Ok(Bytecode::new_raw(AlloyBytes::from(raw)))
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        // kardamom-types' StateDatabase::storage takes a B256 key. Revm
        // uses U256. The two are equivalent: U256 is a 32-byte big-endian
        // integer, and B256 is a 32-byte buffer.
        let key = B256::from(index.to_be_bytes::<32>());
        self.inner
            .storage(address, key)
            .map_err(|e| StateRefError(e.to_string()))
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        // Consensus rule (version 0, pinned by phase 3): BLOCKHASH returns
        // the zero hash for every height. The kardamom-types StateDatabase
        // trait exposes no block_hash, and there is no ancestor cache.
        // Every execution profile (live executor, validator re-exec,
        // stateless guest) flows through this one adapter, so the rule
        // holds uniformly and a proof replays it exactly. Adding a real
        // ancestor chain is a deliberate consensus change (a new spec and
        // a coordinated release), not a wiring fix.
        Ok(B256::ZERO)
    }
}

/// Concrete error type for `SnapshotRef`. This collapses the
/// `StateDatabase` associated error into a string, so the
/// `revm::Database` blanket impl sees a single concrete type.
#[derive(Debug, thiserror::Error)]
#[error("snapshot ref error: {0}")]
pub struct StateRefError(pub String);

impl revm::database_interface::DBErrorMarker for StateRefError {}

/// Seed one delta layer into a block cache. Later inserts overwrite
/// earlier ones, so seeding parent then delta composes the `snapshot`,
/// `parent`, `delta` view.
///
/// This is the one view-composition primitive. The tx path
/// ([`super::ExecScope::seed_layer`]) and the deposit path
/// ([`super::execute_deposit_tx`]) both go through it. The executor and
/// validator must compose the view the same way. A one-sided change here
/// causes a consensus divergence, not just a refactor.
pub(super) fn seed_cache_layer<DB: DatabaseRef>(
    cache: &mut CacheDB<DB>,
    layer: &PendingDelta,
) -> Result<(), String> {
    for (addr, (nonce, balance, code_hash)) in &layer.accounts {
        let code = layer
            .code
            .get(code_hash)
            .cloned()
            .filter(|b| !b.is_empty())
            .map(|b| Bytecode::new_raw(AlloyBytes::from(b)));
        cache.insert_account_info(
            *addr,
            AccountInfo {
                balance: *balance,
                nonce: *nonce,
                code_hash: *code_hash,
                account_id: None,
                code,
            },
        );
    }
    for ((addr, key), value) in &layer.storage {
        let u_key = U256::from_be_bytes::<32>(key.0);
        cache
            .insert_account_storage(*addr, u_key, *value)
            .map_err(|e| format!("seed layer storage: {e:?}"))?;
    }
    Ok(())
}
