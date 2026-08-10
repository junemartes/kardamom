//! Snapshot-backed `revm::DatabaseRef` adapters and the shared
//! view-composition primitive ([`seed_cache_layer`]) that both the tx path
//! ([`super::ExecScope`]) and the deposit path
//! ([`super::execute_deposit_tx`]) layer on.

use alloy_primitives::Bytes as AlloyBytes;
use alloy_primitives::{Address, B256, U256};
use kardamom_types::StateDatabase;
use revm::database::{CacheDB, DatabaseRef};
use revm::primitives::KECCAK_EMPTY;
use revm::state::{AccountInfo, Bytecode};

use alloc::format;
use alloc::string::{String, ToString};

use crate::delta::PendingDelta;

/// `revm::DatabaseRef` adapter for a `StateDatabase` snapshot. Reads only —
/// writes go through revm's per-tx state journal returned by `transact`.
pub struct SnapshotRef<'a, S: StateDatabase> {
    pub inner: &'a S,
}

/// Owned variant of [`SnapshotRef`]: [`super::ExecScope`] must be storable
/// across the actor's loop iterations, so it owns its snapshot (`S` can
/// still be a `&T` via the blanket `StateDatabase for &T` impl).
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
            // Genesis-seeded EOAs carry `code_hash = B256::ZERO` in the state
            // DB, and revm's CacheDB normalizes zero code hashes to
            // KECCAK_EMPTY when accounts pass through an execution scope
            // (`CacheDB::insert_contract`). Without normalizing HERE too, the
            // code_hash an account's write set carries depends on WHERE it was
            // read from — the mdbx snapshot (fresh scope ⇒ ZERO) vs the
            // intra-scope commit cache (⇒ KECCAK_EMPTY). The executor batches
            // per block and the validator per BAL chunk, so a fresh account's
            // first two txs straddling a validator batch boundary while
            // sharing an executor block produced a wsh-only receipt
            // "divergence" and a validator fail-stop (#159). Two spellings of
            // "no code" must hash identically.
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
        // kardamom-types' StateDatabase::storage takes a B256 key; revm uses
        // U256. The two are isomorphic — U256 is a 32-byte big-endian integer
        // and B256 is a 32-byte buffer.
        let key = B256::from(index.to_be_bytes::<32>());
        self.inner
            .storage(address, key)
            .map_err(|e| StateRefError(e.to_string()))
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        // The shipped kardamom-types StateDatabase trait does not expose
        // block_hash. v0 callers (BLOCKHASH opcode in executed contracts)
        // observe the zero hash — historically what an executor without an
        // ancestor cache returns until S6 wires one in.
        Ok(B256::ZERO)
    }
}

/// Concrete error type for `SnapshotRef`. We collapse the `StateDatabase`
/// associated error into a string so the `revm::Database` blanket impl can
/// see a single concrete type.
#[derive(Debug, thiserror::Error)]
#[error("snapshot ref error: {0}")]
pub struct StateRefError(pub String);

impl revm::database_interface::DBErrorMarker for StateRefError {}

/// Seed one delta layer into a block cache — later inserts overwrite, so
/// seeding parent-then-delta composes the `snapshot ∘ parent ∘ delta` view.
///
/// This is the ONE view-composition primitive: the tx path
/// ([`super::ExecScope::seed_layer`]) and the deposit path
/// ([`super::execute_deposit_tx`]) both go through it, and the executor and
/// validator MUST compose the view identically — a one-sided change here is
/// a consensus divergence, not a refactor.
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
