//! Test-only helpers shared across this crate's unit tests
//! (`#[cfg(test)]` modules under `src/`). Integration tests and benches
//! cannot see this module — they use `tests/common/mod.rs` instead.

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256};

use crate::env::{Durability, StateEnv, StateEnvBuilder};
use crate::trie::AccountTrieParts;

/// Open a fresh tempdir-backed env. Returns the tempdir guard, so the
/// caller controls its lifetime.
pub(crate) fn temp_env() -> (tempfile::TempDir, StateEnv) {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    (dir, env)
}

/// An `AccountTrieParts` with no code and no storage: `nonce` and
/// `balance` only. This is the recovery and trie-bootstrap tests'
/// repeated literal shape.
pub(crate) fn parts(nonce: u64, balance: u64) -> AccountTrieParts {
    AccountTrieParts {
        nonce,
        balance: U256::from(balance),
        code_hash: B256::ZERO,
        storage_root: B256::ZERO,
    }
}

/// Write `rows` (`address, nonce, balance`) into `accounts`, with no code
/// and no storage root, and commit. This is the plain-state shape a
/// trie-off writer leaves: only `accounts`, no hashed mirror, no trie
/// nodes — recovery and trie-bootstrap tests build fixtures this way.
pub(crate) fn put_plain_accounts(env: &StateEnv, rows: &[(Address, u64, u64)]) {
    let txn = env.raw().begin_rw_sync().unwrap();
    let accounts_db = txn.open_db(Some(crate::schema::TABLE_ACCOUNTS)).unwrap();
    for &(addr, nonce, balance) in rows {
        let v = crate::schema::AccountValue {
            nonce,
            balance: U256::from(balance),
            code_hash: B256::ZERO,
            storage_root: B256::ZERO,
        };
        txn.put(
            accounts_db,
            crate::schema::encode_account_key(addr),
            crate::schema::encode_account_value(&v),
            signet_libmdbx::WriteFlags::UPSERT,
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

/// The full-rebuild oracle root over an in-memory model: recompute each
/// account's storage root from scratch, then the world-state root over
/// the resulting account set. This is the reference the writer's and the
/// incremental trie's randomized tests both check the production
/// incremental path against.
pub(crate) fn model_state_root(
    accounts: &BTreeMap<Address, (u64, U256, B256)>,
    storage: &BTreeMap<Address, BTreeMap<B256, U256>>,
) -> B256 {
    crate::trie::state_root(accounts.iter().map(|(addr, &(nonce, balance, code_hash))| {
        let sroot = storage
            .get(addr)
            .map_or_else(crate::trie::empty_root, |slots| {
                crate::trie::storage_root(slots.iter().map(|(k, v)| (*k, *v)))
            });
        (
            *addr,
            AccountTrieParts {
                nonce,
                balance,
                code_hash,
                storage_root: sroot,
            },
        )
    }))
}
