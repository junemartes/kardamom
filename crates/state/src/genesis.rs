//! One-shot genesis seeding for a fresh env.
//!
//! The executor seeds the chain's genesis allocations (balances / nonces /
//! code) into a brand-new env *before* spawning the [`crate::writer::StateWriter`],
//! so the writer's initial published snapshot already reflects genesis and the
//! first block's transactions have account state to debit.
//!
//! Seeding is gated on the [`KEY_GENESIS_APPLIED`] meta flag so a restart against
//! an already-seeded env is a no-op — independent of the block cursor, since
//! genesis is "block 0" and `last_committed_block` stays 0 until the first real
//! block commits.

use alloy_primitives::B256;
use kardamom_types::{AccountChange, CodeEntry};
use signet_libmdbx::WriteFlags;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{KEY_GENESIS_APPLIED, encode_u32};
use crate::schema::{
    AccountValue, TABLE_ACCOUNTS, TABLE_CODE, TABLE_META, encode_account_key, encode_account_value,
    encode_code_key,
};

/// True if genesis has already been seeded into this env (the
/// [`KEY_GENESIS_APPLIED`] flag is present).
pub fn genesis_applied(env: &StateEnv) -> Result<bool, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    Ok(txn
        .get::<Vec<u8>>(meta.dbi(), KEY_GENESIS_APPLIED)?
        .is_some())
}

/// Idempotently seed genesis allocations into a fresh env.
///
/// Writes every account and code entry plus the [`KEY_GENESIS_APPLIED`] flag in a
/// single RW txn. Returns `Ok(true)` if it seeded, `Ok(false)` if genesis was
/// already applied (the txn is dropped without writing).
///
/// `storage_root` is persisted as `B256::ZERO` — v0 keeps no per-account MPT
/// roots, matching [`crate::writer::StateWriter`].
pub fn seed_genesis(
    env: &StateEnv,
    accounts: &[AccountChange],
    code: &[CodeEntry],
) -> Result<bool, StateError> {
    let txn = env.raw().begin_rw_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    if txn
        .get::<Vec<u8>>(meta.dbi(), KEY_GENESIS_APPLIED)?
        .is_some()
    {
        // Already seeded — abort the txn (drop) and report no-op.
        drop(txn);
        return Ok(false);
    }

    let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS))?;
    let code_db = txn.open_db(Some(TABLE_CODE))?;

    for change in accounts {
        let key = encode_account_key(change.address);
        let v = AccountValue {
            nonce: change.nonce,
            balance: change.balance,
            code_hash: change.code_hash,
            storage_root: B256::ZERO,
        };
        txn.put(
            accounts_db,
            key,
            encode_account_value(&v),
            WriteFlags::UPSERT,
        )?;
    }

    for entry in code {
        let key = encode_code_key(entry.code_hash);
        // Code is content-addressed; NO_OVERWRITE skips a redundant write when
        // two allocs share bytecode.
        match txn.put(code_db, key, &entry.code, WriteFlags::NO_OVERWRITE) {
            Ok(()) => {}
            Err(signet_libmdbx::MdbxError::KeyExist) => {}
            Err(e) => return Err(e.into()),
        }
    }

    // Flag last so a crash mid-seed leaves the env un-flagged and the next start
    // re-seeds cleanly.
    txn.put(meta, KEY_GENESIS_APPLIED, encode_u32(1), WriteFlags::UPSERT)?;
    txn.commit()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::{Durability, StateEnvBuilder};
    use crate::snapshot::StateSnapshot;
    use alloy_primitives::{Address, U256, keccak256};
    use bytes::Bytes;
    use kardamom_types::StateDatabase;

    fn temp_env() -> (tempfile::TempDir, StateEnv) {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .open()
            .unwrap();
        (dir, env)
    }

    #[test]
    fn seed_genesis_writes_accounts_and_code() {
        let (_dir, env) = temp_env();
        let addr = Address::from([0x11; 20]);
        let code = Bytes::from_static(b"\x60\x00");
        let code_hash = keccak256(&code);
        let accounts = vec![AccountChange {
            address: addr,
            nonce: 3,
            balance: U256::from(1_000u64),
            code_hash,
        }];
        let codes = vec![CodeEntry {
            code_hash,
            code: code.clone(),
        }];

        assert!(seed_genesis(&env, &accounts, &codes).unwrap());

        let snap = StateSnapshot::open(&env).unwrap();
        let (nonce, balance, ch) = snap.basic(addr).unwrap().unwrap();
        assert_eq!(nonce, 3);
        assert_eq!(balance, U256::from(1_000u64));
        assert_eq!(ch, code_hash);
        assert_eq!(snap.code_by_hash(code_hash).unwrap(), code);
    }

    #[test]
    fn seed_genesis_is_idempotent() {
        let (_dir, env) = temp_env();
        let addr = Address::from([0x22; 20]);
        let accounts = vec![AccountChange {
            address: addr,
            nonce: 0,
            balance: U256::from(7u64),
            code_hash: B256::ZERO,
        }];

        assert!(!genesis_applied(&env).unwrap());
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());
        assert!(genesis_applied(&env).unwrap());
        // Second call is a no-op.
        assert!(!seed_genesis(&env, &accounts, &[]).unwrap());

        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.basic(addr).unwrap().unwrap().1, U256::from(7u64));
    }

    #[test]
    fn seed_genesis_empty_alloc_sets_flag() {
        let (_dir, env) = temp_env();
        assert!(seed_genesis(&env, &[], &[]).unwrap());
        assert!(genesis_applied(&env).unwrap());
        assert!(!seed_genesis(&env, &[], &[]).unwrap());
    }
}
