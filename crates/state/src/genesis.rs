//! One-shot genesis seeding for a fresh env.
//!
//! The executor seeds the chain's genesis allocations (balances, nonces,
//! and code) into a brand-new env before it spawns the
//! [`crate::writer::StateWriter`]. This way, the writer's first published
//! snapshot already reflects genesis, and the first block's transactions
//! have account state to debit.
//!
//! Seeding is gated on the [`KEY_GENESIS_APPLIED`] meta flag, so a restart
//! against an already-seeded env is a no-op. This check is independent of
//! the block cursor, because genesis is "block 0". `last_committed_block`
//! stays 0 until the first real block commits.

use alloy_primitives::{B256, keccak256};
use kardamom_types::{AccountChange, CodeEntry};
use signet_libmdbx::WriteFlags;

use crate::env::StateEnv;
use crate::error::StateError;
use crate::meta::{KEY_GENESIS_APPLIED, KEY_GENESIS_DIGEST, encode_u32};
use crate::schema::{
    AccountValue, TABLE_ACCOUNTS, TABLE_CODE, TABLE_META, encode_account_key, encode_account_value,
    encode_code_key,
};

/// An order-insensitive digest of a genesis allocation. This is a keccak
/// hash over the sorted account entries (address, nonce, balance,
/// code_hash) and the sorted code hashes. Bytecode is content-addressed,
/// so the hash pins the bytes.
///
/// The digest is stored at seed time under [`KEY_GENESIS_DIGEST`], and
/// compared on every later [`seed_genesis`] call. This makes startup fail
/// on a changed `--chain` file, or a node pointed at the wrong state
/// directory, instead of silently ignoring the change.
pub fn genesis_digest(accounts: &[AccountChange], code: &[CodeEntry]) -> B256 {
    let mut accs: Vec<&AccountChange> = accounts.iter().collect();
    accs.sort_by_key(|a| a.address);
    let mut codes: Vec<B256> = code.iter().map(|c| c.code_hash).collect();
    codes.sort();

    let mut buf = Vec::with_capacity(accs.len() * 92 + codes.len() * 32);
    for a in &accs {
        buf.extend_from_slice(a.address.as_slice());
        buf.extend_from_slice(&a.nonce.to_be_bytes());
        buf.extend_from_slice(&a.balance.to_be_bytes::<32>());
        buf.extend_from_slice(a.code_hash.as_slice());
    }
    for ch in &codes {
        buf.extend_from_slice(ch.as_slice());
    }
    keccak256(&buf)
}

/// Returns true if genesis has already been seeded into this env. This
/// checks whether the [`KEY_GENESIS_APPLIED`] flag is present.
pub fn genesis_applied(env: &StateEnv) -> Result<bool, StateError> {
    let txn = env.raw().begin_ro_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    Ok(txn
        .get::<Vec<u8>>(meta.dbi(), KEY_GENESIS_APPLIED)?
        .is_some())
}

/// Seed genesis allocations into a fresh env. This function is idempotent.
///
/// It writes every account and code entry, plus the
/// [`KEY_GENESIS_APPLIED`] flag and the [`KEY_GENESIS_DIGEST`], in a
/// single read-write transaction.
///
/// Returns `Ok(true)` if it seeded the env. Returns `Ok(false)` if genesis
/// was already applied, after checking that the supplied allocations
/// match the digest the env was seeded from. A mismatch, from a changed
/// `--chain` file or the wrong state directory, returns
/// [`StateError::GenesisMismatch`].
///
/// An env seeded before the digest existed gets the digest backfilled
/// from the supplied allocations on the next start.
///
/// `storage_root` is stored as `B256::ZERO`. v0 keeps no per-account MPT
/// roots, to match [`crate::writer::StateWriter`].
pub fn seed_genesis(
    env: &StateEnv,
    accounts: &[AccountChange],
    code: &[CodeEntry],
) -> Result<bool, StateError> {
    let digest = genesis_digest(accounts, code);
    let txn = env.raw().begin_rw_sync()?;
    let meta = txn.open_db(Some(TABLE_META))?;
    if txn
        .get::<Vec<u8>>(meta.dbi(), KEY_GENESIS_APPLIED)?
        .is_some()
    {
        // Already seeded. Check that the supplied allocation matches the
        // one this env was seeded from, then report a no-op.
        match crate::meta::read_meta_b256(&txn, meta, KEY_GENESIS_DIGEST)? {
            Some(stored) => {
                drop(txn);
                if stored != digest {
                    return Err(StateError::GenesisMismatch {
                        stored,
                        supplied: digest,
                    });
                }
            }
            None => {
                // This env was seeded before the digest existed. We cannot
                // verify the original allocation, so we backfill from the
                // supplied one. Future restarts can then detect drift.
                txn.put(
                    meta,
                    KEY_GENESIS_DIGEST,
                    crate::meta::encode_b256(digest),
                    WriteFlags::UPSERT,
                )?;
                txn.commit()?;
            }
        }
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
        // Code is content-addressed. NO_OVERWRITE skips a redundant write
        // when two allocations share the same bytecode.
        match txn.put(code_db, key, &entry.code, WriteFlags::NO_OVERWRITE) {
            Ok(()) => {}
            Err(signet_libmdbx::MdbxError::KeyExist) => {}
            Err(e) => return Err(e.into()),
        }
    }

    // Build the initial hashed-state mirror and account trie. This lets a
    // from-genesis validator start at the correct world-state root before
    // block 1. This step is harmless for the executor: its writer runs
    // with `TrieMode::Off` and ignores these tables.
    let trie_tables = crate::trie::TrieTables::open(&txn)?;
    let genesis_delta = kardamom_types::BlockDelta {
        block_number: 0,
        accounts: accounts.to_vec(),
        storage: Vec::new(),
        code: code.to_vec(),
        receipts: Vec::new(),
    };
    let root = crate::trie::update_for_block(&txn, &trie_tables, &genesis_delta)?;
    txn.put(
        meta,
        crate::meta::KEY_STATE_ROOT,
        crate::meta::encode_b256(root),
        WriteFlags::UPSERT,
    )?;

    // The flag and digest commit atomically with the allocations, in one
    // read-write transaction. A crash mid-seed aborts everything, so the
    // next start re-seeds cleanly. The put order within the transaction
    // does not matter.
    txn.put(
        meta,
        KEY_GENESIS_DIGEST,
        crate::meta::encode_b256(digest),
        WriteFlags::UPSERT,
    )?;
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
    fn seed_genesis_sets_canonical_state_root() {
        // A from-genesis validator must start at the correct world-state root
        // before block 1.
        let (_dir, env) = temp_env();
        let a1 = Address::repeat_byte(0x11);
        let a2 = Address::repeat_byte(0x22);
        let accounts = vec![
            AccountChange {
                address: a1,
                nonce: 1,
                balance: U256::from(100u64),
                code_hash: B256::ZERO,
            },
            AccountChange {
                address: a2,
                nonce: 0,
                balance: U256::from(50u64),
                code_hash: B256::ZERO,
            },
        ];
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());

        let want = crate::trie::state_root(accounts.iter().map(|a| {
            (
                a.address,
                crate::trie::AccountTrieParts {
                    nonce: a.nonce,
                    balance: a.balance,
                    code_hash: a.code_hash,
                    storage_root: B256::ZERO,
                },
            )
        }));
        let snap = StateSnapshot::open(&env).unwrap();
        assert_eq!(snap.state_root().unwrap(), Some(want));
        assert_ne!(want, crate::trie::empty_root());
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
    fn seed_genesis_rejects_changed_alloc() {
        // A restart with a different --chain file must fail clearly. It
        // must not silently keep the old genesis state.
        let (_dir, env) = temp_env();
        let accounts = vec![AccountChange {
            address: Address::repeat_byte(0x11),
            nonce: 1,
            balance: U256::from(100u64),
            code_hash: B256::ZERO,
        }];
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());

        let mut changed = accounts.clone();
        changed[0].balance = U256::from(999u64);
        let err = seed_genesis(&env, &changed, &[]).unwrap_err();
        assert!(matches!(err, StateError::GenesisMismatch { .. }), "{err}");

        // The same allocation in a different order is still the same genesis.
        let mut two = vec![
            AccountChange {
                address: Address::repeat_byte(0x22),
                nonce: 0,
                balance: U256::from(1u64),
                code_hash: B256::ZERO,
            },
            accounts[0].clone(),
        ];
        let (_dir2, env2) = temp_env();
        assert!(seed_genesis(&env2, &two, &[]).unwrap());
        two.reverse();
        assert!(!seed_genesis(&env2, &two, &[]).unwrap());
    }

    #[test]
    fn seed_genesis_backfills_missing_digest() {
        // An env seeded before the digest existed gets it backfilled on
        // the next start. After that, drift is detected.
        let (_dir, env) = temp_env();
        let accounts = vec![AccountChange {
            address: Address::repeat_byte(0x33),
            nonce: 2,
            balance: U256::from(5u64),
            code_hash: B256::ZERO,
        }];
        assert!(seed_genesis(&env, &accounts, &[]).unwrap());

        // Simulate a legacy env by removing the stored digest.
        {
            let txn = env.raw().begin_rw_sync().unwrap();
            let meta = txn.open_db(Some(TABLE_META)).unwrap();
            txn.del(meta, KEY_GENESIS_DIGEST, None).unwrap();
            txn.commit().unwrap();
        }

        // The first restart backfills the digest. Verification is not
        // possible yet.
        assert!(!seed_genesis(&env, &accounts, &[]).unwrap());
        // After that, a changed allocation is rejected.
        let mut changed = accounts.clone();
        changed[0].nonce = 9;
        assert!(matches!(
            seed_genesis(&env, &changed, &[]).unwrap_err(),
            StateError::GenesisMismatch { .. }
        ));
    }

    #[test]
    fn seed_genesis_empty_alloc_sets_flag() {
        let (_dir, env) = temp_env();
        assert!(seed_genesis(&env, &[], &[]).unwrap());
        assert!(genesis_applied(&env).unwrap());
        assert!(!seed_genesis(&env, &[], &[]).unwrap());
    }
}
