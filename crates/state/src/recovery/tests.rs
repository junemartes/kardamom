use super::*;
use crate::env::StateEnvBuilder;
use crate::schema::{
    AccountValue, TABLE_ACCOUNTS, TABLE_STORAGE, encode_account_key, encode_account_value,
    encode_storage_key, encode_storage_value,
};
use alloy_primitives::{Address, B256, U256};
use signet_libmdbx::WriteFlags;

/// Adoption drill (#143): a plain trie-off state image (what an executor
/// checkpoint restores to) bootstraps a mirror + trie whose root matches
/// the pure full-rebuild oracle, and the probe flips accordingly.
#[test]
fn bootstrap_builds_trie_matching_oracle_on_trie_off_state() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();

    let addr_a = Address::repeat_byte(0x11);
    let addr_b = Address::repeat_byte(0x22);
    let slot = B256::repeat_byte(0x03);
    let slot_val = U256::from(7u64);

    // Write plain state the way a trie-off writer leaves it: accounts +
    // storage tables only, no hashed mirror, no trie nodes.
    {
        let txn = env.raw().begin_rw_sync().unwrap();
        let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
        for (addr, nonce, bal) in [(addr_a, 3u64, 100u64), (addr_b, 1, 55)] {
            let v = AccountValue {
                nonce,
                balance: U256::from(bal),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            };
            txn.put(
                accounts_db,
                encode_account_key(addr),
                encode_account_value(&v),
                WriteFlags::UPSERT,
            )
            .unwrap();
        }
        let storage_db = txn.open_db(Some(TABLE_STORAGE)).unwrap();
        txn.put(
            storage_db,
            encode_storage_key(addr_b, slot),
            encode_storage_value(slot_val),
            WriteFlags::UPSERT,
        )
        .unwrap();
        txn.commit().unwrap();
    }
    assert!(!has_trie(&env).unwrap(), "trie-off image must probe absent");

    let root = bootstrap_trie_from_state(&env).unwrap();
    assert!(
        has_trie(&env).unwrap(),
        "bootstrap must populate the mirror"
    );
    assert_ne!(root, crate::trie::empty_root());

    // Oracle: pure full rebuild over the same accounts, with B's storage
    // root computed from its one slot.
    let storage_root = crate::trie::storage_root([(slot, slot_val)]);
    let want = crate::trie::state_root([
        (
            addr_a,
            crate::trie::AccountTrieParts {
                nonce: 3,
                balance: U256::from(100u64),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            },
        ),
        (
            addr_b,
            crate::trie::AccountTrieParts {
                nonce: 1,
                balance: U256::from(55u64),
                code_hash: B256::ZERO,
                storage_root,
            },
        ),
    ]);
    assert_eq!(root, want, "bootstrap root must equal the rebuild oracle");

    // Idempotent: re-running lands on the same root.
    assert_eq!(bootstrap_trie_from_state(&env).unwrap(), root);
}

/// The REAL adopted-executor-checkpoint shape (validator-join chaos case
/// finding): every env carries the GENESIS-seeded mirror + trie (built by
/// seed_genesis, then never updated by a trie-off writer), so an adopted
/// image has a stale-at-genesis mirror under newer plain state. The
/// bootstrap must converge that to the oracle root of the CURRENT state —
/// and a presence probe alone would wrongly skip it (which is why
/// adoption is marker-signaled, not probed).
#[test]
fn bootstrap_corrects_genesis_stale_mirror_under_newer_state() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();
    let addr_a = Address::repeat_byte(0x11);
    let addr_b = Address::repeat_byte(0x22);

    // Genesis: A exists with nonce 0 / balance 1000. Seeds mirror + trie.
    let genesis = [kardamom_types::AccountChange {
        address: addr_a,
        nonce: 0,
        balance: U256::from(1000u64),
        code_hash: B256::ZERO,
    }];
    assert!(crate::genesis::seed_genesis(&env, &genesis, &[]).unwrap());
    assert!(has_trie(&env).unwrap(), "genesis seed builds the mirror");

    // Trie-off progress (what an executor does): A changes, B appears —
    // plain tables only, mirror left frozen at genesis.
    {
        let txn = env.raw().begin_rw_sync().unwrap();
        let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
        for (addr, nonce, bal) in [(addr_a, 7u64, 400u64), (addr_b, 2, 600)] {
            let v = AccountValue {
                nonce,
                balance: U256::from(bal),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            };
            txn.put(
                accounts_db,
                encode_account_key(addr),
                encode_account_value(&v),
                WriteFlags::UPSERT,
            )
            .unwrap();
        }
        txn.commit().unwrap();
    }
    // The trap: the probe still says "trie present" (it is — stale).
    assert!(has_trie(&env).unwrap());

    let root = bootstrap_trie_from_state(&env).unwrap();
    let want = crate::trie::state_root([
        (
            addr_a,
            crate::trie::AccountTrieParts {
                nonce: 7,
                balance: U256::from(400u64),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            },
        ),
        (
            addr_b,
            crate::trie::AccountTrieParts {
                nonce: 2,
                balance: U256::from(600u64),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            },
        ),
    ]);
    assert_eq!(
        root, want,
        "bootstrap must converge a stale mirror to the current-state oracle"
    );
}

/// The join must not just adopt — VERIFIED execution continues on top of
/// the bootstrapped trie. After bootstrap, applying the next block
/// INCREMENTALLY must land on the same root as a pure full rebuild over
/// the merged state (the property the validator's post-adoption
/// verification rides on).
#[test]
fn incremental_block_on_bootstrapped_trie_matches_oracle() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();
    let addr_a = Address::repeat_byte(0x11);
    let addr_c = Address::repeat_byte(0x33);

    {
        let txn = env.raw().begin_rw_sync().unwrap();
        let accounts_db = txn.open_db(Some(TABLE_ACCOUNTS)).unwrap();
        let v = AccountValue {
            nonce: 3,
            balance: U256::from(100u64),
            code_hash: B256::ZERO,
            storage_root: B256::ZERO,
        };
        txn.put(
            accounts_db,
            encode_account_key(addr_a),
            encode_account_value(&v),
            WriteFlags::UPSERT,
        )
        .unwrap();
        txn.commit().unwrap();
    }
    bootstrap_trie_from_state(&env).unwrap();

    // "Next block": A bumps its nonce, C appears.
    let delta = kardamom_types::BlockDelta {
        block_number: 1,
        accounts: vec![
            kardamom_types::AccountChange {
                address: addr_a,
                nonce: 4,
                balance: U256::from(90u64),
                code_hash: B256::ZERO,
            },
            kardamom_types::AccountChange {
                address: addr_c,
                nonce: 1,
                balance: U256::from(10u64),
                code_hash: B256::ZERO,
            },
        ],
        storage: Vec::new(),
        code: Vec::new(),
        receipts: Vec::new(),
    };
    let txn = env.raw().begin_rw_sync().unwrap();
    let tables = crate::trie::TrieTables::open(&txn).unwrap();
    let root = crate::trie::update_for_block(&txn, &tables, &delta).unwrap();
    txn.commit().unwrap();

    let want = crate::trie::state_root([
        (
            addr_a,
            crate::trie::AccountTrieParts {
                nonce: 4,
                balance: U256::from(90u64),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            },
        ),
        (
            addr_c,
            crate::trie::AccountTrieParts {
                nonce: 1,
                balance: U256::from(10u64),
                code_hash: B256::ZERO,
                storage_root: B256::ZERO,
            },
        ),
    ]);
    assert_eq!(
        root, want,
        "incremental block on a bootstrapped trie must match the oracle"
    );
}
