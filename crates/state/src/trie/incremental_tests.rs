//! The correctness gate: drive randomized blocks through the incremental walker
//! and assert the root matches the full-rebuild oracle after every block.

use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{Address, B256, U256, keccak256};
use signet_libmdbx::WriteFlags;

use super::cursor::{encode_account_leaf, get_hashed_account};
use super::{AccountTrieParts, PrefixSet, StateRoot, apply_trie_updates, empty_root};
use crate::env::{Durability, StateEnv, StateEnvBuilder};
use crate::schema::{
    TABLE_ACCOUNT_TRIE, TABLE_HASHED_ACCOUNTS, TABLE_HASHED_STORAGE, TABLE_STORAGE_TRIE,
};

/// Basic (non-storage_root) account fields tracked by the model.
#[derive(Clone, Copy)]
struct Basic {
    nonce: u64,
    balance: U256,
    code_hash: B256,
}

/// A single block's changes.
#[derive(Default)]
struct Block {
    acct_upserts: Vec<(Address, Basic)>,
    acct_deletes: Vec<Address>,
    storage: Vec<(Address, B256, U256)>, // value 0 == delete slot
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Apply a block's changes to the trie tables via the incremental walker and
/// return the new world-state root.
fn apply_block(env: &StateEnv, b: &Block) -> B256 {
    let txn = env.raw().begin_rw_sync().unwrap();
    let at = txn.open_db(Some(TABLE_ACCOUNT_TRIE)).unwrap();
    let st = txn.open_db(Some(TABLE_STORAGE_TRIE)).unwrap();
    let ha = txn.open_db(Some(TABLE_HASHED_ACCOUNTS)).unwrap();
    let hs = txn.open_db(Some(TABLE_HASHED_STORAGE)).unwrap();

    let mut touched: BTreeSet<Address> = BTreeSet::new();
    let mut new_sroot: BTreeMap<Address, B256> = BTreeMap::new();

    // --- storage tries first ---
    let mut stor_by_acct: BTreeMap<Address, Vec<(B256, U256)>> = BTreeMap::new();
    for (addr, slot, val) in &b.storage {
        stor_by_acct.entry(*addr).or_default().push((*slot, *val));
    }
    for (addr, changes) in &stor_by_acct {
        let ah = keccak256(addr);
        let mut changed = Vec::new();
        for (slot, val) in changes {
            let sh = keccak256(slot);
            changed.push(sh);
            let mut key = ah.as_slice().to_vec();
            key.extend_from_slice(sh.as_slice());
            if val.is_zero() {
                let _ = txn.del(hs, key, None);
            } else {
                txn.put(hs, key, val.to_be_bytes::<32>(), WriteFlags::UPSERT)
                    .unwrap();
            }
        }
        let ps = PrefixSet::from_b256s(changed);
        let (sroot, sup) = StateRoot::storage_root_incremental(&txn, st, hs, ah, &ps).unwrap();
        apply_trie_updates(&txn, st, Some(&ah), &sup).unwrap();
        new_sroot.insert(*addr, sroot);
        touched.insert(*addr);
    }

    for (addr, _) in &b.acct_upserts {
        touched.insert(*addr);
    }
    for addr in &b.acct_deletes {
        touched.insert(*addr);
    }

    // --- hashed_accounts rows for every touched account ---
    for addr in &touched {
        let ah = keccak256(addr);
        if b.acct_deletes.contains(addr) {
            let _ = txn.del(ha, ah.as_slice(), None);
            del_prefix(&txn, hs, ah.as_slice());
            del_prefix(&txn, st, ah.as_slice());
            continue;
        }
        let existing = get_hashed_account(&txn, ha, &ah).unwrap();
        let basic = b
            .acct_upserts
            .iter()
            .find(|(a, _)| a == addr)
            .map(|(_, x)| *x);
        let (nonce, balance, code_hash) = match basic {
            Some(x) => (x.nonce, x.balance, x.code_hash),
            None => existing
                .map(|e| (e.nonce, e.balance, e.code_hash))
                .unwrap_or((0, U256::ZERO, B256::ZERO)),
        };
        let storage_root = match new_sroot.get(addr) {
            Some(sr) => *sr,
            None => existing.map(|e| e.storage_root).unwrap_or(empty_root()),
        };
        let parts = AccountTrieParts {
            nonce,
            balance,
            code_hash,
            storage_root,
        };
        if parts.is_empty() {
            let _ = txn.del(ha, ah.as_slice(), None);
        } else {
            txn.put(
                ha,
                ah.as_slice(),
                encode_account_leaf(&parts),
                WriteFlags::UPSERT,
            )
            .unwrap();
        }
    }

    // --- account trie ---
    let ps = PrefixSet::from_b256s(touched.iter().map(keccak256));
    let (root, aup) = StateRoot::state_root_incremental(&txn, at, ha, &ps).unwrap();
    apply_trie_updates(&txn, at, None, &aup).unwrap();
    txn.commit().unwrap();
    root
}

/// Delete every row whose key starts with `prefix` (account cleanup on delete).
fn del_prefix(
    txn: &signet_libmdbx::tx::aliases::RwTxSync,
    db: signet_libmdbx::Database,
    prefix: &[u8],
) {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    {
        let mut cur = txn.cursor(db).unwrap();
        let mut item = cur.set_range::<Vec<u8>, Vec<u8>>(prefix).unwrap();
        while let Some((k, _)) = item {
            if !k.starts_with(prefix) {
                break;
            }
            keys.push(k);
            item = cur.next::<Vec<u8>, Vec<u8>>().unwrap();
        }
    }
    for k in keys {
        let _ = txn.del(db, k, None);
    }
}

/// Full-rebuild oracle root from the model.
fn oracle_root(
    accts: &BTreeMap<Address, Basic>,
    stor: &BTreeMap<Address, BTreeMap<B256, U256>>,
) -> B256 {
    super::state_root(accts.iter().map(|(addr, basic)| {
        let sroot = stor
            .get(addr)
            .map(|m| super::storage_root(m.iter().map(|(k, v)| (*k, *v))))
            .unwrap_or_else(empty_root);
        (
            *addr,
            AccountTrieParts {
                nonce: basic.nonce,
                balance: basic.balance,
                code_hash: basic.code_hash,
                storage_root: sroot,
            },
        )
    }))
}

#[test]
fn incremental_equals_full_rebuild_over_random_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();

    // A larger pool grows the account trie deep enough to exercise multi-level
    // stored branch nodes (and thus the skip path) — small pools collapse to a
    // shallow trie that never stores intermediate nodes.
    let addrs: Vec<Address> = (1u8..=40).map(Address::repeat_byte).collect();
    let slots: Vec<B256> = (1u8..=8).map(|i| B256::from(U256::from(i))).collect();

    let mut m_accts: BTreeMap<Address, Basic> = BTreeMap::new();
    let mut m_stor: BTreeMap<Address, BTreeMap<B256, U256>> = BTreeMap::new();

    for block in 0..50u64 {
        let mut rng = block.wrapping_mul(0x1234_5678_9abc_def1) ^ 0xdead_beef;

        // One canonical op per account per block (a real aggregated BlockDelta
        // never repeats an account): None = delete, Some = non-empty upsert.
        let mut ops: BTreeMap<Address, Option<Basic>> = BTreeMap::new();
        let n_acct = 1 + (splitmix(&mut rng) % 4);
        for _ in 0..n_acct {
            let addr = addrs[(splitmix(&mut rng) % addrs.len() as u64) as usize];
            if splitmix(&mut rng) % 6 == 3 {
                ops.insert(addr, None);
            } else {
                let nonce = 1 + splitmix(&mut rng) % 9;
                let balance = U256::from(1 + splitmix(&mut rng) % 1000);
                ops.insert(
                    addr,
                    Some(Basic {
                        nonce,
                        balance,
                        code_hash: B256::ZERO,
                    }),
                );
            }
        }
        // Storage ops, deduped per (addr, slot) with last write winning. Only on
        // accounts that exist (or are upserted this block).
        let mut stor_ops: BTreeMap<(Address, B256), U256> = BTreeMap::new();
        let n_stor = splitmix(&mut rng) % 5;
        for _ in 0..n_stor {
            let addr = addrs[(splitmix(&mut rng) % addrs.len() as u64) as usize];
            if !m_accts.contains_key(&addr) && !matches!(ops.get(&addr), Some(Some(_))) {
                ops.insert(
                    addr,
                    Some(Basic {
                        nonce: 1,
                        balance: U256::from(1u64),
                        code_hash: B256::ZERO,
                    }),
                );
            }
            let slot = slots[(splitmix(&mut rng) % slots.len() as u64) as usize];
            let val = U256::from(splitmix(&mut rng) % 100);
            stor_ops.insert((addr, slot), val);
        }

        let mut b = Block::default();
        for (addr, op) in &ops {
            match op {
                Some(basic) => b.acct_upserts.push((*addr, *basic)),
                None => b.acct_deletes.push(*addr),
            }
        }
        for ((addr, slot), val) in &stor_ops {
            // skip storage on accounts being deleted this block
            if matches!(ops.get(addr), Some(None)) {
                continue;
            }
            b.storage.push((*addr, *slot, *val));
        }

        // --- advance the model ---
        for addr in &b.acct_deletes {
            m_accts.remove(addr);
            m_stor.remove(addr);
        }
        for (addr, basic) in &b.acct_upserts {
            if b.acct_deletes.contains(addr) {
                continue;
            }
            // upserts are always non-empty (see block generation).
            m_accts.insert(*addr, *basic);
        }
        for (addr, slot, val) in &b.storage {
            if !m_accts.contains_key(addr) {
                continue;
            }
            let s = m_stor.entry(*addr).or_default();
            if val.is_zero() {
                s.remove(slot);
            } else {
                s.insert(*slot, *val);
            }
        }

        let got = apply_block(&env, &b);
        let want = oracle_root(&m_accts, &m_stor);
        assert_eq!(got, want, "root mismatch at block {block}");
    }
}

#[test]
fn debug_two_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let mk = |bytes: &[(u8, u64)]| -> Block {
        Block {
            acct_upserts: bytes
                .iter()
                .map(|(b, bal)| {
                    (
                        Address::repeat_byte(*b),
                        Basic {
                            nonce: 1,
                            balance: U256::from(*bal),
                            code_hash: B256::ZERO,
                        },
                    )
                })
                .collect(),
            ..Default::default()
        }
    };
    let mut m: BTreeMap<Address, Basic> = BTreeMap::new();
    let stor: BTreeMap<Address, BTreeMap<B256, U256>> = BTreeMap::new();

    // block 0: five accounts
    let b0 = mk(&[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);
    for (a, x) in &b0.acct_upserts {
        m.insert(*a, *x);
    }
    let r0 = apply_block(&env, &b0);
    assert_eq!(r0, oracle_root(&m, &stor), "block0");

    // block 1: one new account
    let b1 = mk(&[(9, 90)]);
    for (a, x) in &b1.acct_upserts {
        m.insert(*a, *x);
    }
    let r1 = apply_block(&env, &b1);
    assert_eq!(r1, oracle_root(&m, &stor), "block1");
}

#[test]
fn empty_then_one_account_then_delete() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    let a = Address::repeat_byte(0x42);

    // empty
    assert_eq!(apply_block(&env, &Block::default()), empty_root());

    // one funded account
    let b1 = Block {
        acct_upserts: vec![(
            a,
            Basic {
                nonce: 1,
                balance: U256::from(100u64),
                code_hash: B256::ZERO,
            },
        )],
        ..Default::default()
    };
    let r1 = apply_block(&env, &b1);
    assert_ne!(r1, empty_root());

    // delete it -> back to empty
    let b2 = Block {
        acct_deletes: vec![a],
        ..Default::default()
    };
    assert_eq!(apply_block(&env, &b2), empty_root());
}
