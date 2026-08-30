//! The correctness gate. This drives randomized blocks through the
//! production incremental update path ([`super::update_for_block`]),
//! and asserts that the root matches the full-rebuild oracle after every
//! block. It also runs a stored-node-table equivalence check against a
//! from-scratch build, as an orphan detector.
//!
//! A deterministic regression test
//! (`extension_collapse_regrow_no_stale_orphans`) mines addresses whose
//! hashed keys share nibble prefixes. This constructs the
//! stored-branch-under-extension, collapse, regrow, stale-skip geometry
//! that the visited-minus-updated removal scheme missed, which caused a
//! silent root divergence.

use std::collections::BTreeMap;

use alloy_primitives::{Address, B256, U256, keccak256};
use kardamom_types::{AccountChange, BlockDelta, StorageChange};

use super::{AccountTrieParts, TrieTables, empty_root, update_for_block};
use crate::env::{Durability, StateEnv, StateEnvBuilder};
use crate::schema::{TABLE_ACCOUNT_TRIE, TABLE_STORAGE_TRIE};

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
    storage: Vec<(Address, B256, U256)>, // A value of 0 deletes the slot.
}

impl Block {
    /// The production write payload for this block. An account delete is
    /// an upsert to the EIP-161-empty account, with zero nonce, balance,
    /// and code. `update_for_block` interprets this exactly as removal.
    fn to_delta(&self, block_number: u64) -> BlockDelta {
        let mut accounts: Vec<AccountChange> = self
            .acct_upserts
            .iter()
            .map(|(address, b)| AccountChange {
                address: *address,
                nonce: b.nonce,
                balance: b.balance,
                code_hash: b.code_hash,
            })
            .collect();
        accounts.extend(self.acct_deletes.iter().map(|address| AccountChange {
            address: *address,
            nonce: 0,
            balance: U256::ZERO,
            code_hash: B256::ZERO,
        }));
        BlockDelta {
            block_number,
            accounts,
            storage: self
                .storage
                .iter()
                .map(|(address, key, value)| StorageChange {
                    address: *address,
                    key: *key,
                    value: *value,
                })
                .collect(),
            code: Vec::new(),
            receipts: Vec::new(),
        }
    }
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Apply a block through the production incremental path and return the new
/// world-state root.
fn apply_block(env: &StateEnv, b: &Block) -> B256 {
    apply_delta(env, &b.to_delta(0))
}

fn apply_delta(env: &StateEnv, delta: &BlockDelta) -> B256 {
    let txn = env.raw().begin_rw_sync().unwrap();
    let tables = TrieTables::open(&txn).unwrap();
    let root = update_for_block(&txn, &tables, delta).unwrap();
    txn.commit().unwrap();
    root
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

/// Dump every `(key, value)` row of a table, ascending.
fn dump_table(env: &StateEnv, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let txn = env.raw().begin_rw_sync().unwrap();
    let db = txn.open_db(Some(name)).unwrap();
    let mut cur = txn.cursor(db).unwrap();
    let mut out = Vec::new();
    let mut item = cur.first::<Vec<u8>, Vec<u8>>().unwrap();
    while let Some((k, v)) = item {
        out.push((k, v));
        item = cur.next::<Vec<u8>, Vec<u8>>().unwrap();
    }
    out
}

fn temp_env() -> (tempfile::TempDir, StateEnv) {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .open()
        .unwrap();
    (dir, env)
}

/// The orphan detector. Replay the model's cumulative state into a
/// fresh env as one block, and require both node tables to be
/// byte-identical to the incrementally maintained ones. Stored branch
/// nodes are a pure function of the leaf set: masks and hashes depend
/// only on subtree content. So any extra row in the incremental tables
/// is a stale orphan, and any differing row is drift.
fn assert_node_tables_match_fresh_build(
    env: &StateEnv,
    accts: &BTreeMap<Address, Basic>,
    stor: &BTreeMap<Address, BTreeMap<B256, U256>>,
    context: &str,
) {
    let (_dir, fresh) = temp_env();
    let mut b = Block::default();
    for (addr, basic) in accts {
        b.acct_upserts.push((*addr, *basic));
    }
    for (addr, slots) in stor {
        for (k, v) in slots {
            b.storage.push((*addr, *k, *v));
        }
    }
    apply_block(&fresh, &b);
    assert_eq!(
        dump_table(env, TABLE_ACCOUNT_TRIE),
        dump_table(&fresh, TABLE_ACCOUNT_TRIE),
        "account_trie diverged from a fresh build ({context})"
    );
    assert_eq!(
        dump_table(env, TABLE_STORAGE_TRIE),
        dump_table(&fresh, TABLE_STORAGE_TRIE),
        "storage_trie diverged from a fresh build ({context})"
    );
}

#[test]
fn incremental_equals_full_rebuild_over_random_blocks() {
    let (_dir, env) = temp_env();

    // A larger pool grows the account trie deep enough to exercise
    // multi-level stored branch nodes, extension-shaped children (hashed
    // keys sharing 2 or more nibbles), and collapse and regrow cycles. A
    // small pool collapses to a shallow trie that never stores
    // intermediate nodes off exact child paths.
    let addrs: Vec<Address> = (1u16..=160)
        .map(|i| {
            let mut b = [0u8; 20];
            b[..2].copy_from_slice(&i.to_be_bytes());
            Address::from(b)
        })
        .collect();
    let slots: Vec<B256> = (1u8..=10).map(|i| B256::from(U256::from(i))).collect();

    let mut m_accts: BTreeMap<Address, Basic> = BTreeMap::new();
    let mut m_stor: BTreeMap<Address, BTreeMap<B256, U256>> = BTreeMap::new();

    for block in 0..80u64 {
        let mut rng = block.wrapping_mul(0x1234_5678_9abc_def1) ^ 0xdead_beef;

        // Use one op per account per block. A real, aggregated BlockDelta
        // never repeats an account. None means delete; Some means a
        // non-empty upsert. Deletes happen often, 1 in 4, so subtries
        // keep collapsing and regrowing.
        let mut ops: BTreeMap<Address, Option<Basic>> = BTreeMap::new();
        let n_acct = 1 + (splitmix(&mut rng) % 6);
        for _ in 0..n_acct {
            let addr = addrs[(splitmix(&mut rng) % addrs.len() as u64) as usize];
            if splitmix(&mut rng) % 4 == 3 {
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
        // Storage ops, deduplicated per (addr, slot), with the last write
        // winning. These only apply to accounts that exist, or are
        // upserted this block.
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
            // Skip storage on accounts being deleted this block.
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
            // Upserts are always non-empty. See the block-generation code above.
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

        let got = apply_delta(&env, &b.to_delta(block));
        let want = oracle_root(&m_accts, &m_stor);
        assert_eq!(got, want, "root mismatch at block {block}");

        if block % 10 == 9 {
            assert_node_tables_match_fresh_build(
                &env,
                &m_accts,
                &m_stor,
                &format!("block {block}"),
            );
        }
    }
    assert_node_tables_match_fresh_build(&env, &m_accts, &m_stor, "final");
}

/// This test constructs, deterministically, the exact geometry that an
/// earlier removal scheme missed. It mines addresses whose hashed keys
/// share nibble prefixes:
///
/// 1. Block 1 stores a branch node under an extension, at the 2-nibble
///    path `[n0,n1]`. The walker only ever exact-gets `[n0]`.
/// 2. Block 2 rebuilds that subtrie from leaves, after an exact-get miss
///    at `[n0]`. The new content both collapses `[n0,n1]` and drifts the
///    surviving subtree under `[n0,n1,n2]`. Under the old
///    visited-minus-updated removal scheme, the `[n0,n1]` node becomes a
///    stale orphan that holds a pre-drift child hash.
/// 3. Block 3 regrows a stored node at exactly `[n0]`. Its `tree_mask`
///    points into the orphaned region, without re-upserting `[n0,n1]`.
/// 4. Block 4 changes a sibling under `[n0,n1]`. The walk exact-hits the
///    stale orphan, and skips its "unchanged" child with `add_branch`,
///    using the stale hash. The result is a silently wrong root.
#[test]
fn extension_collapse_regrow_no_stale_orphans() {
    let nibs5 = |a: &Address| -> [u8; 5] {
        let h = keccak256(a);
        std::array::from_fn(|i| {
            let b = h[i / 2];
            if i % 2 == 0 { b >> 4 } else { b & 0x0f }
        })
    };
    // Mine an address whose first five hashed-key nibbles satisfy `pred`.
    // A salted address never collides with `a` below, because its tail
    // bytes are non-zero.
    let mine = |pred: &dyn Fn(&[u8; 5]) -> bool| -> Address {
        for salt in 0u64..3_000_000 {
            let mut bytes = [0u8; 20];
            bytes[..8].copy_from_slice(&salt.to_le_bytes());
            let x = Address::from(bytes);
            if pred(&nibs5(&x)) {
                return x;
            }
        }
        panic!("address mining exhausted — hashed-key prefix never found");
    };

    // `a` defines the target prefix [n0,n1,n2,n3]. The rest are mined
    // relative to it. Keys are keccak-hashed, so this searches for them
    // instead of choosing them directly.
    let a = Address::repeat_byte(0xa5);
    let an = nibs5(&a);
    // b shares 4 nibbles with a, and diverges at nibble 4. This makes
    // branch(a,b) at [n0,n1,n2,n3], and [n0,n1,n2] a stored node.
    let b = mine(&|m| m[..4] == an[..4] && m[4] != an[4]);
    // e shares 3 nibbles, and diverges at nibble 3. This makes
    // [n0,n1,n2] a branch.
    let e = mine(&|m| m[..3] == an[..3] && m[3] != an[3]);
    // c shares 2 nibbles, and diverges at nibble 2. This makes [n0,n1] a
    // branch, stored under the extension from the root's n0 child.
    let c = mine(&|m| m[..2] == an[..2] && m[2] != an[2]);
    // d shares 1 nibble, and diverges at nibble 1. It later regrows a
    // stored branch at exactly [n0].
    let d = mine(&|m| m[0] == an[0] && m[1] != an[1]);
    // anchor has a different first nibble, so the root is always a branch.
    let anchor = mine(&|m| m[0] != an[0]);

    let (_dir, env) = temp_env();
    let mut model: BTreeMap<Address, Basic> = BTreeMap::new();
    let no_stor: BTreeMap<Address, BTreeMap<B256, U256>> = BTreeMap::new();
    let basic = |bal: u64| Basic {
        nonce: 1,
        balance: U256::from(bal),
        code_hash: B256::ZERO,
    };
    let step = |model: &mut BTreeMap<Address, Basic>,
                ups: &[(Address, u64)],
                dels: &[Address],
                label: &str| {
        let block = Block {
            acct_upserts: ups.iter().map(|(addr, bal)| (*addr, basic(*bal))).collect(),
            acct_deletes: dels.to_vec(),
            ..Default::default()
        };
        for (addr, bal) in ups {
            model.insert(*addr, basic(*bal));
        }
        for addr in dels {
            model.remove(addr);
        }
        let got = apply_block(&env, &block);
        assert_eq!(
            got,
            oracle_root(model, &no_stor),
            "root mismatch at {label}"
        );
    };
    let stored_node_at = |path: &[u8]| -> bool {
        let txn = env.raw().begin_rw_sync().unwrap();
        let db = txn.open_db(Some(TABLE_ACCOUNT_TRIE)).unwrap();
        txn.get::<Vec<u8>>(db.dbi(), path).unwrap().is_some()
    };

    // 1. A stored branch under an extension: nodes at [n0,n1] and
    //    [n0,n1,n2], and nothing at [n0].
    step(
        &mut model,
        &[(anchor, 10), (a, 11), (b, 12), (e, 13), (c, 14)],
        &[],
        "block1",
    );
    assert!(
        stored_node_at(&an[..2]),
        "geometry sanity: expected a stored node at [n0,n1] under the extension \
         (alloy-trie storage rules changed?)"
    );
    assert!(!stored_node_at(&an[..1]));

    // 2. Collapse [n0,n1] by deleting c, while drifting the surviving
    //    subtree, a's balance. The subtrie is rebuilt from leaves after
    //    an exact-get miss at [n0], so only prefix clearing removes the
    //    [n0,n1] node.
    step(&mut model, &[(a, 21)], &[c], "block2");
    assert!(
        !stored_node_at(&an[..2]),
        "stale orphan left at [n0,n1] after the subtree collapsed (F09.1)"
    );

    // 3. Regrow a stored node at exactly [n0]. Its tree_mask points into
    //    the formerly orphaned region.
    step(&mut model, &[(d, 15)], &[], "block3");
    assert!(stored_node_at(&an[..1]));

    // 4. Change a sibling under [n0,n1]. With the orphan present, the
    //    walk exact-hits it, and skips its "unchanged" child with
    //    add_branch, using a hash from before block 2's drift. The
    //    result is a silently wrong root.
    step(&mut model, &[(c, 16)], &[], "block4");

    assert_node_tables_match_fresh_build(&env, &model, &no_stor, "after regrow");
}

#[test]
fn debug_two_blocks() {
    let (_dir, env) = temp_env();
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
    let (_dir, env) = temp_env();
    let a = Address::repeat_byte(0x42);

    // Start empty.
    assert_eq!(apply_block(&env, &Block::default()), empty_root());

    // Add one funded account.
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

    // Delete it. Back to empty.
    let b2 = Block {
        acct_deletes: vec![a],
        ..Default::default()
    };
    assert_eq!(apply_block(&env, &b2), empty_root());
}
