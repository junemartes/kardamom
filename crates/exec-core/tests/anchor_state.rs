//! Account/storage-level anchor gate (spec: no-std-exec-core, phase 3b):
//! `verify_witness_anchored` + `recompute_post_root` over a realistic
//! two-trie state, cross-checked against alloy-trie's one-shot roots (the
//! primitive `kardamom-state`'s oracle wraps).
//!
//! The raw sparse-trie mechanics are proven in `anchor_sparse.rs`; this
//! file proves the STATE SEMANTICS layered on them: TrieAccount leaves,
//! per-account storage roots, exclusion proofs for absent accounts and
//! explicit-zero slots, EIP-161 empty-account handling, and refutation of
//! every witness lie.

use std::collections::{BTreeMap, HashMap};

use alloy_primitives::{Address, B256, Bytes, U256, address, keccak256};
use alloy_trie::proof::ProofRetainer;
use alloy_trie::{EMPTY_ROOT_HASH, HashBuilder, KECCAK_EMPTY, Nibbles, TrieAccount};
use bytes::Bytes as WireBytes;
use kardamom_exec_core::anchor::{
    AnchorError, NodeStore, recompute_post_root, verify_witness_anchored,
};
use kardamom_exec_core::delta::PendingDelta;
use kardamom_types::{ExecutionWitness, WitnessAccount, WitnessProofs, WitnessSlot};

const A: Address = address!("00000000000000000000000000000000000000A1");
const B: Address = address!("00000000000000000000000000000000000000B2");
const ABSENT: Address = address!("00000000000000000000000000000000000000C3");
const FRESH: Address = address!("00000000000000000000000000000000000000D4");

const S1: B256 = B256::with_last_byte(1);
const S2: B256 = B256::with_last_byte(2);
const S3: B256 = B256::with_last_byte(3); // explicit-zero read

/// Full reference state: account → (TrieAccount, storage map).
type RefState = BTreeMap<Address, (TrieAccount, BTreeMap<B256, U256>)>;

fn base_state() -> RefState {
    let mut st = RefState::new();
    let a_storage: BTreeMap<B256, U256> = [(S1, U256::from(11)), (S2, U256::from(22))].into();
    st.insert(
        A,
        (
            TrieAccount {
                nonce: 5,
                balance: U256::from(1_000_000),
                storage_root: alloy_trie::root::storage_root_unhashed(
                    a_storage.iter().map(|(k, v)| (*k, *v)),
                ),
                code_hash: KECCAK_EMPTY,
            },
            a_storage,
        ),
    );
    st.insert(
        B,
        (
            TrieAccount {
                nonce: 0,
                balance: U256::from(777),
                storage_root: EMPTY_ROOT_HASH,
                code_hash: KECCAK_EMPTY,
            },
            BTreeMap::new(),
        ),
    );
    // Background accounts so the account trie has real branch structure.
    for i in 0u8..24 {
        let addr = Address::repeat_byte(0xE0u8.wrapping_add(i));
        st.insert(
            addr,
            (
                TrieAccount {
                    nonce: 1,
                    balance: U256::from(i as u64 + 1),
                    storage_root: EMPTY_ROOT_HASH,
                    code_hash: KECCAK_EMPTY,
                },
                BTreeMap::new(),
            ),
        );
    }
    st
}

fn oracle_state_root(st: &RefState) -> B256 {
    alloy_trie::root::state_root_unhashed(st.iter().map(|(addr, (ta, _))| (*addr, *ta)))
}

/// EVERY node of the account trie and every touched storage trie, keyed by
/// hash — the test's stand-in for the validator's live trie.
fn all_nodes(st: &RefState) -> (B256, HashMap<B256, Bytes>) {
    let mut nodes = HashMap::new();
    // Account trie with all keys as proof targets.
    let mut entries: Vec<(B256, Vec<u8>)> = st
        .iter()
        .map(|(addr, (ta, _))| {
            let mut rlp = Vec::new();
            alloy_rlp::Encodable::encode(ta, &mut rlp);
            (keccak256(addr), rlp)
        })
        .collect();
    entries.sort_by_key(|(k, _)| *k);
    let targets: Vec<Nibbles> = entries.iter().map(|(k, _)| Nibbles::unpack(k)).collect();
    let mut hb = HashBuilder::default().with_proof_retainer(ProofRetainer::new(targets));
    for (k, v) in &entries {
        hb.add_leaf(Nibbles::unpack(k), v);
    }
    let root = hb.root();
    for node in hb.take_proof_nodes().into_inner().into_values() {
        if node.len() >= 32 {
            nodes.insert(keccak256(&node), node);
        }
    }
    // Each non-empty storage trie, all keys as targets.
    for (_, storage) in st.values() {
        if storage.is_empty() {
            continue;
        }
        let mut sentries: Vec<(B256, Vec<u8>)> = storage
            .iter()
            .filter(|(_, v)| !v.is_zero())
            .map(|(k, v)| {
                let mut rlp = Vec::new();
                alloy_rlp::Encodable::encode(v, &mut rlp);
                (keccak256(k), rlp)
            })
            .collect();
        sentries.sort_by_key(|(k, _)| *k);
        let stargets: Vec<Nibbles> = sentries.iter().map(|(k, _)| Nibbles::unpack(k)).collect();
        let mut shb = HashBuilder::default().with_proof_retainer(ProofRetainer::new(stargets));
        for (k, v) in &sentries {
            shb.add_leaf(Nibbles::unpack(k), v);
        }
        let _ = shb.root();
        for node in shb.take_proof_nodes().into_inner().into_values() {
            if node.len() >= 32 {
                nodes.insert(keccak256(&node), node);
            }
        }
    }
    (root, nodes)
}

fn proofs_from(nodes: impl IntoIterator<Item = Bytes>) -> WitnessProofs {
    let mut v: Vec<(B256, Bytes)> = nodes.into_iter().map(|n| (keccak256(&n), n)).collect();
    v.sort_by_key(|(h, _)| *h);
    v.dedup_by_key(|(h, _)| *h);
    WitnessProofs {
        nodes: v
            .into_iter()
            .map(|(_, n)| WireBytes::from(n.to_vec()))
            .collect(),
    }
}

/// The honest witness for this test's read set: A (with s1, s2 and an
/// explicit-zero s3), B, and a proven-absent account.
fn witness(st: &RefState, root: B256) -> ExecutionWitness {
    let (a, _) = &st[&A];
    let (b, _) = &st[&B];
    let mut accounts = vec![
        WitnessAccount {
            address: A,
            exists: true,
            nonce: a.nonce,
            balance: a.balance,
            code_hash: a.code_hash,
        },
        WitnessAccount {
            address: B,
            exists: true,
            nonce: b.nonce,
            balance: b.balance,
            code_hash: b.code_hash,
        },
        WitnessAccount {
            address: ABSENT,
            exists: false,
            nonce: 0,
            balance: U256::ZERO,
            code_hash: B256::ZERO,
        },
    ];
    accounts.sort_by_key(|a| a.address);
    let mut storage = vec![
        WitnessSlot {
            address: A,
            key: S1,
            value: U256::from(11),
        },
        WitnessSlot {
            address: A,
            key: S2,
            value: U256::from(22),
        },
        WitnessSlot {
            address: A,
            key: S3,
            value: U256::ZERO,
        },
    ];
    storage.sort_by_key(|s| (s.address, s.key));
    ExecutionWitness {
        block_number: 7,
        accounts,
        storage,
        code: Vec::new(),
        pre_state_root: Some(root),
    }
}

/// Fixed point over the complete node map (the validator's algorithm; the
/// test's node source is the reference builder instead of the live trie).
fn anchored<T>(
    all: &HashMap<B256, Bytes>,
    op: impl Fn(&WitnessProofs) -> Result<T, AnchorError>,
) -> (T, WitnessProofs) {
    let mut have: Vec<Bytes> = Vec::new();
    for round in 0..(all.len() + 2) {
        let proofs = proofs_from(have.clone());
        match op(&proofs) {
            Ok(v) => return (v, proofs),
            Err(AnchorError::MissingNode { hash, .. }) => {
                have.push(
                    all.get(&hash)
                        .unwrap_or_else(|| panic!("round {round}: unknown node {hash}"))
                        .clone(),
                );
            }
            Err(e) => panic!("unexpected anchor error: {e:?}"),
        }
    }
    panic!("fixed point diverged");
}

#[test]
fn honest_witness_verifies_and_recomputes_the_oracle_post_root() {
    let st = base_state();
    let (root, all) = all_nodes(&st);
    assert_eq!(root, oracle_state_root(&st), "reference builders agree");
    let w = witness(&st, root);

    // The block: A's balance and nonce change, s1 rewritten, s2 zeroed
    // (storage deletion collapse), B untouched, FRESH account created.
    let mut delta = PendingDelta::new();
    delta
        .accounts
        .insert(A, (6, U256::from(900_000), KECCAK_EMPTY));
    delta.storage.insert((A, S1), U256::from(1111));
    delta.storage.insert((A, S2), U256::ZERO);
    delta
        .accounts
        .insert(FRESH, (0, U256::from(100_000), KECCAK_EMPTY));

    // Witness for FRESH: the execution READ it (absent) before creating it.
    let mut w = w;
    w.accounts.push(WitnessAccount {
        address: FRESH,
        exists: false,
        nonce: 0,
        balance: U256::ZERO,
        code_hash: B256::ZERO,
    });
    w.accounts.sort_by_key(|a| a.address);

    // Oracle post state.
    let mut post = st.clone();
    {
        let (a, storage) = post.get_mut(&A).unwrap();
        a.nonce = 6;
        a.balance = U256::from(900_000);
        storage.insert(S1, U256::from(1111));
        storage.remove(&S2);
        a.storage_root =
            alloy_trie::root::storage_root_unhashed(storage.iter().map(|(k, v)| (*k, *v)));
    }
    post.insert(
        FRESH,
        (
            TrieAccount {
                nonce: 0,
                balance: U256::from(100_000),
                storage_root: EMPTY_ROOT_HASH,
                code_hash: KECCAK_EMPTY,
            },
            BTreeMap::new(),
        ),
    );
    let oracle_post = oracle_state_root(&post);

    // Fixed point: verification + recompute under one growing node set —
    // exactly the capture loop's shape.
    let (sparse_post, proofs) = anchored(&all, |proofs| {
        let pre = verify_witness_anchored(&w, proofs)?;
        recompute_post_root(&w, proofs, &pre, &delta)
    });
    assert_eq!(sparse_post, oracle_post, "post root equals the oracle");

    // The final proof set also verifies standalone (guest shape: one shot,
    // no retry).
    let pre = verify_witness_anchored(&w, &proofs).expect("guest-shape verify");
    let again = recompute_post_root(&w, &proofs, &pre, &delta).expect("guest-shape recompute");
    assert_eq!(again, oracle_post);
}

#[test]
fn every_witness_lie_is_refuted() {
    let st = base_state();
    let (root, all) = all_nodes(&st);
    let w = witness(&st, root);

    // A complete proof set for the honest witness (fixed point once).
    let (_, proofs) = anchored(&all, |proofs| verify_witness_anchored(&w, proofs));

    let refuted = |mutate: &dyn Fn(&mut ExecutionWitness)| {
        let mut lie = w.clone();
        mutate(&mut lie);
        matches!(
            verify_witness_anchored(&lie, &proofs),
            Err(AnchorError::Refuted { .. })
        )
    };

    assert!(
        refuted(&|w| w.accounts[0].balance = U256::from(1)),
        "balance lie"
    );
    assert!(refuted(&|w| w.accounts[0].nonce += 1), "nonce lie");
    assert!(
        refuted(&|w| w.accounts[0].code_hash = B256::repeat_byte(0x66)),
        "code_hash lie"
    );
    assert!(
        refuted(&|w| {
            // Claim the absent account exists WITH STATE. (Merely flipping
            // `exists` on all-zero fields is EIP-161-empty — equivalent to
            // absent by execution semantics, deliberately NOT a lie: the
            // state table keeps touched-empty rows the trie excludes.)
            let i = w.accounts.iter().position(|a| a.address == ABSENT).unwrap();
            w.accounts[i].exists = true;
            w.accounts[i].nonce = 1;
        }),
        "absence lie"
    );
    {
        // The EIP-161 equivalence, positively: witnessed-present-but-empty
        // against a trie exclusion VERIFIES (the touched-zero-fee-coinbase
        // shape the live pipeline produces).
        let mut empty_present = w.clone();
        let i = empty_present
            .accounts
            .iter()
            .position(|a| a.address == ABSENT)
            .unwrap();
        empty_present.accounts[i].exists = true;
        verify_witness_anchored(&empty_present, &proofs)
            .expect("empty-but-present must verify as absent");
    }
    assert!(
        refuted(&|w| {
            // Claim an existing account absent.
            let i = w.accounts.iter().position(|a| a.address == B).unwrap();
            w.accounts[i].exists = false;
        }),
        "presence lie"
    );
    assert!(
        refuted(&|w| {
            let i = w.storage.iter().position(|s| s.key == S1).unwrap();
            w.storage[i].value = U256::from(999);
        }),
        "slot value lie"
    );
    assert!(
        refuted(&|w| {
            let i = w.storage.iter().position(|s| s.key == S3).unwrap();
            w.storage[i].value = U256::from(1); // zero slot claimed non-zero
        }),
        "explicit-zero lie"
    );
    assert!(
        refuted(&|w| {
            let i = w.storage.iter().position(|s| s.key == S2).unwrap();
            w.storage[i].value = U256::ZERO; // non-zero slot claimed zero
        }),
        "zeroed-slot lie"
    );

    // And a lying ROOT is a MissingNode, not a refutation: nothing links.
    let mut lie = w.clone();
    lie.pre_state_root = Some(B256::repeat_byte(0x42));
    assert!(matches!(
        verify_witness_anchored(&lie, &proofs),
        Err(AnchorError::MissingNode { .. })
    ));
}

#[test]
fn emptying_a_preexisting_account_fails_closed() {
    let st = base_state();
    let (root, all) = all_nodes(&st);
    let w = witness(&st, root);

    // B (nonce 0, no code) drained to zero balance → EIP-161 empty →
    // account-trie deletion, unreachable live and unsupported v0.
    let mut delta = PendingDelta::new();
    delta.accounts.insert(B, (0, U256::ZERO, KECCAK_EMPTY));

    let mut have: Vec<Bytes> = Vec::new();
    let err = loop {
        let proofs = proofs_from(have.clone());
        match verify_witness_anchored(&w, &proofs)
            .and_then(|pre| recompute_post_root(&w, &proofs, &pre, &delta))
        {
            Err(AnchorError::MissingNode { hash, .. }) => have.push(all[&hash].clone()),
            Err(e) => break e,
            Ok(_) => panic!("must fail closed"),
        }
    };
    assert!(matches!(
        err,
        AnchorError::AccountDeleteUnsupported { address } if address == B
    ));

    // Whereas a FRESH account touched-but-empty is EIP-161-skipped: no-op.
    let mut w2 = w.clone();
    w2.accounts.push(WitnessAccount {
        address: FRESH,
        exists: false,
        nonce: 0,
        balance: U256::ZERO,
        code_hash: B256::ZERO,
    });
    w2.accounts.sort_by_key(|a| a.address);
    let mut delta2 = PendingDelta::new();
    delta2.accounts.insert(FRESH, (0, U256::ZERO, B256::ZERO));
    let (post, _) = anchored(&all, |proofs| {
        let pre = verify_witness_anchored(&w2, proofs)?;
        recompute_post_root(&w2, proofs, &pre, &delta2)
    });
    assert_eq!(post, root, "touched-but-empty leaves the root unchanged");
}

#[test]
fn slot_under_unwitnessed_account_is_rejected() {
    let st = base_state();
    let (root, all) = all_nodes(&st);
    let mut w = witness(&st, root);
    // Remove account B but leave a slot claiming to be under it.
    w.accounts.retain(|a| a.address != B);
    w.storage.push(WitnessSlot {
        address: B,
        key: S1,
        value: U256::ZERO,
    });
    w.storage.sort_by_key(|s| (s.address, s.key));

    let mut have: Vec<Bytes> = Vec::new();
    let err = loop {
        let proofs = proofs_from(have.clone());
        match verify_witness_anchored(&w, &proofs) {
            Err(AnchorError::MissingNode { hash, .. }) => have.push(all[&hash].clone()),
            Err(e) => break e,
            Ok(_) => panic!("must reject"),
        }
    };
    assert!(matches!(err, AnchorError::Refuted { .. }));
    let _ = NodeStore::new(&WitnessProofs::default()).unwrap();
}
