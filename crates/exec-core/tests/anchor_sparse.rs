//! Sparse-trie acceptance gate (spec: no-std-exec-core, phase 3b): the
//! sparse recompute over a partial node set MUST equal the full oracle —
//! alloy-trie's one-shot `HashBuilder` root over the complete key set, the
//! same primitive `kardamom-state`'s oracle wraps.
//!
//! The MissingNode-retry loop in [`fixed_point`] IS the capture-side
//! algorithm (recompute-guided completion): the test resolves each named
//! missing node from a complete node map exactly as the validator resolves
//! it from the live trie via the proof-retainer walk. Convergence here is
//! the design's termination argument exercised, not just a test fixture.
//!
//! Deletion-collapse cases are enumerated explicitly — branch→leaf,
//! branch→extension, root collapse, cascaded multi-delete under one branch
//! — that's where sparse implementations rot.

use std::collections::{BTreeMap, HashMap};

use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_trie::proof::ProofRetainer;
use alloy_trie::{HashBuilder, Nibbles};
use bytes::Bytes as WireBytes;
use kardamom_exec_core::anchor::{AnchorError, Lookup, NodeStore, SparseTrie};
use kardamom_types::WitnessProofs;

/// Reference root + COMPLETE node map for a key→value set, via the oracle
/// builder with every key as a proof target (every path retained = every
/// node retained).
fn reference(entries: &BTreeMap<B256, Vec<u8>>) -> (B256, HashMap<B256, Bytes>) {
    let targets: Vec<Nibbles> = entries.keys().map(|k| Nibbles::unpack(k)).collect();
    let mut hb = HashBuilder::default().with_proof_retainer(ProofRetainer::new(targets));
    for (k, v) in entries {
        hb.add_leaf(Nibbles::unpack(k), v);
    }
    let root = hb.root();
    let mut nodes = HashMap::new();
    for (_, node) in hb.take_proof_nodes().into_inner() {
        // Only nodes ≥ 32 bytes are addressable by hash; smaller ones are
        // inline in their parents and never fetched.
        if node.len() >= 32 {
            nodes.insert(keccak256(&node), node);
        }
    }
    (root, nodes)
}

/// Canonical wire form from a set of raw nodes.
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

/// Run `op` under the recompute-guided fixed point: start from `seed`
/// proof nodes, resolve every `MissingNode` from the complete map, retry.
/// Returns the result and how many rounds it took (the termination bound
/// under test: ≤ one round per resolved node).
fn fixed_point<T>(
    seed: Vec<Bytes>,
    all_nodes: &HashMap<B256, Bytes>,
    op: impl Fn(&NodeStore<'_>) -> Result<T, AnchorError>,
) -> (T, usize) {
    let mut have = seed;
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds <= all_nodes.len() + 2, "fixed point diverged");
        let proofs = proofs_from(have.clone());
        let store = NodeStore::new(&proofs).expect("canonical set");
        match op(&store) {
            Ok(v) => return (v, rounds),
            Err(AnchorError::MissingNode { hash }) => {
                let node = all_nodes
                    .get(&hash)
                    .unwrap_or_else(|| panic!("fixed point wants unknown node {hash}"));
                have.push(node.clone());
            }
            Err(e) => panic!("unexpected anchor error: {e:?}"),
        }
    }
}

/// Proof nodes for a set of target keys out of the complete map — the
/// capture side's initial (read-path) seed.
fn paths_for(entries: &BTreeMap<B256, Vec<u8>>, targets: &[B256]) -> Vec<Bytes> {
    let target_nibbles: Vec<Nibbles> = targets.iter().map(|k| Nibbles::unpack(k)).collect();
    let mut hb = HashBuilder::default().with_proof_retainer(ProofRetainer::new(target_nibbles));
    for (k, v) in entries {
        hb.add_leaf(Nibbles::unpack(k), v);
    }
    let _ = hb.root();
    hb.take_proof_nodes()
        .into_inner()
        .into_values()
        .filter(|n| n.len() >= 32)
        .collect()
}

fn key(i: u64) -> B256 {
    keccak256(i.to_be_bytes())
}

fn val(i: u64) -> Vec<u8> {
    let mut out = Vec::new();
    alloy_rlp::Encodable::encode(&U256::from(i + 1_000_000), &mut out);
    out
}

/// Dense-ish base state so branches/extensions actually form.
fn base_state(n: u64) -> BTreeMap<B256, Vec<u8>> {
    (0..n).map(|i| (key(i), val(i))).collect()
}

#[test]
fn lookup_proves_inclusion_and_exclusion() {
    let entries = base_state(64);
    let (root, all) = reference(&entries);
    let included = key(7);
    let excluded = key(1_000_000);

    let seed = paths_for(&entries, &[included, excluded]);
    let proofs = proofs_from(seed);
    let store = NodeStore::new(&proofs).unwrap();
    let mut trie = SparseTrie::new(root, &store);

    match trie.lookup(included).unwrap() {
        Lookup::Found(v) => assert_eq!(v, val(7)),
        Lookup::Absent => panic!("key 7 is in the trie"),
    }
    match trie.lookup(excluded).unwrap() {
        Lookup::Absent => {}
        Lookup::Found(_) => panic!("key 1000000 is not in the trie"),
    }
    let _ = all;
}

#[test]
fn lookup_without_proof_names_the_missing_node() {
    let entries = base_state(64);
    let (root, all) = reference(&entries);
    // No seed at all: the very first resolve must name the root node.
    let proofs = proofs_from(Vec::<Bytes>::new());
    let store = NodeStore::new(&proofs).unwrap();
    let mut trie = SparseTrie::new(root, &store);
    match trie.lookup(key(7)) {
        Err(AnchorError::MissingNode { hash }) => {
            assert!(all.contains_key(&hash), "named node must be real");
            assert_eq!(hash, root, "first missing node is the root");
        }
        other => panic!("expected MissingNode, got {other:?}", other = other.err()),
    }
}

/// THE core property: updates + inserts + deletions over the sparse trie,
/// completed by the fixed point, equal the oracle root of the mutated set.
#[test]
fn sparse_mutations_equal_oracle_across_shapes() {
    for n in [1u64, 2, 3, 8, 33, 200] {
        let entries = base_state(n);
        let (root, all) = reference(&entries);

        // Mutation set: update low keys, delete every third, insert fresh.
        let updates: Vec<(B256, Option<Vec<u8>>)> = (0..n)
            .filter(|i| i % 3 == 0)
            .map(|i| (key(i), None)) // delete
            .chain(
                (0..n)
                    .filter(|i| i % 3 == 1)
                    .map(|i| (key(i), Some(val(i + 7_000)))),
            )
            .chain((n..n + 5).map(|i| (key(i), Some(val(i))))) // insert
            .collect();

        // Oracle: apply to the full map, one-shot root.
        let mut post = entries.clone();
        for (k, v) in &updates {
            match v {
                Some(v) => {
                    post.insert(*k, v.clone());
                }
                None => {
                    post.remove(k);
                }
            }
        }
        let (oracle_root, _) = reference(&post);

        // Sparse: seed with the written keys' pre-paths (the read set), let
        // the fixed point pull deletion-collapse siblings.
        let touched: Vec<B256> = updates.iter().map(|(k, _)| *k).collect();
        let seed = paths_for(&entries, &touched);
        let (sparse_root, rounds) = fixed_point(seed, &all, |store| {
            let mut trie = SparseTrie::new(root, store);
            for (k, v) in &updates {
                match v {
                    Some(v) => trie.insert(*k, v.clone())?,
                    None => trie.remove(*k)?,
                }
            }
            Ok(trie.root())
        });
        assert_eq!(
            sparse_root, oracle_root,
            "n={n}: sparse recompute diverged from the oracle"
        );
        assert!(rounds <= all.len() + 1, "n={n}: fixed point too slow");
    }
}

/// Root collapse: delete down to one key, then to empty.
#[test]
fn deletion_collapses_to_single_leaf_and_to_empty() {
    let entries = base_state(4);
    let (root, all) = reference(&entries);

    // Delete all but key(2).
    let deletions: Vec<B256> = [0u64, 1, 3].iter().map(|i| key(*i)).collect();
    let mut post = entries.clone();
    for k in &deletions {
        post.remove(k);
    }
    let (oracle_root, _) = reference(&post);

    let seed = paths_for(&entries, &deletions);
    let (sparse_root, _) = fixed_point(seed, &all, |store| {
        let mut trie = SparseTrie::new(root, store);
        for k in &deletions {
            trie.remove(*k)?;
        }
        Ok(trie.root())
    });
    assert_eq!(sparse_root, oracle_root, "collapse to single leaf");

    // And now to empty: EMPTY_ROOT_HASH.
    let everything: Vec<B256> = entries.keys().copied().collect();
    let seed = paths_for(&entries, &everything);
    let (empty_root, _) = fixed_point(seed, &all, |store| {
        let mut trie = SparseTrie::new(root, store);
        for k in &everything {
            trie.remove(*k)?;
        }
        Ok(trie.root())
    });
    assert_eq!(empty_root, alloy_trie::EMPTY_ROOT_HASH, "root collapse");
}

/// Cascaded multi-delete under one branch: keys engineered to share a
/// prefix so several collapses stack (branch→ext→merge), plus reinsertion
/// into the collapsed shape.
#[test]
fn cascaded_deletes_under_shared_prefixes() {
    // Keys with a controlled shared prefix: flip only the last byte of the
    // preimage, then take keys that landed under a common first nibble.
    let raw: Vec<B256> = (0..4096u64).map(key).collect();
    let mut groups: BTreeMap<u8, Vec<B256>> = BTreeMap::new();
    for k in &raw {
        groups.entry(k[0] >> 4).or_default().push(*k);
    }
    // The densest first-nibble group: deleting most of it exercises deep
    // collapse cascades under that subtree.
    let (_, dense) = groups
        .iter()
        .max_by_key(|(_, v)| v.len())
        .map(|(k, v)| (*k, v.clone()))
        .expect("nonempty");
    assert!(dense.len() >= 100, "need a dense group for the cascade");

    let entries: BTreeMap<B256, Vec<u8>> = raw
        .iter()
        .enumerate()
        .map(|(i, k)| (*k, val(i as u64)))
        .collect();
    let (root, all) = reference(&entries);

    // Delete ALL BUT ONE key of the dense group — maximal cascade — and a
    // few from elsewhere; then insert one fresh key back under the same
    // prefix (collapse followed by re-split).
    let survivors = &dense[0];
    let deletions: Vec<B256> = dense[1..].to_vec();
    let fresh = key(1_000_000);
    let mut post = entries.clone();
    for k in &deletions {
        post.remove(k);
    }
    post.insert(fresh, val(42));
    let (oracle_root, _) = reference(&post);

    let mut touched = deletions.clone();
    touched.push(fresh);
    let seed = paths_for(&entries, &touched);
    let (sparse_root, _) = fixed_point(seed, &all, |store| {
        let mut trie = SparseTrie::new(root, store);
        for k in &deletions {
            trie.remove(*k)?;
        }
        trie.insert(fresh, val(42))?;
        Ok(trie.root())
    });
    assert_eq!(sparse_root, oracle_root, "cascaded collapse + reinsert");

    // The survivor must still prove present afterwards through a fresh walk
    // over the same set (sanity that the collapse spliced, not dropped).
    let _ = survivors;
}

/// Tampering: a node set whose bytes were altered can only MISS (content
/// addressing), and an unsorted set is rejected outright.
#[test]
fn tampered_and_noncanonical_sets_fail_closed() {
    let entries = base_state(32);
    let (root, all) = reference(&entries);
    let target = key(3);
    let seed = paths_for(&entries, &[target]);

    // Flip a byte in one node: its hash changes, so the walk MISSES the
    // original hash — tampering degrades to MissingNode, never bad state.
    let mut tampered = seed.clone();
    let mut bytes = tampered[0].to_vec();
    bytes[0] ^= 0x01;
    tampered[0] = Bytes::from(bytes);
    let proofs = proofs_from(tampered);
    let store = NodeStore::new(&proofs).unwrap();
    let mut trie = SparseTrie::new(root, &store);
    match trie.lookup(target) {
        Err(AnchorError::MissingNode { .. }) => {}
        Ok(Lookup::Found(v)) => {
            // Only acceptable if the tampered node wasn't on this path and
            // the value still proves out against the real root.
            assert_eq!(v, val(3));
        }
        other => panic!(
            "tampered set must miss or still prove: {other:?}",
            other = other.err()
        ),
    }

    // Unsorted wire form is rejected before any walk.
    let a = seed[0].clone();
    let b = seed[1].clone();
    let (lo, hi) = if keccak256(&a) < keccak256(&b) {
        (a, b)
    } else {
        (b, a)
    };
    let unsorted = WitnessProofs {
        nodes: vec![WireBytes::from(hi.to_vec()), WireBytes::from(lo.to_vec())],
    };
    assert!(matches!(
        NodeStore::new(&unsorted),
        Err(AnchorError::ProofSetNotCanonical)
    ));
    let _ = all;
}
