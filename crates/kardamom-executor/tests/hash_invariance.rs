//! Permutation-invariance and value-change sensitivity tests for write_set_hash.
//!
//! Property: for any WriteSet `ws`, building `ws'` by inserting the same
//! (addr, kind, key, value) tuples in a randomly shuffled order yields
//! `ws'.hash() == ws.hash()`. Sensitivity: changing any single value flips
//! the hash.

use alloy_primitives::{Address, B256, U256};
use kardamom_executor::delta::WriteSet;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

fn build(
    accounts: &[(Address, (u64, U256, B256))],
    storage: &[((Address, B256), U256)],
) -> WriteSet {
    let mut ws = WriteSet::default();
    for (a, c) in accounts {
        ws.accounts.insert(*a, *c);
    }
    for (k, v) in storage {
        ws.storage.insert(*k, *v);
    }
    ws
}

fn sample(
    seed: u64,
) -> (
    Vec<(Address, (u64, U256, B256))>,
    Vec<((Address, B256), U256)>,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let n_acc: u64 = 32;
    let n_sto: u64 = 128;
    let accounts: Vec<(Address, (u64, U256, B256))> = (0..n_acc)
        .map(|i| {
            let mut a = [0u8; 20];
            rng.fill(&mut a);
            (
                Address::from(a),
                (i, U256::from(i * 7), B256::repeat_byte((i % 256) as u8)),
            )
        })
        .collect();
    let storage: Vec<((Address, B256), U256)> = (0..n_sto)
        .map(|i| {
            let addr = accounts[(i as usize) % n_acc as usize].0;
            ((addr, B256::from(U256::from(i))), U256::from(i * 13))
        })
        .collect();
    (accounts, storage)
}

#[test]
fn permuting_input_does_not_change_hash() {
    let (accounts, storage) = sample(0xDEADBEEF);
    let base = build(&accounts, &storage).hash();

    let mut rng = ChaCha8Rng::seed_from_u64(0xC0FFEE);
    for _ in 0..16 {
        let mut a = accounts.clone();
        let mut s = storage.clone();
        a.shuffle(&mut rng);
        s.shuffle(&mut rng);
        assert_eq!(build(&a, &s).hash(), base);
    }
}

#[test]
fn flipping_one_storage_value_changes_hash() {
    let (accounts, storage) = sample(42);
    let base = build(&accounts, &storage).hash();
    let mut storage_b = storage.clone();
    storage_b[0].1 = storage_b[0].1 + U256::from(1u64);
    assert_ne!(build(&accounts, &storage_b).hash(), base);
}

#[test]
fn flipping_one_balance_changes_hash() {
    let (accounts, storage) = sample(99);
    let base = build(&accounts, &storage).hash();
    let mut accounts_b = accounts.clone();
    accounts_b[0].1.1 = accounts_b[0].1.1 + U256::from(1u64);
    assert_ne!(build(&accounts_b, &storage).hash(), base);
}
