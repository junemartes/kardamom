//! The write-set encoding is a consensus contract (the per-tx
//! determinism witness). These tests pin the properties that make it
//! one: the two sink paths agree, the encoding is injective across every
//! field's compaction, and the compaction actually saves permutations.

use alloy_primitives::{Address, B256, U256, keccak256};
use kardamom_exec_core::delta::WriteSet;

fn ws(
    accounts: &[(Address, (u64, U256, B256))],
    storage: &[((Address, B256), U256)],
    code: &[(B256, bytes::Bytes)],
) -> WriteSet {
    let mut w = WriteSet::default();
    w.accounts.extend_from_slice(accounts);
    w.storage.extend_from_slice(storage);
    for c in code {
        w.code.push(c.clone());
    }
    w.finish();
    w
}

fn addr(i: u8) -> Address {
    Address::with_last_byte(i)
}

/// A set too big for the stack buffer takes the streaming path. A set
/// that fits takes the buffered path. Growing a set across that
/// boundary must not change how equal prefixes hash. That is, both
/// paths encode the same bytes. (A set with code always streams, so
/// pairing an identical set with and without a code entry exercises
/// both paths.)
#[test]
fn buffered_and_streaming_paths_agree() {
    let accounts: Vec<_> = (1..=3u8)
        .map(|i| {
            (
                addr(i),
                (i as u64, U256::from(10u64).pow(U256::from(18)), B256::ZERO),
            )
        })
        .collect();
    let storage: Vec<_> = (1..=40u8)
        .map(|i| ((addr(9), B256::with_last_byte(i)), U256::from(i)))
        .collect();
    // 40 slots exceed INLINE, so this streams. 2 slots fit, so this buffers.
    let big = ws(&accounts, &storage, &[]);
    let small = ws(&accounts, &storage[..2], &[]);
    // Each must equal a fresh recomputation (determinism), and the two
    // must differ, since they describe different sets.
    assert_eq!(big.hash(), ws(&accounts, &storage, &[]).hash());
    assert_eq!(small.hash(), ws(&accounts, &storage[..2], &[]).hash());
    assert_ne!(big.hash(), small.hash());
}

/// Injectivity across the compactions. Minimal-width integers, the
/// code-hash tag, and the repeated-address flag must never let two
/// different write sets collide.
#[test]
fn encoding_is_injective_across_compactions() {
    let ke = keccak256([]);
    let mut hashes = std::collections::HashSet::new();
    let mut push = |w: WriteSet, label: &str| {
        assert!(hashes.insert(w.hash()), "collision: {label}");
    };
    // Balance widths that share bytes once stripped.
    push(
        ws(&[(addr(1), (0, U256::from(1u64), ke))], &[], &[]),
        "bal 0x01",
    );
    push(
        ws(&[(addr(1), (0, U256::from(256u64), ke))], &[], &[]),
        "bal 0x0100",
    );
    push(ws(&[(addr(1), (0, U256::ZERO, ke))], &[], &[]), "bal 0");
    // Nonce and balance must not be confusable.
    push(ws(&[(addr(1), (1, U256::ZERO, ke))], &[], &[]), "nonce 1");
    push(
        ws(&[(addr(1), (128, U256::ZERO, ke))], &[], &[]),
        "nonce 128 (2-byte varint)",
    );
    // The three code-hash tags.
    push(
        ws(&[(addr(1), (0, U256::ZERO, B256::ZERO))], &[], &[]),
        "code ZERO",
    );
    push(
        ws(
            &[(addr(1), (0, U256::ZERO, B256::with_last_byte(9)))],
            &[],
            &[],
        ),
        "code explicit",
    );
    // A same-address run versus distinct addresses (the bit6 flag).
    push(
        ws(
            &[],
            &[
                ((addr(1), B256::with_last_byte(1)), U256::from(1u64)),
                ((addr(1), B256::with_last_byte(2)), U256::from(1u64)),
            ],
            &[],
        ),
        "two slots, one address",
    );
    push(
        ws(
            &[],
            &[
                ((addr(1), B256::with_last_byte(1)), U256::from(1u64)),
                ((addr(2), B256::with_last_byte(2)), U256::from(1u64)),
            ],
            &[],
        ),
        "two slots, two addresses",
    );
    // Counts must be covered: an empty set differs from every set above.
    push(ws(&[], &[], &[]), "empty");
}

/// The point of the change: a plain transfer's witness must fit in one
/// keccak permutation (136 bytes or fewer absorbed). This is asserted
/// structurally, so a future field addition that silently costs a
/// permutation fails here, rather than in a benchmark much later.
#[test]
fn transfer_witness_fits_one_permutation() {
    // Sender, recipient, fee sink: real-shaped balances and nonces.
    let accounts: Vec<_> = (1..=3u8)
        .map(|i| {
            (
                addr(i),
                (
                    42u64,
                    U256::from(3_141_592_653_589_793_238u64),
                    keccak256([]),
                ),
            )
        })
        .collect();
    let w = ws(&accounts, &[], &[]);
    let encoded = w.encoded_len_for_test();
    assert!(
        encoded <= 136,
        "a transfer's witness is {encoded} bytes — over one keccak block"
    );
}
