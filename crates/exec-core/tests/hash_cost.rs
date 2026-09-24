//! Cost of the consensus witness (`WriteSet::hash`) in isolation.
//!
//! The STM commit tail's hash-and-validate lanes measured 1.44 µs/tx on
//! transfers, about 70% of it spent hashing. That is far above what 3
//! keccak permutations should cost. This separates the hash's own cost
//! (hot data) from the cost of reaching cold write sets scattered across
//! a 1.8MB result set, because the two need completely different fixes.
use alloy_primitives::{Address, B256, U256};
use kardamom_exec_core::delta::{AccountFields, WriteSet};

mod common;
use common::ns_per_op;

fn transfer_ws(i: u64) -> WriteSet {
    let mut ws = WriteSet::default();
    for k in 0..3u64 {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "the address suffix wraps mod 256 on purpose: this only needs 3000 distinct-enough addresses, not a faithful account identity"
        )]
        let addr = Address::with_last_byte((i + k) as u8);
        ws.accounts.push((
            addr,
            AccountFields {
                nonce: i,
                balance: U256::from(1_000_000u64 + i + k),
                code_hash: B256::ZERO,
            },
        ));
    }
    ws.finish();
    ws
}

#[test]
fn write_set_hash_cost() {
    const REPS: usize = 50;
    let sets: Vec<WriteSet> = (0..1000).map(transfer_ws).collect();
    for w in &sets {
        std::hint::black_box(w.hash());
    }
    let hot = ns_per_op(REPS, sets.len(), || {
        for w in &sets {
            std::hint::black_box(w.hash());
        }
    });

    // Cold: stride through 16MB between hashes, so every write set is a
    // fresh cache miss, matching the tail's access pattern.
    let junk = vec![7u8; 16 << 20];
    let mut acc = 0u64;
    let t = std::time::Instant::now();
    for (n, w) in sets.iter().enumerate() {
        acc += u64::from(junk[(n * 4093) % junk.len()]);
        std::hint::black_box(w.hash());
    }
    #[allow(
        clippy::cast_precision_loss,
        reason = "a nanosecond-scale timing report; precision loss here is irrelevant"
    )]
    let cold = t.elapsed().as_nanos() as f64 / sets.len() as f64;
    eprintln!(
        "WriteSet::hash 3-account: hot {hot:.0} ns/tx | cold-ish {cold:.0} ns/tx (junk {acc})"
    );
    assert!(hot > 0.0);
}
