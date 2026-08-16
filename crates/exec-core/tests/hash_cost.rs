//! Cost of the consensus witness (`WriteSet::hash`) in isolation.
//!
//! The STM commit tail's hash+validate lanes measured 1.44 µs/tx on
//! transfers, ~70% of it hashing — far above what 3 keccak permutations
//! should cost. This separates the hash's OWN cost (hot data) from the
//! cost of reaching cold write sets scattered across a 1.8MB result
//! carcass, because the two have completely different fixes.
use alloy_primitives::{Address, B256, U256};
use kardamom_exec_core::delta::WriteSet;

fn transfer_ws(i: u64) -> WriteSet {
    let mut ws = WriteSet::default();
    for k in 0..3u64 {
        ws.accounts.push((
            Address::with_last_byte((i + k) as u8),
            (i, U256::from(1_000_000u64 + i + k), B256::ZERO),
        ));
    }
    ws.finish();
    ws
}

#[test]
fn write_set_hash_cost() {
    let sets: Vec<WriteSet> = (0..1000).map(transfer_ws).collect();
    for w in &sets {
        std::hint::black_box(w.hash());
    }
    const REPS: usize = 50;
    let t = std::time::Instant::now();
    for _ in 0..REPS {
        for w in &sets {
            std::hint::black_box(w.hash());
        }
    }
    let hot = t.elapsed().as_nanos() as f64 / (REPS * sets.len()) as f64;

    // Cold: stride through 64MB between hashes so every write set is a
    // fresh miss, as it is in the tail.
    let junk = vec![7u8; 16 << 20];
    let mut acc = 0u64;
    let t = std::time::Instant::now();
    for (n, w) in sets.iter().enumerate() {
        acc += junk[(n * 4093) % junk.len()] as u64;
        std::hint::black_box(w.hash());
    }
    let cold = t.elapsed().as_nanos() as f64 / sets.len() as f64;
    eprintln!(
        "WriteSet::hash 3-account: hot {hot:.0} ns/tx | cold-ish {cold:.0} ns/tx (junk {acc})"
    );
    assert!(hot > 0.0);
}
