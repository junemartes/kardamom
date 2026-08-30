//! Block-STM engine.
//!
//! This engine predicts contention before it executes. The footprint
//! predictor (measured offline, shadow-validated online) builds a dependency
//! DAG for the whole block before any transaction runs. Predicted overlap
//! becomes the execution order, so conflicting transactions never run at
//! the same time, and no abort storms happen. Parallelism comes from the
//! real structure of the workload (distinct pools, books, or senders).
//! Cold or unpredictable transactions run in the serial `Tail` lane at the
//! end of the block.
//!
//! This crate is an offline milestone: the engine and its A/B
//! validation harness. It is not wired into the live executor yet. That
//! wiring uses the `--parallel-execution` flag, the determinism suite,
//! and the in-stack validator.
//!
//! Correctness rules (spec invariants):
//! 1. Receipts and the delta must be byte-identical to sequential
//!    execution. Tests and the A/B harness check this on every run.
//! 2. Heuristics affect scheduling only. The engine validates every
//!    recorded read against the multi-version cache after execution. A
//!    violation discards the block and re-executes it sequentially
//!    (`fallback` in [`execute::StmOutcome`] — invariant #3).
//! 3. The `Accumulator` fee sink is written by almost every transaction,
//!    so it would force full serialization without one exception: each
//!    worker reads its block-start value, and every transaction's credit
//!    is folded as an exact delta. The canonical-order commit then
//!    computes absolute balances and fixes up each write-set hash, so
//!    receipts and the delta still match sequential execution byte for
//!    byte. (A runtime guard for the BALANCE opcode is a live-executor
//!    concern. The footprint shadow scheduler shows its trigger rate is
//!    near zero, and the A/B
//!    equivalence check catches any workload that breaks this assumption.)

/// Fast `BuildHasher` for the engine's internal maps.
///
/// The keys (addresses, slot hashes, domain tuples) are already
/// high-entropy. SipHash's defense against adversarial input costs real
/// time and buys nothing here: the hottest path in the engine probes
/// these caches about 7 times per transaction. A hash collision costs
/// lookup time only. It never affects correctness.
#[derive(Default, Clone, Copy)]
pub struct FnvBuild;

pub struct Fnv(u64);

impl std::hash::BuildHasher for FnvBuild {
    type Hasher = Fnv;
    fn build_hasher(&self) -> Fnv {
        Fnv(0xcbf2_9ce4_8422_2325)
    }
}

impl std::hash::Hasher for Fnv {
    fn finish(&self) -> u64 {
        // A final avalanche step. It spreads a 64-bit key that differs
        // only in its high bits across the table's low bits.
        let mut h = self.0;
        h ^= h >> 32;
        h = h.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 29;
        h
    }
    /// Hashes 8 bytes at a time.
    ///
    /// The keys this map family sees (addresses, slot hashes, and
    /// (address, slot) pairs) are already high-entropy, so a per-8-byte
    /// multiply-xorshift mixes them well. This replaces a byte-at-a-time
    /// FNV loop that cost about 50 cycles per 20-byte key and ran twice
    /// per upsert. The remaining tail bytes fold in as one word.
    fn write(&mut self, bytes: &[u8]) {
        const K: u64 = 0x9E37_79B9_7F4A_7C15;
        let (chunks, rem) = bytes.as_chunks::<8>();
        for c in chunks {
            let w = u64::from_le_bytes(*c);
            self.0 = (self.0 ^ w).wrapping_mul(K).rotate_left(23);
        }
        if !rem.is_empty() {
            let mut buf = [0u8; 8];
            buf[..rem.len()].copy_from_slice(rem);
            let w = u64::from_le_bytes(buf) ^ ((rem.len() as u64) << 56);
            self.0 = (self.0 ^ w).wrapping_mul(K).rotate_left(23);
        }
    }
    fn write_u64(&mut self, n: u64) {
        self.write(&n.to_le_bytes());
    }
    fn write_u32(&mut self, n: u32) {
        self.write(&n.to_le_bytes());
    }
    fn write_u8(&mut self, n: u8) {
        self.write(&[n]);
    }
}

/// Hash map using [`FnvBuild`].
pub type FastMap<K, V> = std::collections::HashMap<K, V, FnvBuild>;

pub mod execute;
pub mod pool;

/// Re-exported so harnesses can pre-decode for both engines (see
/// `execute::execute_block_sequential_decoded`).
pub use kardamom_exec_core::executor::DecodedTx;
pub mod mv;
pub mod schedule;

/// The fee sink the accumulator marks (mirrors
/// `kardamom_exec_core::block_env`: beneficiary = address(0), basefee = 0 —
/// the documented V0 burn).
pub const FEE_SINK: alloy_primitives::Address = alloy_primitives::Address::ZERO;
