//! Block-STM engine, P2 (spec: `docs/agents/block-stm-executor-spec.md`).
//!
//! Pessimistic-by-prediction parallel execution over a multi-version cache:
//! the footprint predictor (P0-measured, P1-shadow-validated) turns each
//! block into a dependency DAG BEFORE anything executes — predicted overlap
//! is authoritative ORDER, so conflicting txs never run concurrently and
//! there are no abort storms to absorb. Parallelism comes from the
//! workload's true structure (distinct pools / books / senders); cold or
//! unpredictable txs take the serial `Tail` lane at the block's end.
//!
//! This crate is the OFFLINE milestone (P2a): the engine + its A/B
//! validation harness seam. It is deliberately not wired into the live
//! executor — that is P3 (`--parallel-execution`), which rides the
//! determinism suite and the in-stack validator.
//!
//! Correctness stance (spec invariants):
//! 1. Byte-identical receipts + delta vs sequential execution, enforced in
//!    tests and by the A/B harness on every run.
//! 2. Heuristics affect scheduling only: every recorded read is VALIDATED
//!    against the multi-version cache after execution; any violation
//!    discards the block and re-executes it sequentially
//!    (`fallback` in [`execute::StmOutcome`] — invariant #3).
//! 3. The `Accumulator` fee sink (P0: written by 100% of txs — a universal
//!    serializer without this) is served DEFERRED: workers read the
//!    block-start value, per-tx credits are folded as exact deltas, and the
//!    canonical-order commit materializes absolute balances and fixes up
//!    each write-set hash — receipts and delta land byte-identical to
//!    sequential. (The BALANCE-opcode runtime guard is a P3 concern; the
//!    P1 shadow already measures its trigger rate as ~zero, and the A/B
//!    equivalence check catches any workload that violates the assumption.)

/// FNV-1a `BuildHasher` for the engine's internal maps.
///
/// Their keys — addresses, slot hashes, domain tuples — are already
/// high-entropy, so SipHash's collision resistance against adversarial
/// input buys nothing here and costs real time: the state caches are
/// probed ~7 times per transaction on the hottest path in the engine.
/// A collision would cost lookup time, never correctness.
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
        // Final avalanche so a 64-bit key that only differs in high
        // bits still spreads across the table's low bits.
        let mut h = self.0;
        h ^= h >> 32;
        h = h.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        h ^= h >> 29;
        h
    }
    /// WORD-AT-A-TIME. The keys this map family sees (addresses, slot
    /// hashes, (address, slot) pairs) are already high-entropy, so a
    /// per-8-byte multiply-xorshift mixes them fully; the byte-at-a-time
    /// FNV loop it replaces was ~50 dependent-latency cycles per 20-byte
    /// key and hashed twice per upsert — measured as the serial feed's
    /// last-toucher stage (0.40µs/tx). Tail bytes fold in as one word.
    fn write(&mut self, bytes: &[u8]) {
        const K: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let w = u64::from_le_bytes(c.try_into().unwrap());
            self.0 = (self.0 ^ w).wrapping_mul(K).rotate_left(23);
        }
        let rem = chunks.remainder();
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

/// Re-exported so harnesses can pre-decode for BOTH engines (see
/// `execute::execute_block_sequential_decoded`).
pub use kardamom_exec_core::executor::decode_alloy_envelope;
pub mod mv;
pub mod schedule;

/// The accumulator-marked fee sink (mirrors `kardamom_exec_core::block_env`:
/// beneficiary = address(0), basefee = 0 — the V0 documented burn).
pub const FEE_SINK: alloy_primitives::Address = alloy_primitives::Address::ZERO;
