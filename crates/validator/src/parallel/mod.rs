//! Seeded parallel batch re-execution (spec:
//! `docs/agents/bal-attribution-parallel-validation-spec.md`, v3).
//!
//! # Why this can be FULLY parallel
//!
//! The BAL carries write **values**, not just locations. So each batch of
//! txs can have its inputs SEEDED from the BAL's own claims: for every
//! account/slot the batch reads, the value is either the latest claimed
//! write by an earlier tx, or the pre-block snapshot. No batch waits for
//! another — conflicts are resolved by value-passing, not ordering.
//!
//! # Why seeding from unverified claims is sound
//!
//! Verification is an INDUCTION anchored at the snapshot. Batch 1 executes
//! against pure pre-block state (ground truth), so if its computed writes
//! equal its claimed writes, those claims are true. Batch 2's seeds are
//! then verified-true inputs, and so on: a claim is always checked at the
//! batch that PRODUCES it, so a false claim cannot be laundered by
//! downstream batches that merely consume it. EVM determinism then forces
//! every verified batch to equal what sequential execution would produce.
//!
//! Any mismatch → the caller records a divergence and fail-stops. The
//! validator's other checks (per-tx receipts, merged write-set hash) are
//! unchanged and still run.
//!
//! Layout: [`claims`] indexes the BAL for seeding and comparison; [`engine`]
//! executes and verifies (batches + sequential fallback + the exec-loop
//! strategy); [`dump`] serializes divergence inputs for offline replay.

mod claims;
mod dump;
mod engine;
#[cfg(test)]
mod engine_tests;

pub use claims::{ClaimIndex, ClaimSlice, batch_ranges};
pub(crate) use dump::{claims_json, records_json};
pub use engine::{
    BatchOutcome, BlockOutcome, build_seed, execute_batch, execute_block_parallel,
    execute_block_sequential, parallel_block_exec,
};
