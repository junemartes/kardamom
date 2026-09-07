//! Seeded parallel batch re-execution.
//!
//! # Why this can run fully in parallel
//!
//! The BAL carries write values, not only locations. So each batch of
//! txs can seed its inputs from the BAL's own claims. For every account
//! or slot the batch reads, the value is either the latest claimed write
//! by an earlier tx, or the pre-block snapshot. No batch waits for
//! another; value-passing resolves conflicts, not ordering.
//!
//! # Why seeding from unverified claims is sound
//!
//! Verification works by induction, anchored at the snapshot. Batch 1
//! runs against pure pre-block state, the ground truth, so if its
//! computed writes equal its claimed writes, those claims are true.
//! Batch 2's seeds are then verified-true inputs, and so on. A claim is
//! always checked at the batch that produces it, so a false claim cannot
//! be laundered by downstream batches that only consume it. EVM
//! determinism then forces every verified batch to equal what sequential
//! execution would produce.
//!
//! Any mismatch makes the caller record a divergence and stop. The
//! validator's other checks (per-tx receipts, merged write-set hash) are
//! unchanged and still run.
//!
//! Layout: [`claims`] indexes the BAL for seeding and comparison.
//! [`engine`] executes and verifies (batches, sequential fallback, and
//! the exec-loop strategy). [`dump`] serializes divergence inputs for
//! offline replay.

mod claims;
mod dump;
mod engine;
#[cfg(test)]
mod engine_tests;

pub use claims::ClaimIndex;
pub(crate) use dump::{claims_json, records_json};
pub(crate) use engine::execute_block_sequential;
pub use engine::{BlockInputs, execute_block_parallel, parallel_block_exec};
