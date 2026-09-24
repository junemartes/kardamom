//! The kardamom batch guest: one proof per
//! posted batch. Reads a rkyv [`BatchProverInput`], runs the same anchored
//! per-block execution as the single-block guest over each frame, and
//! chains the roots internally: block i's claimed `pre_state_root` must
//! equal block i-1's recomputed post root, so the proof's outer roots
//! attest the whole contiguous range. It folds the batch records
//! commitment (L2 txs only; deposits are L1-originated and excluded from
//! DA batches by design) and commits the 160-byte [`BatchPublicOutputs`]
//! layout the `KardamomProofOracle` decodes.
//!
//! [`BatchProverInput`]: kardamom_types::BatchProverInput
//! [`BatchPublicOutputs`]: kardamom_types::BatchPublicOutputs

#![no_main]
sp1_zkvm::entrypoint!(main);

use alloy_primitives::B256;
use kardamom_types::{batch_records_commitment, BatchProverInput, BatchPublicOutputs, ProverInput};

/// The batch's block list, proven non-empty at construction — a panic
/// here is the guest's fail-closed posture. This replaces four separate
/// emptiness checks (the assert this used to be, plus three
/// `.expect("nonempty")` calls on `first`/`last`) with the one here.
struct NonEmptyBatch(Vec<ProverInput>);

impl NonEmptyBatch {
    fn new(blocks: Vec<ProverInput>) -> Self {
        assert!(!blocks.is_empty(), "empty batch");
        Self(blocks)
    }

    fn first(&self) -> &ProverInput {
        &self.0[0]
    }

    fn last(&self) -> &ProverInput {
        &self.0[self.0.len() - 1]
    }

    fn len(&self) -> usize {
        self.0.len()
    }
}

impl IntoIterator for NonEmptyBatch {
    type Item = ProverInput;
    type IntoIter = std::vec::IntoIter<ProverInput>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Validate that `batch`'s blocks are numbered contiguously from
/// `first_block`, in one pass over the whole range. This is input
/// validation (unlike [`check_root_chain`]), so it runs once here instead
/// of once per iteration in the execution loop below.
///
/// `first_block` and every `block_number` are untrusted rkyv input, and
/// the guest release profile sets no `overflow-checks`, so a plain `+`
/// would let a wrapped range pass this check. `checked_add` turns that
/// into an explicit panic instead.
fn ensure_contiguous(batch: &NonEmptyBatch, first_block: u64) {
    for (i, block) in batch.0.iter().enumerate() {
        let number = block.boundary.block_number;
        let expected = first_block
            .checked_add(i as u64)
            .expect("batch block number overflowed u64");
        assert_eq!(number, expected, "batch blocks must be contiguous");
    }
}

/// Check the inductive root chain: this block's claimed `pre_state_root`
/// must equal the running root the prior block's post-state produced.
/// This is a real invariant of execution order, not input validation — it
/// cannot move to a constructor, because `running_root` comes from
/// execution, not from the input alone.
fn check_root_chain(block: &ProverInput, running_root: B256) {
    assert_eq!(
        block.witness.pre_state_root,
        Some(running_root),
        "root chain broken at block {}",
        block.boundary.block_number
    );
}

/// # Panics
///
/// Panics (the guest's fail-closed posture) when `input_bytes` fails to
/// decode as a [`BatchProverInput`], when the batch is empty, when the
/// blocks are not numbered contiguously, when the root chain breaks
/// between blocks, when a published BAL frame fails to decode, or when
/// anchored stateless execution fails for any block.
pub fn main() {
    let input_bytes = sp1_zkvm::io::read_vec();
    let input: BatchProverInput =
        rkyv::from_bytes::<BatchProverInput, rkyv::rancor::Error>(&input_bytes)
            .expect("batch prover input frame");
    let batch = NonEmptyBatch::new(input.blocks);
    let first_block = batch.first().boundary.block_number;
    let last_block = batch.last().boundary.block_number;
    let batch_pre_root = batch
        .first()
        .witness
        .pre_state_root
        .expect("anchored input");
    ensure_contiguous(&batch, first_block);

    let mut running_root = batch_pre_root;
    let mut block_digests = Vec::with_capacity(batch.len());
    for block in batch {
        check_root_chain(&block, running_root);

        let run = kardamom_zk_guest::GuestBlock::run(block);
        block_digests.push(run.records_digest);
        running_root = run.anchored.post_state_root;
    }

    let outputs = BatchPublicOutputs {
        pre_state_root: batch_pre_root,
        post_state_root: running_root,
        first_block,
        last_block,
        records_commitment: batch_records_commitment(block_digests),
    };
    sp1_zkvm::io::commit_slice(&outputs.encode());
}
