//! `KardamomL2Settlement` sol! bindings and a Rust post helper.
//!
//! The contract is the pure DA sink. Calldata is the abi-encoded
//! `(prevBatchIndex, blobVersionedHashes, l2BlockStart, l2BlockEnd)`
//! payload. The blob bytes travel in the 4844 sidecar.

use alloy_eips::eip4844::{Blob, kzg_to_versioned_hash};
use alloy_primitives::{Address, B256};
use alloy_sol_types::sol;

use crate::error::BatcherError;

sol!(
    #[sol(rpc)]
    #[derive(Debug)]
    IKardamomL2Settlement,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/out/KardamomL2Settlement.sol/KardamomL2Settlement.json"
    )
);

/// Compute the versioned hash from the KZG commitment, for each blob. Used
/// to assemble the contract calldata. This function accepts a precomputed
/// commitment for each blob, so callers that already have one (or a
/// deterministic test stub) need not recompute it from the trusted setup.
#[must_use]
pub fn versioned_hashes_from_commitments(commitments: &[[u8; 48]]) -> Vec<B256> {
    commitments
        .iter()
        .map(|c| kzg_to_versioned_hash(c.as_slice()))
        .collect()
}

/// Assembles and validates the parameters `postBatch(...)` expects, so the
/// CLI and tests can inspect what would be sent.
#[derive(Clone, Debug)]
pub struct PostBatchParams {
    pub settlement: Address,
    pub prev_batch_index: u64,
    pub blobs: Vec<Blob>,
    pub versioned_hashes: Vec<B256>,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
    pub records_commitment: B256,
}

impl PostBatchParams {
    /// # Errors
    /// Returns an error when `blobs` and `versioned_hashes` have different
    /// lengths, `blobs` is empty, or `l2_block_end < l2_block_start`.
    pub fn new(
        settlement: Address,
        prev_batch_index: u64,
        blobs: Vec<Blob>,
        versioned_hashes: Vec<B256>,
        l2_block_start: u64,
        l2_block_end: u64,
        records_commitment: B256,
    ) -> Result<Self, BatcherError> {
        if blobs.len() != versioned_hashes.len() {
            return Err(BatcherError::L1(
                "blobs / versioned_hashes length mismatch".into(),
            ));
        }
        if blobs.is_empty() {
            return Err(BatcherError::L1("post requires at least one blob".into()));
        }
        if l2_block_end < l2_block_start {
            return Err(BatcherError::L1("l2_block_end < l2_block_start".into()));
        }
        Ok(Self {
            settlement,
            prev_batch_index,
            blobs,
            versioned_hashes,
            l2_block_start,
            l2_block_end,
            records_commitment,
        })
    }
}
