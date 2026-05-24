//! `KardamomL2Settlement` sol! bindings + Rust post helper.
//!
//! The contract is the pure DA sink (S0 D-Sh11). Calldata is the abi-encoded
//! `(prevBatchIndex, blobVersionedHashes, l2BlockStart, l2BlockEnd)` payload;
//! blob bytes ride along in the 4844 sidecar.

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

/// Compute KZG → versioned-hash for each blob. Used to assemble the contract
/// calldata. The blob → KZG commitment step requires the trusted setup; in v0
/// we accept either pre-computed commitments (passed in) or, for tests, a
/// deterministic stub. The real broadcast path (future task) uses
/// `alloy-consensus::BlobTransactionSidecar` which carries its own
/// commitments.
pub fn versioned_hashes_from_commitments(commitments: &[[u8; 48]]) -> Vec<B256> {
    commitments
        .iter()
        .map(|c| kzg_to_versioned_hash(c.as_slice()))
        .collect()
}

/// Best-effort placeholder: assemble the parameters expected by
/// `postBatch(...)`. The actual transaction broadcast (with sidecar) is a
/// future-task concern; this helper just packages the fields together so the
/// CLI + tests can inspect what would be sent.
#[derive(Clone, Debug)]
pub struct PostBatchParams {
    pub settlement: Address,
    pub prev_batch_index: u64,
    pub blobs: Vec<Blob>,
    pub versioned_hashes: Vec<B256>,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
}

impl PostBatchParams {
    pub fn new(
        settlement: Address,
        prev_batch_index: u64,
        blobs: Vec<Blob>,
        versioned_hashes: Vec<B256>,
        l2_block_start: u64,
        l2_block_end: u64,
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
        })
    }
}
