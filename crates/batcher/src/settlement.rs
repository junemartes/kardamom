//! `KardamomL2Settlement` sol! bindings and the post parameters.
//!
//! The contract is the pure DA sink. Calldata is the abi-encoded
//! `(prevBatchIndex, daCert, l2BlockStart, l2BlockEnd, recordsCommitment)`.
//! The payload lives in EigenDA under the certificate.

use alloy_primitives::{Address, B256, Bytes};
use alloy_sol_types::sol;

use crate::error::BatcherError;

sol!(
    #[sol(rpc)]
    #[derive(Debug)]
    IKardamomL2Settlement,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/abi/KardamomL2Settlement.json"
    )
);

/// The parameters `postBatch(...)` expects, validated, so the CLI and
/// tests can inspect what would be sent.
#[derive(Clone, Debug)]
pub struct PostBatchParams {
    pub settlement: Address,
    pub prev_batch_index: u64,
    /// The EigenDA certificate of the payload.
    pub da_cert: Bytes,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
    pub records_commitment: B256,
}

impl PostBatchParams {
    /// # Errors
    /// Returns an error when `da_cert` is empty or `l2_block_end <
    /// l2_block_start`.
    pub fn new(
        settlement: Address,
        prev_batch_index: u64,
        da_cert: Bytes,
        l2_block_start: u64,
        l2_block_end: u64,
        records_commitment: B256,
    ) -> Result<Self, BatcherError> {
        if da_cert.is_empty() {
            return Err(BatcherError::L1("post requires a certificate".into()));
        }
        if l2_block_end < l2_block_start {
            return Err(BatcherError::L1("l2_block_end < l2_block_start".into()));
        }
        Ok(Self {
            settlement,
            prev_batch_index,
            da_cert,
            l2_block_start,
            l2_block_end,
            records_commitment,
        })
    }
}
