//! The proof submitter (spec: no-std-exec-core, PR 4): posts batch validity
//! proofs to the `KardamomProofOracle`, aligned with the settlement's
//! L1-as-truth batch cursor.
//!
//! Fully decoupled cadence: the oracle names the next unproven batch, the
//! settlement's stored entry names its range, and the submitter posts the
//! prover's output files for that range when they exist — otherwise it
//! reports "not yet" and the caller retries later. Submission is
//! PERMISSIONLESS on the contract; the proof is the authorization.

use std::path::Path;

use alloy_primitives::Address;
use alloy_provider::Provider;
use alloy_sol_types::sol;
use kardamom_types::BatchPublicOutputs;

use crate::error::BatcherError;
use crate::settlement::IKardamomL2Settlement;

sol!(
    #[sol(rpc)]
    #[derive(Debug)]
    IKardamomProofOracle,
    concat!(
        env!("CARGO_WORKSPACE_DIR"),
        "/contracts/out/KardamomProofOracle.sol/KardamomProofOracle.json"
    )
);

/// What one submission attempt did.
#[derive(Debug, PartialEq, Eq)]
pub enum SubmitOutcome {
    /// The next unproven batch's proof was submitted and accepted.
    Submitted { batch_index: u64 },
    /// The settlement has no entry for the next batch yet (batcher lags).
    NoBatchPosted { batch_index: u64 },
    /// The prover has not produced this batch's files yet (prover lags).
    ProofNotReady { batch_index: u64 },
}

/// Submit the NEXT unproven batch's proof, if both the batch and its proof
/// files exist. `proofs_dir` holds the zk-host layout:
/// `batch-<first>-<last>/{public-values.bin, proof.bin}`.
pub async fn submit_next_proof<P: Provider>(
    provider: P,
    oracle_addr: Address,
    proofs_dir: &Path,
) -> Result<SubmitOutcome, BatcherError> {
    let oracle = IKardamomProofOracle::new(oracle_addr, &provider);
    let last_finalized = oracle
        .lastFinalizedBatch()
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("lastFinalizedBatch: {e}")))?;
    let next = last_finalized + 1;

    let settlement_addr = oracle
        .settlement()
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("oracle.settlement: {e}")))?;
    let settlement = IKardamomL2Settlement::new(settlement_addr, &provider);
    let entry = settlement
        .batches(next)
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("settlement.batches({next}): {e}")))?;
    if entry.recordsCommitment == alloy_primitives::B256::ZERO {
        return Ok(SubmitOutcome::NoBatchPosted { batch_index: next });
    }

    let dir = proofs_dir.join(format!("batch-{}-{}", entry.l2BlockStart, entry.l2BlockEnd));
    let pv = match std::fs::read(dir.join("public-values.bin")) {
        Ok(b) => b,
        Err(_) => return Ok(SubmitOutcome::ProofNotReady { batch_index: next }),
    };
    let proof = std::fs::read(dir.join("proof.bin")).unwrap_or_default();

    // Fail fast client-side on anything the contract would reject: cheaper
    // than a revert, and a precise error beats a raw one.
    let decoded = BatchPublicOutputs::decode(&pv)
        .ok_or_else(|| BatcherError::L1("malformed public-values.bin".into()))?;
    if decoded.first_block != entry.l2BlockStart
        || decoded.last_block != entry.l2BlockEnd
        || decoded.records_commitment != entry.recordsCommitment
    {
        return Err(BatcherError::L1(format!(
            "proof files for batch {next} do not match the posted entry \
             (range {}..={} vs {}..={})",
            decoded.first_block, decoded.last_block, entry.l2BlockStart, entry.l2BlockEnd
        )));
    }

    let receipt = oracle
        .submitBatchProof(next, pv.into(), proof.into())
        .send()
        .await
        .map_err(|e| BatcherError::L1(format!("submitBatchProof({next}): {e}")))?
        .get_receipt()
        .await
        .map_err(|e| BatcherError::L1(format!("submitBatchProof({next}) receipt: {e}")))?;
    if !receipt.status() {
        return Err(BatcherError::L1(format!(
            "submitBatchProof({next}) reverted on-chain"
        )));
    }
    Ok(SubmitOutcome::Submitted { batch_index: next })
}
