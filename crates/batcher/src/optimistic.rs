//! Optimistic-mode L1 drivers (spec: no-std-exec-core, PR 5): the claim
//! poster and the challenge driver, both fed by the validator's prover
//! SPOOL — whose per-block `expected-outputs.bin` (the 160-byte
//! `PublicOutputs`) already carries exactly the `(post_root,
//! records_digest)` pair a claim attests and a watcher cross-checks.
//!
//! Cadences stay decoupled, as everywhere in this series: the poster
//! claims when the spool has covered a posted batch; the challenger
//! compares pending claims against the spool and, at the FIRST divergent
//! offset, submits the single-block proof the prover produced for that
//! block (`zk-host --prove` on the spooled frame). Nothing stalls.

use std::path::Path;

use alloy_consensus::transaction::Transaction as _;
use alloy_primitives::{Address, B256, U256};
use alloy_provider::Provider;
use alloy_sol_types::SolCall;
use kardamom_types::PublicOutputs;

use crate::error::BatcherError;
use crate::prover_submit::IKardamomProofOracle;
use crate::settlement::IKardamomL2Settlement;

/// What one claim attempt did.
#[derive(Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The next batch was claimed (bond posted).
    Claimed { batch_index: u64 },
    /// The settlement has no entry for the next batch yet.
    NoBatchPosted { batch_index: u64 },
    /// The spool has not covered the batch's range yet.
    SpoolNotReady {
        batch_index: u64,
        missing_block: u64,
    },
}

/// Read one block's expected outputs from the spool.
fn spool_outputs(spool: &Path, block: u64) -> Option<PublicOutputs> {
    let bytes = std::fs::read(spool.join(format!("block-{block}/expected-outputs.bin"))).ok()?;
    PublicOutputs::decode(&bytes)
}

/// Assemble `(roots, digests)` for a posted range from the spool.
fn spool_sequences(spool: &Path, start: u64, end: u64) -> Result<(Vec<B256>, Vec<B256>), u64> {
    let mut roots = Vec::with_capacity((end - start + 1) as usize);
    let mut digests = Vec::with_capacity(roots.capacity());
    for n in start..=end {
        let out = spool_outputs(spool, n).ok_or(n)?;
        roots.push(out.post_state_root);
        digests.push(out.records_digest);
    }
    Ok((roots, digests))
}

/// Claim the NEXT unclaimed posted batch from the spool's attestations.
/// The bond is read from the oracle (`minBond`).
pub async fn claim_next_batch<P: Provider>(
    provider: P,
    oracle_addr: Address,
    spool: &Path,
) -> Result<ClaimOutcome, BatcherError> {
    let oracle = IKardamomProofOracle::new(oracle_addr, &provider);
    let next = oracle
        .highestClaimedBatch()
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("highestClaimedBatch: {e}")))?
        + 1;
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
    if entry.recordsCommitment == B256::ZERO {
        return Ok(ClaimOutcome::NoBatchPosted { batch_index: next });
    }
    let (roots, digests) = match spool_sequences(spool, entry.l2BlockStart, entry.l2BlockEnd) {
        Ok(seqs) => seqs,
        Err(missing) => {
            return Ok(ClaimOutcome::SpoolNotReady {
                batch_index: next,
                missing_block: missing,
            });
        }
    };
    let bond = U256::from(
        oracle
            .minBond()
            .call()
            .await
            .map_err(|e| BatcherError::L1(format!("minBond: {e}")))?
            .to::<u128>(),
    );
    let receipt = oracle
        .claimBatch(next, roots, digests)
        .value(bond)
        .send()
        .await
        .map_err(|e| BatcherError::L1(format!("claimBatch({next}): {e}")))?
        .get_receipt()
        .await
        .map_err(|e| BatcherError::L1(format!("claimBatch({next}) receipt: {e}")))?;
    if !receipt.status() {
        return Err(BatcherError::L1(format!("claimBatch({next}) reverted")));
    }
    Ok(ClaimOutcome::Claimed { batch_index: next })
}

/// What one watch/challenge attempt found.
#[derive(Debug, PartialEq, Eq)]
pub enum WatchOutcome {
    /// The pending claim's roots all match the spool: honest.
    ClaimHonest { batch_index: u64 },
    /// Divergence found and the challenge was submitted with the block's
    /// proof files.
    Challenged { batch_index: u64, block_offset: u64 },
    /// Divergence found but the prover has not produced the single-block
    /// proof for that block yet (`zk-host --prove <spool>/block-N`).
    ProofNotReady {
        batch_index: u64,
        divergent_block: u64,
    },
    /// Nothing is pending (or the spool has no coverage to compare).
    NothingPending,
}

/// Compare the next pending claim against the spool; at the FIRST
/// divergent offset, submit `challengeBlock` with the prover's files
/// (`block-N/{public-values.bin, proof.bin}` — the single-block layout).
pub async fn watch_and_challenge<P: Provider>(
    provider: P,
    oracle_addr: Address,
    spool: &Path,
) -> Result<WatchOutcome, BatcherError> {
    let oracle = IKardamomProofOracle::new(oracle_addr, &provider);
    let last_finalized = oracle
        .lastFinalizedBatch()
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("lastFinalizedBatch: {e}")))?;
    let highest = oracle
        .highestClaimedBatch()
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("highestClaimedBatch: {e}")))?;
    if highest == last_finalized {
        return Ok(WatchOutcome::NothingPending);
    }
    let batch_index = last_finalized + 1;
    let settlement_addr = oracle
        .settlement()
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("oracle.settlement: {e}")))?;
    let settlement = IKardamomL2Settlement::new(settlement_addr, &provider);
    let entry = settlement
        .batches(batch_index)
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("settlement.batches({batch_index}): {e}")))?;
    let claim = oracle
        .claims(batch_index)
        .call()
        .await
        .map_err(|e| BatcherError::L1(format!("claims({batch_index}): {e}")))?;

    // Rebuild the claimed sequences from the SPOOL and compare seqHash: if
    // they match, the claim IS the spool's view — honest by our own data.
    let (roots, digests) = match spool_sequences(spool, entry.l2BlockStart, entry.l2BlockEnd) {
        Ok(seqs) => seqs,
        Err(_) => return Ok(WatchOutcome::NothingPending),
    };
    let local_seq_hash = alloy_primitives::keccak256(alloy_sol_types::SolValue::abi_encode(&(
        roots.clone(),
        digests.clone(),
    )));
    if local_seq_hash == claim.seqHash {
        return Ok(WatchOutcome::ClaimHonest { batch_index });
    }

    // Divergent claim. The claim event carries the claimed sequences; we
    // find the first offset where OUR root differs by re-deriving the
    // claimed arrays from the event log.
    let filter = oracle
        .BatchClaimed_filter()
        .topic1(U256::from(batch_index))
        .from_block(0);
    let logs = filter
        .query()
        .await
        .map_err(|e| BatcherError::L1(format!("BatchClaimed logs: {e}")))?;
    let (_, log) = logs
        .last()
        .ok_or_else(|| BatcherError::L1("claim exists but no BatchClaimed event".into()))?;
    // The event stores seqHash, not the arrays; the CLAIM TX's calldata
    // has them. v0: the divergence offset comes from comparing our spool
    // roots against the claim's final root progression — the first block
    // whose spool proof files exist AND whose claimed root (from the tx
    // calldata) differs. Fetching calldata:
    let tx_hash = log
        .transaction_hash
        .ok_or_else(|| BatcherError::L1("claim event without tx hash".into()))?;
    let tx = provider
        .get_transaction_by_hash(tx_hash)
        .await
        .map_err(|e| BatcherError::L1(format!("claim tx fetch: {e}")))?
        .ok_or_else(|| BatcherError::L1("claim tx not found".into()))?;
    let call = IKardamomProofOracle::claimBatchCall::abi_decode(tx.input())
        .map_err(|e| BatcherError::L1(format!("claim calldata decode: {e}")))?;

    let mut offset = None;
    for (i, claimed_root) in call.blockRoots.iter().enumerate() {
        if roots.get(i) != Some(claimed_root) {
            offset = Some(i as u64);
            break;
        }
    }
    let Some(block_offset) = offset else {
        // Roots agree but digests differ — impossible past the fold check;
        // treat as honest rather than invent a challenge we cannot win.
        return Ok(WatchOutcome::ClaimHonest { batch_index });
    };
    let divergent_block = entry.l2BlockStart + block_offset;
    let dir = spool.join(format!("block-{divergent_block}"));
    let (pv, proof) = match (
        std::fs::read(dir.join("public-values.bin")),
        std::fs::read(dir.join("proof.bin")),
    ) {
        (Ok(pv), Ok(proof)) => (pv, proof),
        _ => {
            return Ok(WatchOutcome::ProofNotReady {
                batch_index,
                divergent_block,
            });
        }
    };
    let receipt = oracle
        .challengeBlock(
            batch_index,
            block_offset,
            call.blockRoots,
            call.blockDigests,
            pv.into(),
            proof.into(),
        )
        .send()
        .await
        .map_err(|e| BatcherError::L1(format!("challengeBlock: {e}")))?
        .get_receipt()
        .await
        .map_err(|e| BatcherError::L1(format!("challengeBlock receipt: {e}")))?;
    if !receipt.status() {
        return Err(BatcherError::L1("challengeBlock reverted".into()));
    }
    Ok(WatchOutcome::Challenged {
        batch_index,
        block_offset,
    })
}
