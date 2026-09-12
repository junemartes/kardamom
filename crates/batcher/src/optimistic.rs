//! Optimistic-mode L1 drivers: the claim poster and the challenge driver.
//! See the no-std-exec-core spec. Both drivers read from the validator's
//! prover spool. Each block's `expected-outputs.bin` (the 160-byte
//! `PublicOutputs`) already carries the `(post_root, records_digest)` pair
//! that a claim attests to and a watcher cross-checks.
//!
//! The two cadences stay decoupled. The poster claims a batch once the spool
//! covers it. The challenger compares pending claims against the spool and,
//! at the first divergent offset, submits the single-block proof the prover
//! made for that block (`zk-host --prove` on the spooled frame). Neither
//! driver blocks on the other.

use std::path::{Path, PathBuf};

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

/// The validator's prover spool: one `block-N/` directory per block,
/// each with the block's `expected-outputs.bin` and, once the prover has
/// run, its single-block proof files.
struct Spool {
    dir: PathBuf,
}

impl Spool {
    /// Read one block's expected outputs.
    fn outputs(&self, block: u64) -> Option<PublicOutputs> {
        let bytes =
            std::fs::read(self.dir.join(format!("block-{block}/expected-outputs.bin"))).ok()?;
        PublicOutputs::decode(&bytes)
    }

    /// Assemble `(roots, digests)` for a posted range. `Err` names the
    /// first block the spool has not covered.
    fn sequences(&self, start: u64, end: u64) -> Result<(Vec<B256>, Vec<B256>), u64> {
        // A capacity hint only; a bad estimate costs a realloc, not
        // correctness — EXCEPT for a plain `end - start`, which underflows
        // to a near-u64::MAX span when `end < start` (a malformed
        // settlement entry) and turns a hint into a huge allocation
        // request. `end < start` makes the loop below a no-op
        // (`start..=end` is empty), so a missing checked step falls back
        // to 0, matching what the loop actually does.
        let cap = end
            .checked_sub(start)
            .and_then(|span| span.checked_add(1))
            .and_then(|span| usize::try_from(span).ok())
            .unwrap_or(0);
        let mut roots = Vec::with_capacity(cap);
        let mut digests = Vec::with_capacity(roots.capacity());
        for n in start..=end {
            let out = self.outputs(n).ok_or(n)?;
            roots.push(out.post_state_root);
            digests.push(out.records_digest);
        }
        Ok((roots, digests))
    }

    /// Read the prover's single-block proof files for `block`, if the
    /// prover has produced both of them yet.
    fn block_proof(&self, block: u64) -> Option<(Vec<u8>, Vec<u8>)> {
        let dir = self.dir.join(format!("block-{block}"));
        let pv = std::fs::read(dir.join("public-values.bin")).ok()?;
        let proof = std::fs::read(dir.join("proof.bin")).ok()?;
        Some((pv, proof))
    }
}

/// The claim poster: the proof oracle it claims on, and the spool whose
/// attestations it claims with.
pub struct BatchClaimer {
    oracle: Address,
    spool: Spool,
}

impl BatchClaimer {
    #[must_use]
    pub fn new(oracle: Address, spool_dir: &Path) -> Self {
        Self {
            oracle,
            spool: Spool {
                dir: spool_dir.to_path_buf(),
            },
        }
    }

    /// Claim the next unclaimed posted batch, using the spool's
    /// attestations. Read the bond from the oracle (`minBond`).
    ///
    /// # Errors
    /// Returns an error when an L1 call fails, or when `claimBatch`
    /// reverts.
    pub async fn claim_next<P: Provider>(&self, provider: P) -> Result<ClaimOutcome, BatcherError> {
        let oracle = IKardamomProofOracle::new(self.oracle, &provider);
        let highest_claimed = oracle
            .highestClaimedBatch()
            .call()
            .await
            .map_err(|e| BatcherError::L1(format!("highestClaimedBatch: {e}")))?;
        let next = highest_claimed
            .checked_add(1)
            .ok_or_else(|| BatcherError::L1("highestClaimedBatch overflowed u64".into()))?;
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
        let (roots, digests) = match self.spool.sequences(entry.l2BlockStart, entry.l2BlockEnd) {
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

/// The first block offset, within the claim's range, where a claimed root
/// differs from the spool's local root. Takes no state (just two slices),
/// so it stays a free function rather than a method on [`BatchWatcher`].
fn first_divergent_offset(claimed_roots: &[B256], local_roots: &[B256]) -> Option<u64> {
    claimed_roots
        .iter()
        .enumerate()
        .find(|(i, claimed_root)| local_roots.get(*i) != Some(*claimed_root))
        .map(|(i, _)| i as u64)
}

/// The challenge driver: the proof oracle it watches, and the local
/// prover spool it compares pending claims against.
pub struct BatchWatcher {
    oracle: Address,
    spool: Spool,
}

impl BatchWatcher {
    #[must_use]
    pub fn new(oracle: Address, spool_dir: &Path) -> Self {
        Self {
            oracle,
            spool: Spool {
                dir: spool_dir.to_path_buf(),
            },
        }
    }

    /// The claim's attested arrays, decoded from the `claimBatch` call's
    /// own calldata. The `BatchClaimed` event stores only `seqHash`, so
    /// the arrays it attested to must be re-read from the claim
    /// transaction.
    async fn claimed_arrays_from_log<P: Provider>(
        &self,
        provider: &P,
        batch_index: u64,
    ) -> Result<IKardamomProofOracle::claimBatchCall, BatcherError> {
        let oracle = IKardamomProofOracle::new(self.oracle, provider);
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
        let tx_hash = log
            .transaction_hash
            .ok_or_else(|| BatcherError::L1("claim event without tx hash".into()))?;
        let tx = provider
            .get_transaction_by_hash(tx_hash)
            .await
            .map_err(|e| BatcherError::L1(format!("claim tx fetch: {e}")))?
            .ok_or_else(|| BatcherError::L1("claim tx not found".into()))?;
        IKardamomProofOracle::claimBatchCall::abi_decode(tx.input())
            .map_err(|e| BatcherError::L1(format!("claim calldata decode: {e}")))
    }

    /// Compare the next pending claim against the spool. At the first
    /// divergent offset, submit `challengeBlock` with the prover's files
    /// (`block-N/{public-values.bin, proof.bin}`, the single-block
    /// layout).
    ///
    /// # Errors
    /// Returns an error when an L1 call, log query, or transaction fetch
    /// fails, or when `challengeBlock` reverts.
    pub async fn watch_and_challenge<P: Provider>(
        &self,
        provider: P,
    ) -> Result<WatchOutcome, BatcherError> {
        let oracle = IKardamomProofOracle::new(self.oracle, &provider);
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
        let batch_index = last_finalized
            .checked_add(1)
            .ok_or_else(|| BatcherError::L1("lastFinalizedBatch overflowed u64".into()))?;
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

        // Rebuild the claimed sequences from the spool and compare seqHash. If
        // they match, the claim matches the spool's view. Treat it as honest.
        let Ok((roots, digests)) = self.spool.sequences(entry.l2BlockStart, entry.l2BlockEnd)
        else {
            return Ok(WatchOutcome::NothingPending);
        };
        let local_seq_hash = alloy_primitives::keccak256(alloy_sol_types::SolValue::abi_encode(&(
            roots.clone(),
            digests.clone(),
        )));
        if local_seq_hash == claim.seqHash {
            return Ok(WatchOutcome::ClaimHonest { batch_index });
        }

        // The claim is divergent. Re-derive the claimed arrays from the claim
        // transaction's own calldata, then find the first offset where the
        // local spool root differs from it.
        let call = self.claimed_arrays_from_log(&provider, batch_index).await?;
        let Some(block_offset) = first_divergent_offset(&call.blockRoots, &roots) else {
            // The roots agree but the digests differ. This cannot happen past
            // the fold check. Treat it as honest instead of raising a
            // challenge that cannot win.
            return Ok(WatchOutcome::ClaimHonest { batch_index });
        };
        let divergent_block = entry
            .l2BlockStart
            .checked_add(block_offset)
            .ok_or_else(|| BatcherError::L1("l2BlockStart + block_offset overflowed u64".into()))?;
        let Some((pv, proof)) = self.spool.block_proof(divergent_block) else {
            return Ok(WatchOutcome::ProofNotReady {
                batch_index,
                divergent_block,
            });
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
}
