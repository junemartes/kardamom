//! Live L1 data-availability I/O: post packed batches as real EIP-4844 blob
//! transactions to `KardamomL2Settlement`, and read them back for
//! reconstruction.
//!
//! The write path ([`post_batch`]) builds a trusted-setup-backed blob sidecar
//! from the batcher's packed blobs, records each blob in the DA store keyed by
//! its KZG versioned hash, and sends the `postBatch(prevIndex, versionedHashes,
//! start, end)` transaction with the sidecar attached. L1 thereby holds the
//! ordering + commitments; the DA store holds the bytes.
//!
//! The read path ([`read_posted_batches`] + [`recover_blocks`]) walks the
//! `BatchPosted` event log in index order, fetches each batch's blobs from a
//! [`BlobSource`] by the versioned hashes L1 committed to, and decodes them back
//! into the ordered [`BlockFrame`] stream — the input to
//! [`crate::reexec::reconstruct_state`].

use alloy_consensus::BlobTransactionSidecarVariant;
use alloy_eips::eip4844::BlobTransactionSidecar;
use alloy_eips::eip4844::env_settings::EnvKzgSettings;
use alloy_network::{TransactionBuilder, TransactionBuilder4844};
use alloy_primitives::{Address, B256};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, TransactionRequest};
use alloy_sol_types::{SolCall, SolEvent};

use crate::batcher::PostedBatch;
use crate::da_store::{BlobSource, FsBlobStore};
use crate::error::BatcherError;
use crate::frame::BlockFrame;
use crate::recon::reconstruct;
use crate::settlement::IKardamomL2Settlement;

/// Blob-gas fee cap for posted batches. Deliberately generous; the DA sink is
/// not latency-sensitive and blob base fees are tiny on an idle chain.
pub const DEFAULT_MAX_FEE_PER_BLOB_GAS: u128 = 1_000_000_000; // 1 gwei

/// One posted batch as recovered from an on-chain `BatchPosted` event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchDescriptor {
    pub index: u64,
    /// KZG versioned hashes of the batch's blobs, in blob order.
    pub versioned_hashes: Vec<B256>,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
}

/// Build a real 4844 sidecar (KZG commitments + proofs via the env trusted
/// setup) from already-packed blobs.
pub fn build_sidecar(
    blobs: Vec<alloy_eips::eip4844::Blob>,
) -> Result<BlobTransactionSidecar, BatcherError> {
    // `try_from_blobs` (no settings) is test-only gated; the production entry
    // point takes the trusted setup explicitly. `EnvKzgSettings::Default` is the
    // mainnet KZG ceremony output baked into alloy.
    BlobTransactionSidecar::try_from_blobs_with_settings(blobs, EnvKzgSettings::Default.get())
        .map_err(|e| BatcherError::L1(format!("build blob sidecar: {e}")))
}

/// Post one packed batch to L1 as an EIP-4844 blob transaction, recording its
/// blobs in `da_store` keyed by versioned hash.
///
/// `provider` must be wallet-filled with the authorized batcher EOA.
/// `prev_batch_index` is the contract's current `lastBatchIndex` (CAS replay
/// guard). Returns the new batch index on success.
pub async fn post_batch<P: Provider>(
    provider: &P,
    settlement: Address,
    prev_batch_index: u64,
    batch: &PostedBatch,
    da_store: &FsBlobStore,
) -> Result<u64, BatcherError> {
    let sidecar = build_sidecar(batch.blobs.clone())?;
    let versioned_hashes: Vec<B256> = sidecar.versioned_hashes().collect();
    if versioned_hashes.len() != batch.blobs.len() {
        return Err(BatcherError::L1(format!(
            "sidecar produced {} hashes for {} blobs",
            versioned_hashes.len(),
            batch.blobs.len()
        )));
    }

    // Record the bytes in the DA layer *before* the commitment lands on L1, so a
    // reconstructor that sees the event can always find the blobs.
    for (blob, vh) in batch.blobs.iter().zip(versioned_hashes.iter()) {
        da_store.put(*vh, blob)?;
    }

    let calldata = IKardamomL2Settlement::postBatchCall {
        prevBatchIndex: prev_batch_index,
        blobVersionedHashes: versioned_hashes.clone(),
        l2BlockStart: batch.l2_block_start,
        l2BlockEnd: batch.l2_block_end,
        recordsCommitment: batch.records_commitment,
    }
    .abi_encode();

    let tx = TransactionRequest::default()
        .with_to(settlement)
        .with_input(calldata)
        .with_blob_sidecar(BlobTransactionSidecarVariant::Eip4844(sidecar))
        .with_max_fee_per_blob_gas(DEFAULT_MAX_FEE_PER_BLOB_GAS);

    let receipt = provider
        .send_transaction(tx)
        .await
        .map_err(|e| BatcherError::L1(format!("send postBatch: {e}")))?
        .get_receipt()
        .await
        .map_err(|e| BatcherError::L1(format!("await postBatch receipt: {e}")))?;
    if !receipt.status() {
        return Err(BatcherError::L1("postBatch reverted".into()));
    }
    Ok(prev_batch_index + 1)
}

/// Read all `BatchPosted` events from `settlement` at or after `from_block`,
/// returned in ascending batch-index order.
pub async fn read_posted_batches<P: Provider>(
    provider: &P,
    settlement: Address,
    from_block: u64,
) -> Result<Vec<BatchDescriptor>, BatcherError> {
    let filter = Filter::new()
        .address(settlement)
        .event_signature(IKardamomL2Settlement::BatchPosted::SIGNATURE_HASH)
        .from_block(from_block);
    let logs = provider
        .get_logs(&filter)
        .await
        .map_err(|e| BatcherError::L1(format!("get_logs BatchPosted: {e}")))?;

    let mut out = Vec::with_capacity(logs.len());
    for log in logs {
        let ev = IKardamomL2Settlement::BatchPosted::decode_log(&log.inner)
            .map_err(|e| BatcherError::L1(format!("decode BatchPosted: {e}")))?;
        out.push(BatchDescriptor {
            index: ev.data.batchIndex,
            versioned_hashes: ev.data.blobHashes.clone(),
            l2_block_start: ev.data.l2BlockStart,
            l2_block_end: ev.data.l2BlockEnd,
        });
    }
    out.sort_by_key(|d| d.index);
    Ok(out)
}

/// Recover the ordered [`BlockFrame`] stream for `descriptors` by fetching each
/// batch's blobs from `source` (by the versioned hashes L1 committed to) and
/// decoding them. `descriptors` must be in ascending index order (as returned
/// by [`read_posted_batches`]).
pub fn recover_blocks<S: BlobSource>(
    descriptors: &[BatchDescriptor],
    source: &S,
) -> Result<Vec<BlockFrame>, BatcherError> {
    let mut blocks = Vec::new();
    for d in descriptors {
        let mut blobs = Vec::with_capacity(d.versioned_hashes.len());
        for vh in &d.versioned_hashes {
            let blob = source.fetch_blob(*vh)?;
            verify_blob_against_hash(*vh, &blob)?;
            blobs.push(blob);
        }
        let frames = reconstruct(&blobs)?;
        blocks.extend(frames);
    }
    Ok(blocks)
}

/// Prove that `blob`'s bytes are the ones L1 committed to as `versioned_hash`:
/// recompute the KZG commitment and derive its versioned hash.
///
/// The DA store is an UNTRUSTED boundary — it is keyed by versioned hash but
/// nothing about a filesystem (or a future networked store) guarantees the
/// bytes behind a name. Without this, a store returning right-length wrong
/// bytes was silently accepted and `kardamom-reconstruct` rebuilt a WRONG
/// chain from it — from the layer that exists to be the last-resort backstop
/// when every in-cluster durable copy is gone. The hash is already in the
/// `BatchPosted` event, so the check costs one commitment per blob on a
/// recovery path that runs at most once per incident.
pub fn verify_blob_against_hash(
    versioned_hash: B256,
    blob: &alloy_eips::eip4844::Blob,
) -> Result<(), BatcherError> {
    // Recomputed through the SAME helper that produced the hash on the post
    // path, so producer and verifier can never drift apart.
    let sidecar = build_sidecar(vec![*blob])?;
    let got = sidecar.versioned_hashes().next().ok_or_else(|| {
        BatcherError::Corruption("sidecar produced no versioned hash".to_string())
    })?;
    if got != versioned_hash {
        return Err(BatcherError::Corruption(format!(
            "DA blob content does not match its commitment: L1 committed to \
             {versioned_hash}, stored bytes hash to {got}"
        )));
    }
    Ok(())
}
