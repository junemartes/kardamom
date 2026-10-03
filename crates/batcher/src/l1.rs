//! Live L1 data-availability I/O.
//!
//! This module posts packed batches to `KardamomL2Settlement` and reads
//! them back for reconstruction.
//!
//! The write path ([`post_batch`]) disperses the batch's payload through
//! the EigenDA proxy and gets a certificate; it sends
//! `postBatch(prevIndex, daCert, start, end, recordsCommitment)`. L1 holds
//! the ordering and the certificates. EigenDA holds the bytes.
//!
//! The read path ([`read_posted_batches`] and [`recover_blocks`]) walks the
//! `BatchPosted` event log in index order. It fetches each batch's payload
//! from a [`PayloadSource`] by the certificate L1 committed to, and
//! decodes it back into the ordered [`BlockFrame`] stream, the input the
//! `kardamom-reconstruct` crate re-executes to rebuild L2 state.

use alloy_network::TransactionBuilder;
use alloy_primitives::{Address, Bytes};
use alloy_provider::Provider;
use alloy_rpc_types_eth::{Filter, TransactionRequest};
use alloy_sol_types::{SolCall, SolEvent};

use crate::batcher::PostedBatch;
use crate::da::{DaProxy, PayloadSource};
use crate::error::BatcherError;
use kardamom_types::kar1::BlockFrame;
use crate::recon::reconstruct;
use crate::settlement::IKardamomL2Settlement;

/// One posted batch as recovered from an on-chain `BatchPosted` event,
/// or as the inbox indexer serves it (its JSON carries these fields and
/// more).
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
pub struct BatchDescriptor {
    pub index: u64,
    /// The EigenDA certificate of the batch's payload, as the proxy
    /// returned it: one version byte and an RLP body.
    pub da_cert: Bytes,
    pub l2_block_start: u64,
    pub l2_block_end: u64,
}

/// Post one packed batch: disperse its payload through `da`, then record
/// the certificate on L1.
///
/// `provider` must be wallet-filled with the authorized batcher EOA.
/// `prev_batch_index` is the contract's current `lastBatchIndex` (a
/// compare-and-swap guard against replay). Returns the new batch index on
/// success.
///
/// # Errors
/// Returns an error when the dispersal fails, the transaction fails to
/// send or reverts, or the receipt cannot be fetched.
pub async fn post_batch<P: Provider>(
    provider: &P,
    settlement: Address,
    prev_batch_index: u64,
    batch: &PostedBatch,
    da: &DaProxy,
) -> Result<u64, BatcherError> {
    // The bytes are in the DA layer before the certificate lands on L1. A
    // reconstructor that sees the event can then always find them.
    let da_cert = da.put(&batch.payload).await?;

    let calldata = IKardamomL2Settlement::postBatchCall {
        prevBatchIndex: prev_batch_index,
        daCert: da_cert,
        l2BlockStart: batch.l2_block_start,
        l2BlockEnd: batch.l2_block_end,
        recordsCommitment: batch.records_commitment,
    }
    .abi_encode();

    let tx = TransactionRequest::default()
        .with_to(settlement)
        .with_input(calldata);

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
    prev_batch_index
        .checked_add(1)
        .ok_or_else(|| BatcherError::L1("prev_batch_index overflowed u64".into()))
}

/// Read all `BatchPosted` events from `settlement`, at or after
/// `from_block`. Returns them in ascending batch-index order.
///
/// # Errors
/// Returns an error when the log query or an event's decode fails.
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

    let mut out = logs
        .iter()
        .map(|log| {
            let ev = IKardamomL2Settlement::BatchPosted::decode_log(&log.inner)
                .map_err(|e| BatcherError::L1(format!("decode BatchPosted: {e}")))?;
            Ok(BatchDescriptor {
                index: ev.data.batchIndex,
                da_cert: ev.data.daCert.clone(),
                l2_block_start: ev.data.l2BlockStart,
                l2_block_end: ev.data.l2BlockEnd,
            })
        })
        .collect::<Result<Vec<_>, BatcherError>>()?;
    out.sort_by_key(|d| d.index);
    Ok(out)
}

/// Recover the ordered [`BlockFrame`] stream for `descriptors`. Fetch each
/// batch's payload from `source` by the certificate L1 committed to, and
/// decode it. `descriptors` must be in ascending index order, as
/// [`read_posted_batches`] returns them.
///
/// The source checks the bytes against the certificate: the proxy
/// recomputes the KZG commitment on every read, and the indexer serves
/// what it read from its proxy. This function trusts its source the way
/// it trusts the process's own memory.
///
/// # Errors
/// Returns an error when fetching a payload or decoding it fails.
pub fn recover_blocks<S: PayloadSource>(
    descriptors: &[BatchDescriptor],
    source: &S,
) -> Result<Vec<BlockFrame>, BatcherError> {
    descriptors
        .iter()
        .map(|d| {
            source
                .fetch_payload(&d.da_cert)
                .and_then(|p| reconstruct(&p))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|batches| batches.into_iter().flatten().collect())
}
