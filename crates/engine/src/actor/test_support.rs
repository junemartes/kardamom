//! Shared fixtures for the actor's test modules: canonical-position and
//! signed-legacy-tx builders, writer-signal / writer-queue test doubles, and
//! the commit-channel drain helper.

use std::sync::{Arc, Mutex};

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::Receiver;
use kardamom_types::{BPosition, BlockBoundary, BlockDelta, TxEnvelope as KtTxEnvelope};

use crate::error::ExecutorError;

use super::{ExecToCommit, StateWriterQueue, StateWriterSignal};

pub(super) fn pos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

pub(super) fn legacy(
    signer: &PrivateKeySigner,
    to: Address,
    nonce: u64,
    value: u64,
) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(value),
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
    let raw_tx = Bytes::from(alloy_env.encoded_2718());
    let tx_hash = keccak256(&raw_tx);
    KtTxEnvelope {
        correlation_id: 0,
        raw_tx,
        sender: signer.address(),
        tx_hash,
    }
}

pub(super) struct ImmediateCommit;
impl StateWriterSignal for ImmediateCommit {
    fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
        Ok(at_least)
    }
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(u64::MAX)
    }
}

/// Writer signal whose durable level is test-controlled: `committed()`
/// reads it; `wait_committed` (the depth-cap block) jumps it to the
/// target and counts the call.
pub(super) struct StagedCommit {
    pub(super) durable: Arc<std::sync::atomic::AtomicU64>,
    pub(super) blocking_waits: Arc<std::sync::atomic::AtomicU64>,
}
impl StateWriterSignal for StagedCommit {
    fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
        self.blocking_waits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.durable
            .fetch_max(at_least, std::sync::atomic::Ordering::SeqCst);
        Ok(self.durable.load(std::sync::atomic::Ordering::SeqCst))
    }
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(self.durable.load(std::sync::atomic::Ordering::SeqCst))
    }
}

pub(super) struct RecordingQueue(pub(super) Arc<Mutex<Vec<(BlockBoundary, BlockDelta)>>>);
impl StateWriterQueue for RecordingQueue {
    fn submit(&mut self, b: BlockBoundary, d: BlockDelta) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push((b, d));
        Ok(())
    }
}

/// Drain a closed `ExecToCommit` receiver, returning the block numbers of
/// emitted receipts and boundaries (each in order).
pub(super) fn drain_commits(rx: Receiver<ExecToCommit>) -> (Vec<u64>, Vec<u64>) {
    let mut receipts = Vec::new();
    let mut boundaries = Vec::new();
    while let Ok(m) = rx.recv() {
        match m {
            ExecToCommit::Receipt(r) => receipts.push(r.block_number),
            ExecToCommit::Boundary(b) => boundaries.push(b.block_number),
        }
    }
    (receipts, boundaries)
}

/// Records every submitted block AND applies it to a shared
/// `MockStateDatabase`, so a later block's snapshot observes an earlier
/// block's writes.
///
/// Pair with `MutatingSnapshotSource` over the same handle for any test that
/// spans more than one block. Plain `RecordingQueue` + `StaticSnapshotSource`
/// silently loses committed state: the settle sweep drops a settled block from
/// the parent read layer on the assumption that the refreshed snapshot now
/// contains it, which a static snapshot never does — so multi-block state
/// carry-over reads as zero and a test can "pass" against behaviour production
/// would never produce.
pub(super) struct ApplyingRecordingQueue {
    pub(super) db: kardamom_exec_core::state::MockStateDatabase,
    pub(super) log: Arc<Mutex<Vec<(BlockBoundary, BlockDelta)>>>,
}

impl StateWriterQueue for ApplyingRecordingQueue {
    fn submit(&mut self, b: BlockBoundary, d: BlockDelta) -> Result<(), ExecutorError> {
        self.db.apply_block_delta(&d);
        self.log.lock().unwrap().push((b, d));
        Ok(())
    }
}
