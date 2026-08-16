//! `BatchAccumulator` — groups txs into per-block batches at
//! `BlockBoundaryStart` markers.
//!
//! Spec: §2.6 (sealer emits boundaries onto B) plus S0 (batcher
//! consumes tx_ordering only; never queries the live sequencer).
//!
//! The accumulator is **stream-oriented**: it sees a single ordered sequence of
//! records — interleaved txs, remote-epoch records and boundary markers — and
//! emits one [`ClosedBlock`] per boundary. Records received before the first
//! boundary are attributed to the first block.
//!
//! Remote-epoch records LEAD the block they are attributed to: the sealer
//! closes the open block on a remote-epoch origin advance, so on the stream a
//! record always arrives right after a boundary, before the next block's txs —
//! and its messages execute at the head of that next block. Buffering it
//! alongside the pending txs and draining both at the boundary therefore
//! preserves the canonical order exactly.

use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedTx {
    pub position: BPosition,
    pub envelope: TxEnvelope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClosedBlock {
    pub block_number: u64,
    pub l2_timestamp: u64,
    pub end_tx_idx: BPosition,
    /// Remote-epoch records leading this block (canonical-stream order).
    pub remote_epochs: Vec<RemoteEpochRecord>,
    pub txs: Vec<RecordedTx>,
}

#[derive(Clone, Debug, Default)]
pub struct BatchAccumulator {
    pending: Vec<RecordedTx>,
    pending_remote_epochs: Vec<RemoteEpochRecord>,
}

impl BatchAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe a tx record. Buffered until the next boundary closes the block.
    pub fn observe_tx(&mut self, env: TxEnvelope, pos: BPosition) {
        self.pending.push(RecordedTx {
            position: pos,
            envelope: env,
        });
    }

    /// Observe a remote-epoch record. Buffered until the next boundary closes
    /// the block it LEADS (see the module docs for why "leads" is right).
    pub fn observe_remote_epoch(&mut self, rec: RemoteEpochRecord) {
        self.pending_remote_epochs.push(rec);
    }

    /// Observe a boundary. Closes the current block and returns it. The next
    /// `observe_tx` calls will accumulate into the next block.
    pub fn observe_boundary(&mut self, b: BlockBoundaryStart) -> ClosedBlock {
        let txs = std::mem::take(&mut self.pending);
        let remote_epochs = std::mem::take(&mut self.pending_remote_epochs);
        ClosedBlock {
            block_number: b.block_number,
            l2_timestamp: b.l2_timestamp,
            end_tx_idx: b.end_tx_idx,
            remote_epochs,
            txs,
        }
    }

    /// Number of buffered txs not yet attributed to a block.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
}
