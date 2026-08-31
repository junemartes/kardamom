//! `BatchAccumulator` groups transactions into per-block batches at
//! `BlockBoundaryStart` markers.
//!
//! The sealer emits boundaries onto B. Today, the
//! batcher reads only tx_ordering. It never queries the live sequencer.
//!
//! The accumulator is stream-oriented. It reads one ordered sequence of
//! records, with transactions, remote-epoch records, and boundary markers
//! mixed together. It emits one [`ClosedBlock`] per boundary. Records before
//! the first boundary belong to the first block.
//!
//! Remote-epoch records lead the block they belong to. The sealer closes the
//! open block first on a remote-epoch origin advance. So a remote-epoch
//! record always follows a boundary, before the next block's transactions.
//! Its messages run at the start of that next block. The accumulator buffers
//! the record with the pending transactions and drains both at the boundary.
//! This keeps the canonical order correct.

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
    /// calls to `observe_tx` add to the next block.
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
