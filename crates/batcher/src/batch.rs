//! `BatchAccumulator` groups transactions into per-block batches at
//! `BlockBoundaryStart` markers.
//!
//! The sealer emits boundaries onto B. Today, the
//! batcher reads only tx_ordering. It never queries the live sequencer.
//!
//! The accumulator is stream-oriented. It reads one ordered sequence of
//! records, with transactions and boundary markers mixed together. It emits
//! one [`ClosedBlock`] per boundary. Transactions before the first boundary
//! belong to the first block.

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
    pub txs: Vec<RecordedTx>,
}

#[derive(Clone, Debug, Default)]
pub struct BatchAccumulator {
    pending: Vec<RecordedTx>,
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

    /// Observe a boundary. Closes the current block and returns it. The next
    /// calls to `observe_tx` add to the next block.
    pub fn observe_boundary(&mut self, b: BlockBoundaryStart) -> ClosedBlock {
        let txs = std::mem::take(&mut self.pending);
        ClosedBlock {
            block_number: b.block_number,
            l2_timestamp: b.l2_timestamp,
            end_tx_idx: b.end_tx_idx,
            txs,
        }
    }

    /// Number of buffered txs not yet attributed to a block.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
}
