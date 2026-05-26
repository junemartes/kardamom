//! `BatchAccumulator` — groups txs into per-block batches at
//! `BlockBoundaryStart` markers.
//!
//! Spec: §2.6 (sealer emits boundaries onto B) plus S0 D-Sh10 (batcher
//! consumes tx_ordering only; never queries the live sequencer).
//!
//! The accumulator is **stream-oriented**: it sees a single ordered sequence of
//! records — interleaved txs and boundary markers — and emits one
//! [`ClosedBlock`] per boundary. Txs received before the first boundary are
//! attributed to the first block.

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
    /// `observe_tx` calls will accumulate into the next block.
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
