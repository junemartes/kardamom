//! The pipeline metrics of the exec thread and the commit thread.

use std::time::Instant;

use kardamom_obs::pipeline::{Batch, LoopMeter, Queue, Step};
use metrics::Counter;

/// The metrics of the exec thread. The thread owns them.
///
/// The loop meter charges the wait on the reader channel to idle, and the
/// handling of a message to busy. A send to a full commit channel, and the
/// wait for a free commit slot, are blocked.
pub(super) struct ExecMeters {
    pub(super) meter: LoopMeter,
    /// The messages that wait in the channel from the reader.
    pub(super) reader_to_exec: Queue,
    /// The blocks that the writer did not settle yet.
    pub(super) commits_inflight: Queue,
    block_txs: Batch,
    boundary: Step,
    gas_used: Counter,
}

impl ExecMeters {
    pub(super) fn new() -> Self {
        Self {
            meter: LoopMeter::new("executor.exec"),
            reader_to_exec: Queue::new("executor.reader_to_exec"),
            commits_inflight: Queue::new("executor.commits_inflight"),
            block_txs: Batch::new("executor.block_txs"),
            boundary: Step::new("executor.boundary"),
            gas_used: metrics::counter!(crate::metrics::GAS_USED_TOTAL),
        }
    }

    /// A block with `records` records and `gas` gas is sealed.
    pub(super) fn sealed(&self, records: u64, gas: u64) {
        // A u64 fits a usize on a 64-bit target. Elsewhere the value
        // saturates: no block holds that many records.
        self.block_txs
            .record(usize::try_from(records).unwrap_or(usize::MAX));
        self.gas_used.increment(gas);
    }

    /// The boundary that began at `started` is done.
    pub(super) fn boundary_done(&self, started: Instant) {
        self.boundary.since(started);
    }
}

/// The metrics of the commit thread. The thread owns them.
///
/// The loop meter charges the wait on the exec channel to idle, and the
/// publish to busy. The pause between two must-deliver attempts is
/// blocked: the publication has no room for the batch.
pub(super) struct CommitMeters {
    pub(super) meter: LoopMeter,
    /// The messages that wait in the channel from the exec thread.
    pub(super) exec_to_commit: Queue,
    receipt_batch: Batch,
    receipt_publish: Step,
}

impl CommitMeters {
    pub(super) fn new() -> Self {
        Self {
            meter: LoopMeter::new("executor.commit"),
            exec_to_commit: Queue::new("executor.exec_to_commit"),
            receipt_batch: Batch::new("executor.receipt_batch"),
            receipt_publish: Step::new("executor.receipt_publish"),
        }
    }

    /// A batch of `receipts` receipts that began to publish at `started`
    /// is delivered.
    pub(super) fn published(&self, receipts: usize, started: Instant) {
        self.receipt_publish.since(started);
        self.receipt_batch.record(receipts);
    }
}
