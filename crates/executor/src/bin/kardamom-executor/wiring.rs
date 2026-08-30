//! Role-specific adapters and construction: the executor's `EngineWiring`,
//! the live tx_receipts publication, and the opt-in Block-STM strategy.

use anyhow::{Context, Result};
use kardamom_engine::actor::BlockExec;
use kardamom_engine::bin_support;
use kardamom_executor::{
    CMessage, EngineWiring, ExecutorError, MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal,
    NoEpochCheck, TxReceiptsPublication,
};
use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsPublisherHandle};
use kardamom_log::config::ChannelsConfig;
use kardamom_state::StateSnapshot;

use crate::args::Args;

/// tx_receipts publication. With MDS (fan-in) enabled, this replica
/// publishes the receipt stream and the boundary side-stream to its own
/// per-replica unicast endpoint (chosen by `--recorder-id`). Ingress
/// combines every replica's endpoint into one multi-destination
/// subscription. Without MDS (the IPC default), the code falls back to the
/// shared single-channel path, so single-host and local behavior stays the
/// same. Either way, the commit thread's must-deliver retry drives the same
/// `publish_receipt` and `publish_boundary` surface.
pub(crate) fn open_tx_receipts_pub(
    rt_pub: &AeronRuntime,
    channels: &ChannelsConfig,
    args: &Args,
) -> Result<LiveTxReceiptsPub> {
    let handle = if channels.tx_receipts_mds_enabled() {
        tracing::info!(
            replica_idx = args.recorder_id,
            endpoint = channels.tx_receipts_endpoint(args.recorder_id).as_deref(),
            "tx_receipts MDS publish (per-replica endpoint)"
        );
        TxReceiptsPublisherHandle::open_mds(rt_pub, channels, args.recorder_id)
            .context("open TxReceiptsPublisherHandle (MDS)")?
    } else {
        TxReceiptsPublisherHandle::open(rt_pub, channels)
            .context("open TxReceiptsPublisherHandle")?
    };
    Ok(LiveTxReceiptsPub { handle })
}

/// Block-STM execution strategy (opt-in). The pool server starts at
/// startup and lives for the whole process. Blocks route through it at
/// each boundary. `None` leaves the engine's streaming per-tx path
/// unchanged.
pub(crate) fn build_block_exec(args: &Args) -> Option<BlockExec<StateSnapshot>> {
    if !args.parallel_execution {
        return None;
    }
    // 0 means auto. The hard cap is 40, from the mdbx reader-slot budget
    // (geometry::MAX_READERS = 64, shared with exec, RPC, and compaction).
    let workers = match args.execution_workers {
        0 => std::thread::available_parallelism()
            .map(|n| n.get().min(8))
            .unwrap_or(4),
        n => n.min(40),
    };
    tracing::info!(
        workers,
        "parallel execution ENABLED (Block-STM, block-at-a-time)"
    );
    Some(kardamom_executor::parallel::stm_block_exec(
        kardamom_executor::parallel::StmExecConfig {
            workers,
            pin_cores: Vec::new(),
            keep_hot: false,
        },
    ))
}

// ---------------------------------------------------------------------------
// Role-specific adapter: tx_receipts publication.
// ---------------------------------------------------------------------------

/// The executor role's port types: the concrete mdbx and Aeron
/// implementations, used throughout. This binary makes no runtime
/// implementation choices, so nothing needs the boxed-wiring escape hatch.
pub(crate) struct ExecutorWiring;

impl EngineWiring for ExecutorWiring {
    type TxData = bin_support::LiveTxDataSub;
    type TxOrdering = bin_support::LiveTxOrderingSub;
    type TxReceipts = LiveTxReceiptsPub;
    type Snapshots = MdbxSnapshotSource;
    type WriterSignal = MdbxWriterSignal;
    type WriterQueue = MdbxWriterQueue;
    // No epoch verification: the executor trusts the ordered stream.
    type Epoch = NoEpochCheck;
}

pub(crate) struct LiveTxReceiptsPub {
    handle: TxReceiptsPublisherHandle,
}

impl TxReceiptsPublication for LiveTxReceiptsPub {
    /// One `Vec<Receipt>` wire frame per batch: one encode and one blocking
    /// ack round trip through the Aeron thread, instead of one per receipt.
    /// Each frame is all-or-nothing. A transient failure reports 0
    /// published, and the commit thread's must-deliver loop retries the
    /// whole batch. The duplicates are harmless: tx_receipts delivers at
    /// least once, and consumers dedupe on `tx_idx`.
    fn publish_receipts(
        &mut self,
        receipts: &[kardamom_types::Receipt],
    ) -> (usize, Option<ExecutorError>) {
        match self.handle.publish_receipts(&receipts.to_vec()) {
            Ok(_) => (receipts.len(), None),
            Err(e) => (
                0,
                Some(ExecutorError::State(format!("publish_receipts: {e}"))),
            ),
        }
    }

    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(r) => self
                .handle
                .publish_receipt(&r)
                .map(|_| ())
                .map_err(|e| ExecutorError::State(format!("publish_receipt: {e}"))),
            // Best-effort: a block-boundary marker. Ingress acks on the receipt
            // or durable watermark, not on this marker. Blocking the commit
            // thread here (for example, at startup before ingress's MDS
            // destinations attach) would freeze all state progress. This call
            // is fire-and-forget, so empty blocks never stall the executor.
            // A dropped boundary is harmless.
            CMessage::BlockBoundary(b) => self
                .handle
                .publish_boundary_best_effort(&b)
                .map_err(|e| ExecutorError::State(format!("publish_boundary: {e}"))),
        }
    }
}
