//! Role-specific adapters and construction: the executor's `EngineWiring`,
//! the live `tx_receipts` publication, and the opt-in Block-STM strategy.

use std::num::NonZeroUsize;

use anyhow::{Context, Result};
use kardamom_engine::bin_support;
use kardamom_engine::{
    CMessage, EngineWiring, ExecPorts, ExecutorError, MdbxSnapshotSource, MdbxWriterQueue,
    MdbxWriterSignal, NoEpochCheck, NoRemoteEpochCheck, TxReceiptsPublication,
};
use kardamom_executor::parallel::StmBlockExec;
use kardamom_log::aeron_live::AeronRuntime;
use kardamom_log::discovery::StreamPlane;
use kardamom_state::StateSnapshot;

use crate::args::Args;

/// `tx_receipts` publication: the receipt stream and the boundary
/// side-stream, opened through the stream plane. With discovery, this
/// replica publishes two dynamic MDC publications. Without it, the
/// plane picks the per-replica MDS endpoint (chosen by `--recorder-id`)
/// or the shared channel from the static config. Either way, the commit
/// thread's must-deliver retry drives the same `publish_receipt` and
/// `publish_boundary` surface.
pub(crate) async fn open_tx_receipts_pub(
    rt_pub: &AeronRuntime,
    plane: &mut StreamPlane,
    args: &Args,
) -> Result<LiveTxReceiptsPub> {
    let handle = plane
        .tx_receipts_publisher(rt_pub, args.recorder_id)
        .await
        .context("open TxReceiptsPublisherHandle")?;
    Ok(LiveTxReceiptsPub { handle })
}

/// Block-STM execution strategy (opt-in). The pool server starts at
/// startup and lives for the whole process. Blocks route through it at
/// each boundary. `None` leaves the engine's streaming per-tx path
/// unchanged.
pub(crate) fn build_block_exec(args: &Args) -> Option<StmBlockExec<StateSnapshot>> {
    /// Upper bound for the auto worker count (`--execution-workers` unset).
    const AUTO_WORKER_CAP: NonZeroUsize = NonZeroUsize::new(8).unwrap();
    /// Worker count when the host does not report its parallelism.
    const AUTO_WORKER_FALLBACK: NonZeroUsize = NonZeroUsize::new(4).unwrap();
    /// Hard cap: the mdbx reader-slot budget (`MAX_READERS = 64`) reserves
    /// the rest for RPC and compaction.
    const WORKER_CAP: NonZeroUsize = NonZeroUsize::new(40).unwrap();
    if !args.parallel_execution {
        return None;
    }
    // Unset means auto. The hard cap is 40, from the mdbx reader-slot
    // budget (geometry::MAX_READERS = 64, shared with exec, RPC, and
    // compaction).
    let workers = match args.execution_workers {
        None => std::thread::available_parallelism()
            .map_or(AUTO_WORKER_FALLBACK, |n| n.min(AUTO_WORKER_CAP)),
        Some(n) => n.min(WORKER_CAP),
    };
    tracing::info!(
        workers = workers.get(),
        "parallel execution ENABLED (Block-STM, block-at-a-time)"
    );
    Some(StmBlockExec::spawn(
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

impl ExecPorts for ExecutorWiring {
    type Snapshots = MdbxSnapshotSource;
    type WriterSignal = MdbxWriterSignal;
    type WriterQueue = MdbxWriterQueue;
    // No epoch verification: the executor trusts the ordered stream.
    type Epoch = NoEpochCheck;
    type RemoteEpoch = NoRemoteEpochCheck;
    type BlockExec = StmBlockExec<StateSnapshot>;
}

impl EngineWiring for ExecutorWiring {
    type TxData = bin_support::LiveTxDataSub;
    type TxOrdering = bin_support::LiveTxOrderingSub;
    type TxReceipts = LiveTxReceiptsPub;
}

pub(crate) struct LiveTxReceiptsPub {
    handle: kardamom_log::aeron_live::TxReceiptsPublisherHandle,
}

impl TxReceiptsPublication for LiveTxReceiptsPub {
    /// One `Vec<Receipt>` wire frame per batch: one encode and one blocking
    /// ack round trip through the Aeron thread, instead of one per receipt.
    /// Each frame is all-or-nothing. A transient failure reports 0
    /// published, and the commit thread's must-deliver loop retries the
    /// whole batch. The duplicates are harmless: `tx_receipts` delivers at
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
