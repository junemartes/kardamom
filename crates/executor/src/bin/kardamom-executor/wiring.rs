//! Role-specific adapters and construction: the executor's `EngineWiring`,
//! the live `tx_receipts` publication, and the opt-in Block-STM strategy.

use std::num::NonZeroUsize;

use anyhow::{Context, Result};
use kardamom_cluster_adapter::{LiveIngress, OfferOutcome};
use kardamom_engine::bin_support;
use kardamom_engine::reader::Ordered;
use kardamom_engine::reader::cluster::RecordedCursorPublisher;
use kardamom_engine::{BPosition, TxOrderingSubscription};
use kardamom_engine::{
    CMessage, EngineWiring, ExecPorts, ExecutorError, MdbxSnapshotSource, MdbxWriterQueue,
    MdbxWriterSignal, NoEpochCheck, NoRemoteEpochCheck, NoTxHook, TxReceiptsPublication,
};
use kardamom_executor::parallel::StmBlockExec;
use kardamom_log::aeron_live::{AeronRuntime, TxReceiptsPublisherHandle};
use kardamom_log::discovery::{StreamPlane, TxReceiptsReopen};
use kardamom_state::StateSnapshot;
use kardamom_types::VoidRecord;

use crate::args::Args;

/// `tx_receipts` publication: the receipt stream and the boundary
/// side-stream, opened through the stream plane. With discovery, this
/// replica publishes two dynamic MDC publications. Without it, the
/// plane picks the per-replica MDS endpoint (chosen by `--recorder-id`)
/// or the shared channel from the static config. Either way, the commit
/// thread's must-deliver retry drives the same `publish_receipt` and
/// `publish_boundary` surface, and the same reopen.
pub(crate) async fn open_tx_receipts_pub(
    rt_pub: &AeronRuntime,
    plane: &mut StreamPlane,
    args: &Args,
) -> Result<LiveTxReceiptsPub> {
    let handle = plane
        .tx_receipts_publisher(rt_pub, args.recorder_id)
        .await
        .context("open TxReceiptsPublisherHandle")?;
    // A validator names a divergent replica by this session id.
    tracing::info!(
        session = handle.session_id(),
        "tx_receipts publication open"
    );
    Ok(LiveTxReceiptsPub {
        handle,
        reopen: plane
            .tx_receipts_reopen(args.recorder_id)
            .context("the tx_receipts reopen")?,
        rt_pub: rt_pub.clone(),
        tokio: tokio::runtime::Handle::current(),
    })
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
    // No tx hook. See the `RoleHooks` construction in `main.rs`.
    type TxHook = NoTxHook;
}

impl EngineWiring for ExecutorWiring {
    type TxData = bin_support::LiveTxDataSub;
    type TxOrdering = ExecutorOrdering;
    type TxReceipts = LiveTxReceiptsPub;
    type ExecStream = crossbeam_channel::Sender<kardamom_engine::ExecStreamItem>;
}

/// The canonical stream of the executor.
pub(crate) enum ExecutorOrdering {
    /// The references of the cluster egress. The reader joins each one
    /// with its envelope from `tx_data`.
    Refs(bin_support::LiveTxOrderingSub),
    /// The entries with their bytes, from the member feed. `ingress` is the
    /// publisher-only cluster session that carries the recorded cursor.
    /// The subscription is boxed: it is far larger than the reference
    /// subscription.
    Carried {
        sub: Box<bin_support::LiveCarriedSub>,
        ingress: LiveIngress,
    },
}

impl ExecutorOrdering {
    /// A publisher of the recorded cursor of executor `executor_id` over
    /// the cluster session.
    pub(crate) fn recorded_cursor_publisher(
        &self,
        executor_id: u8,
    ) -> RecordedCursorPublisher<LiveIngress> {
        match self {
            Self::Refs(sub) => sub.recorded_cursor_publisher(executor_id),
            Self::Carried { ingress, .. } => {
                RecordedCursorPublisher::new(ingress.clone(), executor_id)
            }
        }
    }
}

impl TxOrderingSubscription for ExecutorOrdering {
    fn next(&mut self) -> Result<(BPosition, Ordered), ExecutorError> {
        match self {
            Self::Refs(sub) => sub.next(),
            Self::Carried { sub, .. } => sub.next(),
        }
    }

    /// Only the reference stream has entries to void. The carried stream
    /// has no join, so it never votes.
    fn vote(&mut self, voter_id: u8, void: &VoidRecord) -> OfferOutcome {
        match self {
            Self::Refs(sub) => sub.vote(voter_id, void),
            Self::Carried { .. } => OfferOutcome::NotConnected,
        }
    }
}

/// The live `tx_receipts` publication of this replica, with what a reopen
/// needs: the plane's reopen recipe, the publication runtime, and the
/// Tokio handle the catalog call runs on. The commit thread, a plain OS
/// thread, blocks on that handle for the one call of a reopen.
pub(crate) struct LiveTxReceiptsPub {
    handle: TxReceiptsPublisherHandle,
    reopen: TxReceiptsReopen,
    rt_pub: AeronRuntime,
    tokio: tokio::runtime::Handle,
}

impl LiveTxReceiptsPub {
    /// The topic of the receipt stream, for the typed error.
    const TOPIC: &'static str = "tx_receipts";

    /// The engine error of one failed publish. A `NOT_CONNECTED` offer
    /// gets the typed error the escalation clock runs on; every other
    /// transport error stays a transient state error.
    fn publish_error(op: &str, e: &kardamom_log::error::LogError) -> ExecutorError {
        let detail = format!("{op}: {e}");
        if detail.contains("NOT_CONNECTED") {
            return ExecutorError::NotConnected {
                topic: Self::TOPIC.into(),
                detail,
            };
        }
        ExecutorError::State(detail)
    }
}

impl TxReceiptsPublication for LiveTxReceiptsPub {
    fn subscribers_listed(&mut self) -> bool {
        self.reopen.subscribers_listed()
    }

    /// Open the receipt stream again on a new session, move the discovery
    /// record to it, and close the old publication. A failed close only
    /// leaves the old session to the driver's own timeout.
    fn reopen(&mut self) -> Result<(), ExecutorError> {
        let opened = self
            .tokio
            .block_on(self.reopen.reopen(&self.rt_pub))
            .map_err(|e| ExecutorError::State(format!("reopen tx_receipts: {e}")))?;
        let old = self.handle.replace_receipts(opened);
        tracing::warn!(
            old_session = old.session_id(),
            session = self.handle.session_id(),
            "tx_receipts publication reopened on a new session"
        );
        if let Err(e) = old.close() {
            tracing::warn!(error = %e, session = old.session_id(), "the old tx_receipts publication did not close");
        }
        Ok(())
    }

    /// One `ReceiptBatch` wire frame per batch: one encode and one blocking
    /// ack round trip through the Aeron thread, instead of one per receipt.
    /// The per-tx account rows merge into the frame here, at the wire
    /// edge, so a row is the account's state after the batch's last
    /// receipt. Each frame is all-or-nothing. A transient failure reports
    /// 0 published, and the commit thread's must-deliver loop retries the
    /// whole batch, rows included. The duplicates are harmless:
    /// `tx_receipts` delivers at least once, and consumers dedupe on
    /// `tx_idx`.
    fn publish_receipts(
        &mut self,
        items: &[kardamom_types::ReceiptRows],
    ) -> (usize, Option<ExecutorError>) {
        match self
            .handle
            .publish_receipts(&kardamom_types::ReceiptBatch::merge(items))
        {
            Ok(_) => (items.len(), None),
            Err(e) => (0, Some(Self::publish_error("publish_receipts", &e))),
        }
    }

    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        match msg {
            CMessage::Receipt(r) => self
                .handle
                .publish_receipt(&r)
                .map(|_| ())
                .map_err(|e| Self::publish_error("publish_receipt", &e)),
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
