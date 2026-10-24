//! Live Aeron adapters behind the proxy's channel traits.
//!
//! [`LiveIngressPublication`] fans the proxy's validated `TxEnvelope`s out
//! over M per-shard `tx_data` publisher handles. [`LiveIngressSubscription`]
//! pumps each `kardamom_log::aeron_live` subscriber handle into a
//! `tokio::sync::broadcast` sender, so the proxy's `broadcast::Receiver`
//! trait surface can fan out to multiple watchers. This is a lib module,
//! so the pump plumbing is unit-testable without a media driver.

use std::future::Future;
use std::num::{NonZeroU8, NonZeroU32};
use std::sync::Arc;

use tokio::sync::{broadcast, watch};

use kardamom_cache::{LiveAccounts, LiveAccountsConfig, LiveAccountsWriter};
use kardamom_log::aeron_live::{
    AeronRuntime, FsyncWatermarkSubscriberHandle, ReceiptFrame, ServiceEventsSubscriberHandle,
    TxDataPublisherHandle, TxErrorsSubscriberHandle, TxReceiptsBoundarySubscriberHandle,
    TxReceiptsReceiver, TxStatusSubscriberHandle,
};
use kardamom_log::discovery::StreamPlane;
use kardamom_obs::events::BoardView;
use kardamom_obs::pipeline::{Batch, LoopMeter, LoopState};
use kardamom_types::{
    BPosition, BlockBoundary, ClusterStatus, FsyncWatermark, QuorumWatermark, Receipt,
    ReceiptBatch, ReceiptRollback, TxEnvelope, TxError, TxStatus,
};

use crate::channels::{BUS_CAPACITY, IngressPublication, IngressSubscription, ReceiptItem};
use crate::error::IngressError;
use crate::rollback_log::RollbackLog;
use crate::stage_latency::{StageClock, StageStamps};

// ---------------------------------------------------------------------------
// IngressPublication adapter over M TxDataPublisherHandle.
// ---------------------------------------------------------------------------

/// M per-shard `tx_data` publisher handles behind the proxy's
/// [`IngressPublication`] trait, and the stage stamps of the published
/// transactions.
#[derive(Clone)]
pub struct LiveIngressPublication {
    tx_data: Vec<TxDataPublisherHandle>,
    stamps: StageStamps,
}

impl LiveIngressPublication {
    /// Open one `tx_data` publisher per lane. `lanes` is the lane plane
    /// size, not the active shard count.
    ///
    /// # Errors
    ///
    /// Returns `IngressError::Internal` if any lane's `tx_data` handle
    /// fails to open.
    pub async fn open(
        rt: &AeronRuntime,
        plane: &mut StreamPlane,
        lanes: NonZeroU8,
        stamps: StageStamps,
    ) -> Result<Self, IngressError> {
        let mut tx_data = Vec::with_capacity(usize::from(lanes.get()));
        for lane in 0..lanes.get() {
            let handle = plane
                .tx_data_publisher(rt, lane)
                .await
                .map_err(|e| IngressError::internal(format!("open tx_data[{lane}]"), e))?;
            tx_data.push(handle);
        }
        Ok(Self { tx_data, stamps })
    }
}

impl IngressPublication for LiveIngressPublication {
    async fn publish_tx_data(
        &self,
        shard: usize,
        envelope: TxEnvelope,
    ) -> Result<(), IngressError> {
        let pub_handle = self
            .tx_data
            .get(shard)
            .ok_or_else(|| IngressError::Internal(format!("shard {shard} out of range")))?;
        // The stamp comes before the publish, so the egress tap and the
        // receipt pump always have it when the seal and the receipt arrive.
        self.stamps.stamp(envelope.tx_hash);
        // The task awaits the ack of the Aeron thread, so no thread blocks
        // and the publish costs no hop to the blocking pool.
        pub_handle
            .publish_async(&envelope)
            .await
            .map(|_| ())
            .map_err(|e| IngressError::internal("publish_tx_data", e))
    }
}

// ---------------------------------------------------------------------------
// IngressSubscription adapter. Pumps each log handle's receiver into a
// tokio::sync::broadcast::Sender so the proxy's broadcast::Receiver-based
// trait surface can fan out to multiple watchers.
// ---------------------------------------------------------------------------

/// A pull source that the generic broadcast pump can drain: one
/// `(position, item)` stream, with the position dropped at this layer.
/// The four subscription streams, receipts, local-fsync watermark, block
/// boundaries, and `tx_errors`, differ only in how one item is pulled, so
/// they share a single pump.
trait PumpSource: Send + 'static {
    type Item: Clone + Send + 'static;
    fn next_item(&mut self) -> impl Future<Output = Option<Self::Item>> + Send;
}

/// One broadcast fan-out pump: the pull source and the bus it drains
/// into.
struct Pump<S: PumpSource> {
    source: S,
    tx: broadcast::Sender<S::Item>,
}

impl<S: PumpSource> Pump<S> {
    fn new(source: S, tx: broadcast::Sender<S::Item>) -> Self {
        Self { source, tx }
    }

    /// Start the pump on the runtime. It ends when the source closes, on
    /// `AeronRuntime` shutdown.
    fn spawn(self) {
        tokio::spawn(self.run());
    }

    /// Drain the source into the bus. A lagging or absent receiver is the
    /// broadcast channel's concern, so this ignores the send result.
    async fn run(mut self) {
        while let Some(item) = self.source.next_item().await {
            let _ = self.tx.send(item);
        }
    }
}

/// Generic `PumpSource` over a raw tokio `UnboundedReceiver<(BPosition,
/// T)>`, dropping the position. This crate's own test (below) builds one
/// directly, to test the pump plumbing without a media driver.
impl<T: Clone + Send + 'static> PumpSource
    for tokio::sync::mpsc::UnboundedReceiver<(BPosition, T)>
{
    type Item = T;
    async fn next_item(&mut self) -> Option<T> {
        self.recv().await.map(|(_pos, item)| item)
    }
}

/// Implements [`PumpSource`] for a subscriber handle whose `recv` returns
/// `(BPosition, Item)`, dropping the position. The four concrete
/// subscriber handles below share exactly this shape. The receipts
/// stream does not: its frame is a batch with account rows, so
/// [`ReceiptBatchPump`] drains it.
macro_rules! impl_pump_source {
    ($handle:ty, $item:ty) => {
        impl PumpSource for $handle {
            type Item = $item;
            async fn next_item(&mut self) -> Option<$item> {
                self.recv().await.map(|(_pos, item)| item)
            }
        }
    };
}

impl_pump_source!(FsyncWatermarkSubscriberHandle, FsyncWatermark);
impl_pump_source!(TxReceiptsBoundarySubscriberHandle, BlockBoundary);
impl_pump_source!(TxErrorsSubscriberHandle, TxError);
impl_pump_source!(TxStatusSubscriberHandle, TxStatus);

/// The `tx_receipts` pump: one batch frame in, its account rows into the
/// local layer, then its receipts onto the bus, one by one, each timed by
/// the executed clock. The rows go first, so a client its receipt
/// releases sees the new state on its next submit. A rollback frame goes onto the same bus, so the order of
/// the stream holds. The pump then puts each rollback into the rollback
/// log, which a reader that lags on the bus reads. `TxReceiptsReceiver` has
/// no constructor outside the log crate, so [`Self::on_frame`] is the
/// testable step.
struct ReceiptBatchPump {
    writer: LiveAccountsWriter,
    tx: broadcast::Sender<ReceiptItem>,
    log: watch::Sender<RollbackLog>,
    executed: StageClock,
    /// Idle while the pump waits for a frame, busy while it handles one.
    meter: LoopMeter,
    /// The receipts in each batch frame that carries a receipt.
    receipts: Batch,
}

impl ReceiptBatchPump {
    fn new(
        writer: LiveAccountsWriter,
        tx: broadcast::Sender<ReceiptItem>,
        log: watch::Sender<RollbackLog>,
        executed: StageClock,
    ) -> Self {
        Self {
            writer,
            tx,
            log,
            executed,
            meter: LoopMeter::new("ingress.receipt_pump"),
            receipts: Batch::new("ingress.receipt_batch"),
        }
    }

    /// Start the pump on the runtime. It ends when the source closes, on
    /// `AeronRuntime` shutdown.
    fn spawn(self, rx: TxReceiptsReceiver) {
        tokio::spawn(self.run(rx));
    }

    async fn run(mut self, mut rx: TxReceiptsReceiver) {
        while let Some((_, frame)) = rx.recv_frame().await {
            self.on_received(frame);
        }
    }

    /// Handle one frame that the wait received, and meter the wait and
    /// the handling.
    fn on_received(&mut self, frame: ReceiptFrame) {
        self.meter.mark(LoopState::Idle);
        self.on_frame(frame);
        self.meter.mark(LoopState::Busy);
    }

    /// Put one frame onto the bus. A lagging or absent receiver is the
    /// broadcast channel's concern, so this ignores the send result.
    fn on_frame(&mut self, frame: ReceiptFrame) {
        match frame {
            ReceiptFrame::Batch(batch) => self.on_batch(batch),
            ReceiptFrame::Rollback(rollback) => self.on_rollback(rollback),
        }
    }

    /// Take one rollback. A rollback of an event that the log holds already
    /// is a late copy from another executor: it changes nothing, so the
    /// rows of the new stream stay. A new one empties the local account
    /// layer first: the rows of the lost blocks carry positions above the
    /// new stream, so the monotone rule would keep their nonces.
    ///
    /// The bus takes the rollback before the log does. So a reader that
    /// lags finds each rollback of the log in the queue of the bus, or the
    /// bus dropped it. It never finds a rollback that the bus has yet to
    /// carry.
    fn on_rollback(&mut self, rollback: ReceiptRollback) {
        if self.log.borrow().contains(rollback.id) {
            return;
        }
        self.writer.clear();
        let _ = self.tx.send(ReceiptItem::Rollback(rollback));
        self.log.send_modify(|log| {
            log.push(rollback);
        });
    }

    /// Apply the batch's rows at its end position, then fan its receipts
    /// out. A batch with no receipt (a boundary-only frame) carries no
    /// rows either. A lagging or absent receiver is the broadcast
    /// channel's concern, so this ignores the send result.
    fn on_batch(&mut self, batch: ReceiptBatch) {
        if let Some(end) = batch.end_tx_idx() {
            self.receipts.record(batch.receipts.len());
            self.writer.apply(end, &batch.accounts);
        }
        batch
            .receipts
            .into_iter()
            .for_each(|receipt| self.fan_out(receipt));
    }

    /// Time one receipt, then put it onto the bus.
    fn fan_out(&mut self, receipt: Receipt) {
        self.executed.observe(receipt.tx_hash);
        let _ = self.tx.send(receipt.into());
    }
}

/// Live [`IngressSubscription`]. Per-stream pump tasks feed these
/// broadcast buses.
#[derive(Clone)]
pub struct LiveIngressSubscription {
    receipts: broadcast::Sender<ReceiptItem>,
    rollbacks: watch::Receiver<RollbackLog>,
    live: Arc<LiveAccounts>,
    watermarks: broadcast::Sender<QuorumWatermark>,
    local_fsync: broadcast::Sender<FsyncWatermark>,
    block_boundaries: broadcast::Sender<BlockBoundary>,
    tx_errors: broadcast::Sender<TxError>,
    verdicts: broadcast::Sender<TxStatus>,
    /// The latest cluster status, fed by the binary's egress observer
    /// like the watermark bus.
    cluster_status: watch::Sender<ClusterStatus>,
    /// The board of every service's latest state, fed by the `events`
    /// subscription.
    service_board: watch::Receiver<BoardView>,
}

impl LiveIngressSubscription {
    /// Open the five subscriber handles through `plane`: `tx_errors`,
    /// receipts, boundaries, and `events` follow the plane's transport;
    /// the fsync watermark still opens on its static channel. With
    /// `verdicts`, the ingress reads no receipts: it reads the
    /// `tx_verdicts` stream of that ingress id, the answer of the
    /// sequencer to each of its transactions. The receipt pump owns
    /// `executed`, the clock of the executed stage.
    ///
    /// # Errors
    ///
    /// Returns `IngressError::Internal` if any of the five subscriber
    /// handles fails to open.
    pub fn open(
        rt: &AeronRuntime,
        plane: &mut StreamPlane,
        recorder_id: u8,
        executor_count: Option<NonZeroU32>,
        live_cfg: &LiveAccountsConfig,
        verdicts: Option<u8>,
        executed: StageClock,
    ) -> Result<Self, IngressError> {
        let channels = plane.channels().clone();
        let channels = &channels;
        let (receipts_tx, _) = broadcast::channel::<ReceiptItem>(BUS_CAPACITY);
        let (live, live_writer) = LiveAccounts::new(live_cfg);
        let (watermarks_tx, _) = broadcast::channel::<QuorumWatermark>(BUS_CAPACITY);
        let (local_fsync_tx, _) = broadcast::channel::<FsyncWatermark>(BUS_CAPACITY);
        let (block_boundaries_tx, _) = broadcast::channel::<BlockBoundary>(BUS_CAPACITY);
        let (tx_errors_tx, _) = broadcast::channel::<TxError>(BUS_CAPACITY);
        let (verdicts_tx, _) = broadcast::channel::<TxStatus>(BUS_CAPACITY);
        let (cluster_status_tx, _) = watch::channel(ClusterStatus::default());

        let mds = channels.tx_receipts_mds_enabled();
        if mds {
            tracing::info!(
                ?executor_count,
                control_channel = %channels.tx_receipts_control_channel,
                "tx_receipts MDS fan-in: aggregating per-replica executor endpoints"
            );
        }

        // This is the tx_receipts to Receipt fan-out. N executors replay
        // the same canonical order and emit identical receipts, so the
        // proxy dedups by tx hash downstream, first-wins. This layer only
        // aggregates the streams: every executor publisher the plane
        // discovers, or the static channel's members. The batch frame's
        // account rows feed the local layer on the way through.
        //
        // With `verdicts`, the ingress reads no receipts: the receipts go
        // to the egress, and the verdicts of its own transactions come on
        // its `tx_verdicts` stream.
        let (rollback_log, rollbacks) = watch::channel(RollbackLog::default());
        match verdicts {
            None => {
                let receipts_sub = plane
                    .tx_receipts_subscriber(rt, executor_count)
                    .map_err(|e| IngressError::internal("open tx_receipts", e))?;
                // `into_receiver()`: the pump task must not hold an
                // `AeronRuntime` clone. Holding one would keep the runtime
                // alive forever; see `TxReceiptsSubscriberHandle::into_receiver`.
                ReceiptBatchPump::new(live_writer, receipts_tx.clone(), rollback_log, executed)
                    .spawn(receipts_sub.into_receiver());
            }
            Some(own) => {
                tracing::info!(
                    ingress_id = own,
                    "the ingress reads its verdicts, no receipts"
                );
                let verdicts_sub = plane
                    .tx_verdicts_subscriber(rt, own)
                    .map_err(|e| IngressError::internal("open tx_verdicts", e))?;
                Pump::new(verdicts_sub, verdicts_tx.clone()).spawn();
            }
        }

        // Quorum and durable watermark: in the cluster-only topology,
        // there is no Aeron `quorum_watermark` subscription here. The
        // binary's Aeron Cluster egress observer feeds this bus instead;
        // see the note on [`Self::watermark_sender`].

        // This is the per-recorder fsync watermark.
        let fsync_sub = FsyncWatermarkSubscriberHandle::open(rt, channels, recorder_id)
            .map_err(|e| IngressError::internal("open fsync watermark", e))?;
        Pump::new(fsync_sub, local_fsync_tx.clone()).spawn();

        // This is the tx_receipts to BlockBoundary fan-out, the
        // `tx_receipts_stream_id + 1` side stream, over the same
        // publisher set as the receipt stream above.
        let boundary_sub = plane
            .tx_receipt_boundaries_subscriber(rt, executor_count)
            .map_err(|e| IngressError::internal("open tx_receipts boundaries", e))?;
        Pump::new(boundary_sub, block_boundaries_tx.clone()).spawn();

        // This is the tx_errors to TxError fan-out.
        let errors_sub = plane
            .subscriber::<TxErrorsSubscriberHandle>(rt)
            .map_err(|e| IngressError::internal("open tx_errors", e))?;
        Pump::new(errors_sub, tx_errors_tx.clone()).spawn();

        // This is the events stream to the board of service states.
        let service_board = plane
            .subscriber::<ServiceEventsSubscriberHandle>(rt)
            .map_err(|e| IngressError::internal("open events", e))?
            .spawn_board();

        Ok(Self {
            receipts: receipts_tx,
            rollbacks,
            live,
            watermarks: watermarks_tx,
            local_fsync: local_fsync_tx,
            block_boundaries: block_boundaries_tx,
            tx_errors: tx_errors_tx,
            verdicts: verdicts_tx,
            cluster_status: cluster_status_tx,
            service_board,
        })
    }

    /// Producer side of the cluster status, fed by the same egress
    /// observer as [`Self::watermark_sender`].
    #[must_use]
    pub fn cluster_status_sender(&self) -> watch::Sender<ClusterStatus> {
        self.cluster_status.clone()
    }

    /// Producer side of the quorum and durable watermark bus. In the
    /// cluster-only topology, the binary's Aeron Cluster egress observer
    /// feeds this bus, since there is no Aeron `quorum_watermark`
    /// subscription here; see the note in [`Self::open`]. The observer
    /// thread sends into this handle.
    #[must_use]
    pub fn watermark_sender(&self) -> broadcast::Sender<QuorumWatermark> {
        self.watermarks.clone()
    }
}

impl IngressSubscription for LiveIngressSubscription {
    fn subscribe_receipts(&self) -> broadcast::Receiver<ReceiptItem> {
        self.receipts.subscribe()
    }
    fn rollbacks(&self) -> watch::Receiver<RollbackLog> {
        self.rollbacks.clone()
    }
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark> {
        self.watermarks.subscribe()
    }
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark> {
        self.local_fsync.subscribe()
    }
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary> {
        self.block_boundaries.subscribe()
    }
    fn subscribe_tx_errors(&self) -> broadcast::Receiver<TxError> {
        self.tx_errors.subscribe()
    }
    fn subscribe_verdicts(&self) -> broadcast::Receiver<TxStatus> {
        self.verdicts.subscribe()
    }
    fn live_accounts(&self) -> Arc<LiveAccounts> {
        self.live.clone()
    }
    fn cluster_status(&self) -> watch::Receiver<ClusterStatus> {
        self.cluster_status.subscribe()
    }
    fn service_board(&self) -> watch::Receiver<BoardView> {
        self.service_board.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The generic pump delivers items, with the position stripped, to
    // every broadcast subscriber, and ends when the source closes. All
    // four stream pumps share this plumbing.
    #[tokio::test]
    async fn pump_fans_out_and_ends_on_close() {
        // `src_tx` and `bus` (the outer sender handle) live only in this
        // scope. Both go out of scope, and drop, at its end. The pump's own
        // clone of `bus` drops in turn once it drains the two queued items
        // and sees the source close, which is what ends the pump and lets
        // `sub_a`/`sub_b` observe `Closed` below.
        let (mut sub_a, mut sub_b) = {
            let (src_tx, src_rx) = tokio::sync::mpsc::unbounded_channel::<(BPosition, u64)>();
            let (bus, _) = broadcast::channel::<u64>(16);
            let sub_a = bus.subscribe();
            let sub_b = bus.subscribe();
            Pump::new(src_rx, bus.clone()).spawn();

            let pos = BPosition {
                term_id: 0,
                term_offset: 0,
            };
            src_tx.send((pos, 7)).unwrap();
            src_tx.send((pos, 8)).unwrap();
            (sub_a, sub_b)
        };
        assert_eq!(sub_a.recv().await.unwrap(), 7);
        assert_eq!(sub_a.recv().await.unwrap(), 8);
        assert_eq!(sub_b.recv().await.unwrap(), 7);
        assert_eq!(sub_b.recv().await.unwrap(), 8);
        assert!(matches!(
            sub_a.recv().await,
            Err(broadcast::error::RecvError::Closed)
        ));
    }

    // The receipts pump applies a batch's rows before it fans the
    // receipts out: a subscriber that wakes on the receipt reads the new
    // state. A boundary-only batch changes nothing.
    #[tokio::test]
    async fn receipt_batch_pump_applies_rows_before_receipts() {
        use alloy_primitives::{Address, U256};
        use kardamom_types::AccountRow;

        let (live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let (bus, mut sub) = broadcast::channel::<ReceiptItem>(16);
        let mut pump = ReceiptBatchPump::new(
            writer,
            bus,
            watch::channel(RollbackLog::default()).0,
            StageClock::pair().1.executed,
        );
        let end = BPosition::from_index(7);
        let receipt = kardamom_types::Receipt {
            tx_idx: end,
            ..kardamom_types::Receipt::default()
        };
        let row = AccountRow {
            address: Address::repeat_byte(0xaa),
            nonce: 3,
            balance: U256::from(10u64),
        };
        pump.on_batch(ReceiptBatch {
            receipts: vec![receipt],
            accounts: vec![row],
        });
        let Ok(ReceiptItem::Receipt(got)) = sub.recv().await else {
            panic!("the receipt comes first");
        };
        assert_eq!(got.tx_idx, end);
        let view = live.get(Address::repeat_byte(0xaa)).unwrap();
        assert_eq!((view.nonce, view.tx_idx), (3, 7));
        pump.on_batch(ReceiptBatch {
            receipts: Vec::new(),
            accounts: Vec::new(),
        });
        assert_eq!(live.head(), 7);
    }

    // A rollback frame goes onto the receipt bus between the receipts
    // around it, in stream order.
    #[tokio::test]
    async fn receipt_pump_keeps_a_rollback_in_stream_order() {
        let (live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
        let (bus, mut sub) = broadcast::channel::<ReceiptItem>(16);
        let (log, mut log_rx) = watch::channel(RollbackLog::default());
        let mut pump = ReceiptBatchPump::new(writer, bus, log, StageClock::pair().1.executed);
        let receipt = |block_number| kardamom_types::Receipt {
            block_number,
            ..kardamom_types::Receipt::default()
        };
        let rollback = kardamom_types::ReceiptRollback {
            to_block: 4,
            ..Default::default()
        };
        let row = kardamom_types::AccountRow {
            address: alloy_primitives::Address::repeat_byte(0xAA),
            nonce: 9,
            balance: alloy_primitives::U256::from(1u64),
        };
        let batch = |block_number, accounts| {
            ReceiptFrame::Batch(ReceiptBatch {
                receipts: vec![receipt(block_number)],
                accounts,
            })
        };
        pump.on_frame(batch(6, vec![row.clone()]));
        assert!(
            live.get(row.address).is_some(),
            "the row of block 6 is resident"
        );
        pump.on_frame(ReceiptFrame::Rollback(rollback));
        assert!(
            live.get(row.address).is_none(),
            "the rollback empties the layer"
        );
        pump.on_frame(batch(5, vec![row.clone()]));
        assert_eq!(sub.recv().await.unwrap(), receipt(6).into());
        assert_eq!(sub.recv().await.unwrap(), ReceiptItem::Rollback(rollback));
        assert_eq!(sub.recv().await.unwrap(), receipt(5).into());
        assert!(log_rx.has_changed().unwrap(), "the log takes the rollback");
        assert_eq!(log_rx.borrow_and_update().generation(), 1);

        // A late copy of the same event from another executor keeps the
        // rows of the new stream, and goes neither on the bus nor in the log.
        pump.on_frame(ReceiptFrame::Rollback(rollback));
        assert!(live.get(row.address).is_some(), "the new rows stay");
        assert!(!log_rx.has_changed().unwrap());
        assert!(matches!(
            sub.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    // The receipt batch histogram takes one sample for each batch with a
    // receipt. A boundary-only batch adds none.
    #[tokio::test]
    async fn receipt_pump_records_the_receipts_of_each_batch() {
        let text = crate::rendered_metrics_tests::Rendered::of(|| {
            let (_live, writer) = LiveAccounts::new(&LiveAccountsConfig::default());
            let (bus, _sub) = broadcast::channel::<ReceiptItem>(16);
            let log = watch::channel(RollbackLog::default()).0;
            let mut pump = ReceiptBatchPump::new(writer, bus, log, StageClock::pair().1.executed);
            pump.on_batch(ReceiptBatch {
                receipts: vec![Receipt::default(), Receipt::default()],
                accounts: Vec::new(),
            });
            pump.on_batch(ReceiptBatch::default());
        });
        assert_eq!(
            text.value(r#"kardamom_batch_items_sum{batch="ingress.receipt_batch"}"#),
            2.0
        );
        assert_eq!(
            text.value(r#"kardamom_batch_items_count{batch="ingress.receipt_batch"}"#),
            1.0
        );
    }

    // A shard index past the publication vector must give an Internal
    // error, not a panic. The proxy computes shards mod M, but the
    // adapter must not rely on that.
    #[tokio::test]
    async fn publication_rejects_out_of_range_shard() {
        let publication = LiveIngressPublication {
            tx_data: Vec::new(),
            stamps: StageClock::pair().0,
        };
        let err = publication
            .publish_tx_data(0, TxEnvelope::default())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("shard 0 out of range"),
            "unexpected error: {err}"
        );
    }
}
