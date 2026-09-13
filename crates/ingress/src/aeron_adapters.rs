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

use tokio::sync::broadcast;

use kardamom_log::aeron_live::{
    AeronRuntime, FsyncWatermarkSubscriberHandle, TxDataPublisherHandle, TxErrorsSubscriberHandle,
    TxReceiptsBoundarySubscriberHandle, TxReceiptsReceiver, TxReceiptsSubscriberHandle,
};
use kardamom_log::config::ChannelsConfig;
use kardamom_types::{
    BPosition, BlockBoundary, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope, TxError,
};

use crate::channels::{BUS_CAPACITY, IngressPublication, IngressSubscription};
use crate::error::IngressError;

// ---------------------------------------------------------------------------
// IngressPublication adapter over M TxDataPublisherHandle.
// ---------------------------------------------------------------------------

/// M per-shard `tx_data` publisher handles behind the proxy's
/// [`IngressPublication`] trait.
#[derive(Clone)]
pub struct LiveIngressPublication {
    tx_data: Vec<TxDataPublisherHandle>,
}

impl LiveIngressPublication {
    /// Open one `tx_data` publisher per lane. `lanes` is the lane plane
    /// size, not the active shard count.
    ///
    /// # Errors
    ///
    /// Returns `IngressError::Internal` if any lane's `tx_data` handle
    /// fails to open.
    pub fn open(
        rt: &AeronRuntime,
        channels: &ChannelsConfig,
        lanes: NonZeroU8,
    ) -> Result<Self, IngressError> {
        let tx_data = (0..lanes.get())
            .map(|lane| {
                TxDataPublisherHandle::open(rt, channels, lane)
                    .map_err(|e| IngressError::internal(format!("open tx_data[{lane}]"), e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { tx_data })
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
            .ok_or_else(|| IngressError::Internal(format!("shard {shard} out of range")))?
            .clone();
        // The Aeron publish blocks through the runtime's command channel.
        // This runs it off the reactor thread, so it does not stall the
        // JSON-RPC server.
        tokio::task::spawn_blocking(move || pub_handle.publish(&envelope))
            .await
            .map_err(|e| IngressError::internal("publish_tx_data join", e))?
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
/// `(BPosition, Item)`, dropping the position. The three concrete
/// subscriber handles below, plus [`TxReceiptsReceiver`] (`into_receiver()`
/// handles; see the `tx_receipts` comment in
/// [`LiveIngressSubscription::open`]), share exactly this shape.
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
impl_pump_source!(TxReceiptsReceiver, Receipt);

/// Live [`IngressSubscription`]. Per-stream pump tasks feed these
/// broadcast buses.
#[derive(Clone)]
pub struct LiveIngressSubscription {
    receipts: broadcast::Sender<Receipt>,
    watermarks: broadcast::Sender<QuorumWatermark>,
    local_fsync: broadcast::Sender<FsyncWatermark>,
    block_boundaries: broadcast::Sender<BlockBoundary>,
    tx_errors: broadcast::Sender<TxError>,
}

impl LiveIngressSubscription {
    /// # Errors
    ///
    /// Returns `IngressError::Internal` if any of the four subscriber
    /// handles fails to open.
    pub fn open(
        rt: &AeronRuntime,
        channels: &ChannelsConfig,
        recorder_id: u8,
        executor_count: Option<NonZeroU32>,
    ) -> Result<Self, IngressError> {
        let (receipts_tx, _) = broadcast::channel::<Receipt>(BUS_CAPACITY);
        let (watermarks_tx, _) = broadcast::channel::<QuorumWatermark>(BUS_CAPACITY);
        let (local_fsync_tx, _) = broadcast::channel::<FsyncWatermark>(BUS_CAPACITY);
        let (block_boundaries_tx, _) = broadcast::channel::<BlockBoundary>(BUS_CAPACITY);
        let (tx_errors_tx, _) = broadcast::channel::<TxError>(BUS_CAPACITY);

        let mds = channels.tx_receipts_mds_enabled();
        if mds {
            tracing::info!(
                ?executor_count,
                control_channel = %channels.tx_receipts_control_channel,
                "tx_receipts MDS fan-in: aggregating per-replica executor endpoints"
            );
        }

        // This is the tx_receipts to Receipt fan-out.
        //
        // MDS, the fan-in mode: this opens one control-mode=manual
        // subscription on `tx_receipts_control_channel`, and attaches
        // each executor replica's unicast endpoint, `0..executor_count`,
        // as a destination. N executors replay the same canonical order
        // and emit identical receipts, so the proxy dedups by tx hash
        // downstream, first-wins. This layer only aggregates the streams.
        // Legacy IPC uses a plain subscription on the shared
        // `tx_receipts_channel` instead.
        let receipts_sub = TxReceiptsSubscriberHandle::open_auto(rt, channels, executor_count)
            .map_err(|e| IngressError::internal("open tx_receipts", e))?;
        // `into_receiver()`: the pump task must not hold an
        // `AeronRuntime` clone. Holding one would keep the runtime alive
        // forever; see `TxReceiptsSubscriberHandle::into_receiver`.
        Pump::new(receipts_sub.into_receiver(), receipts_tx.clone()).spawn();

        // Quorum and durable watermark: in the cluster-only topology,
        // there is no Aeron `quorum_watermark` subscription here. The
        // binary's Aeron Cluster egress observer feeds this bus instead;
        // see the note on [`Self::watermark_sender`].

        // This is the per-recorder fsync watermark.
        let fsync_sub = FsyncWatermarkSubscriberHandle::open(rt, channels, recorder_id)
            .map_err(|e| IngressError::internal("open fsync watermark", e))?;
        Pump::new(fsync_sub, local_fsync_tx.clone()).spawn();

        // This is the tx_receipts to BlockBoundary fan-out, the
        // `tx_receipts_stream_id + 1` side stream. It uses the same MDS
        // vs. IPC branch as the receipt stream above: it attaches the
        // same per-replica executor endpoints to the boundary MDS
        // subscription.
        let boundary_sub =
            TxReceiptsBoundarySubscriberHandle::open_auto(rt, channels, executor_count)
                .map_err(|e| IngressError::internal("open tx_receipts boundaries", e))?;
        Pump::new(boundary_sub, block_boundaries_tx.clone()).spawn();

        // This is the tx_errors to TxError fan-out.
        let errors_sub = TxErrorsSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::internal("open tx_errors", e))?;
        Pump::new(errors_sub, tx_errors_tx.clone()).spawn();

        Ok(Self {
            receipts: receipts_tx,
            watermarks: watermarks_tx,
            local_fsync: local_fsync_tx,
            block_boundaries: block_boundaries_tx,
            tx_errors: tx_errors_tx,
        })
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
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt> {
        self.receipts.subscribe()
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

    // A shard index past the publication vector must give an Internal
    // error, not a panic. The proxy computes shards mod M, but the
    // adapter must not rely on that.
    #[tokio::test]
    async fn publication_rejects_out_of_range_shard() {
        let publication = LiveIngressPublication {
            tx_data: Vec::new(),
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
