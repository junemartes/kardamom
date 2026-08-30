//! Live Aeron adapters behind the proxy's channel traits.
//!
//! [`LiveIngressPublication`] fans the proxy's validated `TxEnvelope`s out
//! over M per-shard `tx_data` publisher handles. [`LiveIngressSubscription`]
//! pumps each `kardamom_log::aeron_live` subscriber handle into a
//! `tokio::sync::broadcast` sender, so the proxy's `broadcast::Receiver`
//! trait surface can fan out to multiple watchers. Both used to live
//! inside the `kardamom-ingress` binary. They are a lib module now, so
//! the pump plumbing can be unit-tested without a media driver.

use std::future::Future;

use async_trait::async_trait;
use tokio::sync::broadcast;

use kardamom_log::aeron_live::{
    AeronRuntime, FsyncWatermarkSubscriberHandle, TxDataPublisherHandle, TxErrorsSubscriberHandle,
    TxReceiptsBoundarySubscriberHandle, TxReceiptsSubscriberHandle,
};
use kardamom_log::config::ChannelsConfig;
use kardamom_types::{
    BPosition, BlockBoundary, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope, TxError,
};

use crate::channels::{IngressPublication, IngressSubscription};
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
    pub fn open(
        rt: &AeronRuntime,
        channels: &ChannelsConfig,
        shards: u8,
    ) -> Result<Self, IngressError> {
        let mut tx_data = Vec::with_capacity(shards as usize);
        for sid in 0..shards {
            let h = TxDataPublisherHandle::open(rt, channels, sid)
                .map_err(|e| IngressError::Internal(format!("open tx_data[{sid}]: {e}")))?;
            tx_data.push(h);
        }
        Ok(Self { tx_data })
    }
}

#[async_trait]
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
            .map_err(|e| IngressError::Internal(format!("publish_tx_data join: {e}")))?
            .map(|_| ())
            .map_err(|e| IngressError::Internal(format!("publish_tx_data: {e}")))
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
/// boundaries, and tx_errors, differ only in how one item is pulled, so
/// they share a single pump.
trait PumpSource: Send + 'static {
    type Item: Clone + Send + 'static;
    fn next_item(&mut self) -> impl Future<Output = Option<Self::Item>> + Send;
}

/// Spawns one broadcast fan-out pump. Drains `source` into `tx` until the
/// source closes, on AeronRuntime shutdown. A lagging or absent receiver
/// is the broadcast channel's concern, so this ignores the send result.
fn spawn_pump<S: PumpSource>(mut source: S, tx: broadcast::Sender<S::Item>) {
    tokio::spawn(async move {
        while let Some(item) = source.next_item().await {
            let _ = tx.send(item);
        }
    });
}

/// Detached-receiver source, for `into_receiver()` handles. See the
/// tx_receipts comment in [`LiveIngressSubscription::open`].
impl<T: Clone + Send + 'static> PumpSource
    for tokio::sync::mpsc::UnboundedReceiver<(BPosition, T)>
{
    type Item = T;
    async fn next_item(&mut self) -> Option<T> {
        self.recv().await.map(|(_pos, item)| item)
    }
}

impl PumpSource for FsyncWatermarkSubscriberHandle {
    type Item = FsyncWatermark;
    async fn next_item(&mut self) -> Option<FsyncWatermark> {
        self.recv().await.map(|(_pos, w)| w)
    }
}

impl PumpSource for TxReceiptsBoundarySubscriberHandle {
    type Item = BlockBoundary;
    async fn next_item(&mut self) -> Option<BlockBoundary> {
        self.recv().await.map(|(_pos, b)| b)
    }
}

impl PumpSource for TxErrorsSubscriberHandle {
    type Item = TxError;
    async fn next_item(&mut self) -> Option<TxError> {
        self.recv().await.map(|(_pos, e)| e)
    }
}

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
    pub fn open(
        rt: &AeronRuntime,
        channels: &ChannelsConfig,
        recorder_id: u8,
        executor_count: u32,
    ) -> Result<Self, IngressError> {
        let (receipts_tx, _) = broadcast::channel::<Receipt>(1024);
        let (watermarks_tx, _) = broadcast::channel::<QuorumWatermark>(1024);
        let (local_fsync_tx, _) = broadcast::channel::<FsyncWatermark>(1024);
        let (block_boundaries_tx, _) = broadcast::channel::<BlockBoundary>(1024);
        let (tx_errors_tx, _) = broadcast::channel::<TxError>(1024);

        let mds = channels.tx_receipts_mds_enabled();
        if mds {
            tracing::info!(
                executor_count,
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
            .map_err(|e| IngressError::Internal(format!("open tx_receipts: {e}")))?;
        // `into_receiver()`: the handle's AeronRuntime clone must not
        // travel into the pump task. That ownership cycle would keep the
        // runtime alive forever; see
        // `TxReceiptsSubscriberHandle::into_receiver`. This is harmless
        // today only because `main` returns without joining on the
        // streams. It once made the validator unkillable by SIGTERM.
        spawn_pump(receipts_sub.into_receiver(), receipts_tx.clone());

        // Quorum and durable watermark: in the cluster-only topology,
        // this bus is fed by the Aeron Cluster egress observer that
        // `main` spawns. Cluster mode replaced the standalone sealer that
        // used to publish this on Aeron, so there is no Aeron
        // `quorum_watermark` subscription here. The bus and its
        // `subscribe_watermark()` surface are unchanged; only the
        // producer moved.

        // This is the per-recorder fsync watermark.
        let fsync_sub = FsyncWatermarkSubscriberHandle::open(rt, channels, recorder_id)
            .map_err(|e| IngressError::Internal(format!("open fsync watermark: {e}")))?;
        spawn_pump(fsync_sub, local_fsync_tx.clone());

        // This is the tx_receipts to BlockBoundary fan-out, the
        // `tx_receipts_stream_id + 1` side stream. It uses the same MDS
        // vs. IPC branch as the receipt stream above: it attaches the
        // same per-replica executor endpoints to the boundary MDS
        // subscription.
        let boundary_sub =
            TxReceiptsBoundarySubscriberHandle::open_auto(rt, channels, executor_count)
                .map_err(|e| IngressError::Internal(format!("open tx_receipts boundaries: {e}")))?;
        spawn_pump(boundary_sub, block_boundaries_tx.clone());

        // This is the tx_errors to TxError fan-out.
        let errors_sub = TxErrorsSubscriberHandle::open(rt, channels)
            .map_err(|e| IngressError::Internal(format!("open tx_errors: {e}")))?;
        spawn_pump(errors_sub, tx_errors_tx.clone());

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
        let (src_tx, src_rx) = tokio::sync::mpsc::unbounded_channel::<(BPosition, u64)>();
        let (bus, _) = broadcast::channel::<u64>(16);
        let mut sub_a = bus.subscribe();
        let mut sub_b = bus.subscribe();
        spawn_pump(src_rx, bus.clone());

        let pos = BPosition {
            term_id: 0,
            term_offset: 0,
        };
        src_tx.send((pos, 7)).unwrap();
        src_tx.send((pos, 8)).unwrap();
        assert_eq!(sub_a.recv().await.unwrap(), 7);
        assert_eq!(sub_a.recv().await.unwrap(), 8);
        assert_eq!(sub_b.recv().await.unwrap(), 7);
        assert_eq!(sub_b.recv().await.unwrap(), 8);

        // Closing the source ends the pump: the bus's only sender clone
        // inside the pump task drops, so subscribers see Closed.
        drop(src_tx);
        drop(bus);
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
