//! Pub/sub trait surface that the proxy talks to.
//!
//! In production, adapters implement these traits. The adapters wrap the
//! real Aeron publishers and subscribers from [`log`]. For unit and
//! integration tests, this crate provides [`MockChannels`], a fully
//! in-process implementation. It uses `tokio::sync::mpsc` for partition
//! publish, which has a single consumer per partition, and
//! `tokio::sync::broadcast` for the `tx_receipts`, quorum-watermark, and
//! block-boundary fan-out streams.
//!
//! Wire types come only from [`types`]. This module defines no new wire
//! types.

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};

use kardamom_types::{
    BlockBoundary, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope, TxError,
};

use crate::error::IngressError;

/// Publisher surface. The proxy writes validated `TxEnvelope`s onto the
/// sender-sharded `tx_data` streams. `partition_for(envelope.sender, K)`
/// gives the shard index.
#[async_trait]
pub trait IngressPublication: Send + Sync + 'static {
    /// Publishes `envelope` onto `channel_A[shard]`. Multiple proxies can
    /// publish to the same shard's A stream at the same time. Aeron's
    /// shared publication semantics put them into one canonical byte
    /// order.
    async fn publish_tx_data(&self, shard: usize, envelope: TxEnvelope)
    -> Result<(), IngressError>;
}

/// Subscriber surface. The proxy subscribes to the `tx_receipts` `Receipt`
/// stream, which drives the in-memory `tx_hash` and (sender, nonce) indexes
/// and client release; the quorum-watermark stream, for ack gating; and
/// the `tx_receipts` `BlockBoundary` stream, for `eth_blockNumber`.
pub trait IngressSubscription: Send + Sync + 'static {
    /// Stream of enriched `Receipt`s observed on `tx_receipts`. Drives the
    /// in-memory `tx_hash -> Receipt` and `(sender, nonce) -> Receipt`
    /// indexes, and releases parked client submissions in
    /// `PendingReceipts`.
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt>;
    /// Stream of `QuorumWatermark` snapshots.
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark>;
    /// Stream of `FsyncWatermark` snapshots from the local recorder: the
    /// per-recorder watermark stream for the host this proxy runs on.
    /// Ack policies that gate on local fsync use this.
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark>;
    /// Stream of `BlockBoundary` markers on `tx_receipts`. Backs
    /// `eth_blockNumber`.
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary>;
    /// Stream of `TxError` records that the sequencer emits when it
    /// rejects an inbound tx, for example for a past nonce or a
    /// duplicate. Drives early release of parked client submissions with
    /// a JSON-RPC error.
    fn subscribe_tx_errors(&self) -> broadcast::Receiver<TxError>;
}

/// One name for the `(publisher, subscriber)` pair that every proxy
/// helper needs. This replaces two separate type parameters, each with
/// its own `Clone + 'static` bound, with one.
pub(crate) trait ProxyBackend: Send + Sync + 'static {
    type Pub: IngressPublication + Clone + 'static;
    type Sub: IngressSubscription + Clone + 'static;
}

impl<P, S> ProxyBackend for (P, S)
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    type Pub = P;
    type Sub = S;
}

/// Capacity of each `tx_receipts`, watermark, and `tx_errors` broadcast
/// bus. [`crate::aeron_adapters::LiveIngressSubscription`] uses the same
/// value for its live buses.
pub(crate) const BUS_CAPACITY: usize = 1024;

// ============================================================================
// MockChannels: in-process implementation for tests and benches.
// ============================================================================

/// In-process mock of the future Aeron-backed channels. Every integration
/// test in `tests/` and every criterion bench uses this.
#[derive(Clone)]
pub struct MockChannels {
    /// One sender per `tx_data` shard.
    pub(crate) tx_data_tx: Vec<mpsc::UnboundedSender<TxEnvelope>>,
    pub receipt_bus: broadcast::Sender<Receipt>,
    pub watermark_bus: broadcast::Sender<QuorumWatermark>,
    pub(crate) local_fsync_bus: broadcast::Sender<FsyncWatermark>,
    pub(crate) block_boundary_bus: broadcast::Sender<BlockBoundary>,
    pub tx_error_bus: broadcast::Sender<TxError>,
}

impl MockChannels {
    /// Builds a fresh bus with `shards` `tx_data` lanes. Returns the bus and
    /// a `Vec` of receivers, one per shard. The test's fake sequencer
    /// drains these.
    #[must_use]
    pub fn new(shards: usize) -> (Self, Vec<mpsc::UnboundedReceiver<TxEnvelope>>) {
        let (tx_vec, rx_vec): (Vec<_>, Vec<_>) =
            (0..shards).map(|_| mpsc::unbounded_channel()).unzip();
        let (receipt_bus, _) = broadcast::channel(BUS_CAPACITY);
        let (watermark_bus, _) = broadcast::channel(BUS_CAPACITY);
        let (local_fsync_bus, _) = broadcast::channel(BUS_CAPACITY);
        let (block_boundary_bus, _) = broadcast::channel(BUS_CAPACITY);
        let (tx_error_bus, _) = broadcast::channel(BUS_CAPACITY);
        (
            Self {
                tx_data_tx: tx_vec,
                receipt_bus,
                watermark_bus,
                local_fsync_bus,
                block_boundary_bus,
                tx_error_bus,
            },
            rx_vec,
        )
    }
}

#[async_trait]
impl IngressPublication for MockChannels {
    async fn publish_tx_data(
        &self,
        shard: usize,
        envelope: TxEnvelope,
    ) -> Result<(), IngressError> {
        self.tx_data_tx
            .get(shard)
            .ok_or_else(|| {
                IngressError::PartitionUnavailable(format!("shard {shard} out of range"))
            })?
            .send(envelope)
            .map_err(|e| IngressError::PartitionUnavailable(e.to_string()))
    }
}

impl IngressSubscription for MockChannels {
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt> {
        self.receipt_bus.subscribe()
    }
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark> {
        self.watermark_bus.subscribe()
    }
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark> {
        self.local_fsync_bus.subscribe()
    }
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary> {
        self.block_boundary_bus.subscribe()
    }
    fn subscribe_tx_errors(&self) -> broadcast::Receiver<TxError> {
        self.tx_error_bus.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256};
    use bytes::Bytes;

    #[tokio::test]
    async fn mock_routes_to_shard() {
        let (mock, mut rx) = MockChannels::new(4);
        let env = TxEnvelope {
            correlation_id: 1,
            raw_tx: Bytes::new(),
            sender: Address::ZERO,
            tx_hash: B256::ZERO,
        };
        mock.publish_tx_data(2, env.clone()).await.unwrap();
        let received = rx[2].recv().await.unwrap();
        assert_eq!(received.correlation_id, 1);
        // The other shards stay empty.
        assert!(rx[0].try_recv().is_err());
    }
}
