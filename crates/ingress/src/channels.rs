//! Pub/sub trait surface the proxy talks to.
//!
//! In production these traits will be implemented by adapters that wrap the
//! real Aeron publishers/subscribers from [`log`]. For unit and
//! integration tests we provide [`MockChannels`], a fully in-process
//! implementation that uses `tokio::sync::mpsc` (for partition publish, which
//! has a single consumer per partition) and `tokio::sync::broadcast` (for the
//! tx_receipts / quorum-watermark / block-boundary fan-out streams).
//!
//! Wire types come exclusively from [`types`]; this
//! module defines **no new wire types**.

use async_trait::async_trait;
use tokio::sync::{broadcast, mpsc};

use kardamom_types::{
    BlockBoundary, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope, TxError,
};

use crate::error::IngressError;

/// Publisher surface. The proxy writes validated `TxEnvelope`s onto the
/// sender-sharded tx_data streams (`partition_for(envelope.sender, K)`
/// gives the shard index).
#[async_trait]
pub trait IngressPublication: Send + Sync + 'static {
    /// Publish `envelope` onto `channel_A[shard]`. Multiple proxies can
    /// concurrently publish to the same shard's A stream — Aeron's shared
    /// publication semantics serialize them into one canonical byte order.
    async fn publish_tx_data(&self, shard: usize, envelope: TxEnvelope)
    -> Result<(), IngressError>;
}

/// Subscriber surface. The proxy subscribes to the tx_receipts `Receipt`
/// stream (drives the in-memory tx_hash / (sender, nonce) indexes and client
/// release), the quorum-watermark stream (for ack gating), and the
/// tx_receipts `BlockBoundary` stream (for `eth_blockNumber`).
pub trait IngressSubscription: Send + Sync + 'static {
    /// Stream of enriched `Receipt`s observed on tx_receipts. Drives the
    /// in-memory `tx_hash → Receipt` and `(sender, nonce) → Receipt` indexes
    /// and releases parked client submissions in `PendingReceipts`.
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt>;
    /// Stream of `QuorumWatermark` snapshots.
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark>;
    /// Stream of `FsyncWatermark` snapshots from the *local* recorder
    /// (the per-recorder watermark stream for the host this proxy runs on).
    /// Used by ack policies that gate on local fsync.
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark>;
    /// Stream of `BlockBoundary` markers on tx_receipts; backs `eth_blockNumber`.
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary>;
    /// Stream of `TxError` records emitted by the sequencer when an inbound
    /// tx is rejected (e.g. past-nonce / duplicate). Drives early release of
    /// parked client submissions with a JSON-RPC error.
    fn subscribe_tx_errors(&self) -> broadcast::Receiver<TxError>;
}

// ============================================================================
// MockChannels — in-process implementation for tests and benches.
// ============================================================================

/// In-process mock of the future Aeron-backed channels. Used in every `tests/`
/// integration test and in the criterion benches.
#[derive(Clone)]
pub struct MockChannels {
    /// One sender per tx_data shard.
    pub tx_data_tx: Vec<mpsc::UnboundedSender<TxEnvelope>>,
    pub receipt_bus: broadcast::Sender<Receipt>,
    pub watermark_bus: broadcast::Sender<QuorumWatermark>,
    pub local_fsync_bus: broadcast::Sender<FsyncWatermark>,
    pub block_boundary_bus: broadcast::Sender<BlockBoundary>,
    pub tx_error_bus: broadcast::Sender<TxError>,
}

impl MockChannels {
    /// Build a fresh bus with `shards` tx_data lanes. Returns the bus
    /// and a `Vec` of receivers, one per shard (the test's "fake
    /// sequencer" drains these).
    pub fn new(shards: usize) -> (Self, Vec<mpsc::UnboundedReceiver<TxEnvelope>>) {
        let mut tx_vec = Vec::with_capacity(shards);
        let mut rx_vec = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (tx, rx) = mpsc::unbounded_channel();
            tx_vec.push(tx);
            rx_vec.push(rx);
        }
        let (receipt_bus, _) = broadcast::channel(1024);
        let (watermark_bus, _) = broadcast::channel(1024);
        let (local_fsync_bus, _) = broadcast::channel(1024);
        let (block_boundary_bus, _) = broadcast::channel(1024);
        let (tx_error_bus, _) = broadcast::channel(1024);
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
        // Other shards stay empty.
        assert!(rx[0].try_recv().is_err());
    }
}
