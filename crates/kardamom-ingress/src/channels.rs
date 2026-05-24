//! Pub/sub trait surface the proxy talks to.
//!
//! In production these traits will be implemented by adapters that wrap the
//! real Aeron publishers/subscribers from [`kardamom_log`]. For unit and
//! integration tests we provide [`MockChannels`], a fully in-process
//! implementation that uses `tokio::sync::mpsc` (for partition publish, which
//! has a single consumer per partition) and `tokio::sync::broadcast` (for the
//! receipt-cache / quorum-watermark / block-boundary fan-out streams).
//!
//! Wire types come exclusively from [`kardamom_types`] per S0 D-Sh1; this
//! module defines **no new wire types**.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::B256;
use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast, mpsc};

use kardamom_types::{
    BPosition, BlockBoundary, CachedReceipt, QuorumWatermark, Receipt, TxEnvelope,
};

use crate::error::IngressError;

/// Publisher surface. The proxy writes `TxEnvelope` to a specific partition's
/// `ingress[i]` channel and (in the receipt-watcher hot path) republishes
/// `CachedReceipt` entries to the receipt-cache channel.
#[async_trait]
pub trait IngressPublication: Send + Sync + 'static {
    /// Publish `envelope` to the partition selected by
    /// `partition_for(envelope.sender, M)`.
    async fn publish_ingress(
        &self,
        partition: usize,
        envelope: TxEnvelope,
    ) -> Result<(), IngressError>;

    /// Publish a `CachedReceipt` onto the receipt-cache channel.
    async fn publish_receipt_cache(&self, cached: CachedReceipt) -> Result<(), IngressError>;
}

/// Subscriber surface. The proxy subscribes to the receipt-cache channel (for
/// release), the quorum-watermark stream (for I2 ack gating), the channel-C
/// `Receipt` stream (for metrics + cache replay), and the channel-C
/// `BlockBoundary` stream (per S0 D-Sh5, for `eth_blockNumber`).
pub trait IngressSubscription: Send + Sync + 'static {
    /// Stream of `Receipt`s observed on channel C. Surface used for metrics
    /// and bookkeeping; the receipt-cache stream is the source of truth for
    /// client release.
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt>;
    /// Stream of `QuorumWatermark` snapshots.
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark>;
    /// Stream of `CachedReceipt` messages (executor → proxy nonce cache).
    fn subscribe_receipt_cache(&self) -> broadcast::Receiver<CachedReceipt>;
    /// Stream of `BlockBoundary` markers on channel C; backs `eth_blockNumber`.
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary>;
}

// ============================================================================
// MockChannels — in-process implementation for tests and benches.
// ============================================================================

/// In-process mock of the future Aeron-backed channels. Used in every `tests/`
/// integration test and in the criterion benches.
#[derive(Clone)]
pub struct MockChannels {
    pub ingress_tx: Vec<mpsc::UnboundedSender<TxEnvelope>>,
    pub receipt_bus: broadcast::Sender<Receipt>,
    pub watermark_bus: broadcast::Sender<QuorumWatermark>,
    pub receipt_cache_bus: broadcast::Sender<CachedReceipt>,
    pub block_boundary_bus: broadcast::Sender<BlockBoundary>,
    pub published_cache: Arc<Mutex<Vec<CachedReceipt>>>,
}

impl MockChannels {
    /// Build a fresh bus with `partitions` independent ingress lanes. Returns
    /// the bus and a `Vec` of receivers, one per partition (the test's "fake
    /// sequencer" drains these).
    pub fn new(partitions: usize) -> (Self, Vec<mpsc::UnboundedReceiver<TxEnvelope>>) {
        let mut tx_vec = Vec::with_capacity(partitions);
        let mut rx_vec = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            let (tx, rx) = mpsc::unbounded_channel();
            tx_vec.push(tx);
            rx_vec.push(rx);
        }
        let (receipt_bus, _) = broadcast::channel(1024);
        let (watermark_bus, _) = broadcast::channel(1024);
        let (receipt_cache_bus, _) = broadcast::channel(1024);
        let (block_boundary_bus, _) = broadcast::channel(1024);
        (
            Self {
                ingress_tx: tx_vec,
                receipt_bus,
                watermark_bus,
                receipt_cache_bus,
                block_boundary_bus,
                published_cache: Arc::new(Mutex::new(Vec::new())),
            },
            rx_vec,
        )
    }
}

#[async_trait]
impl IngressPublication for MockChannels {
    async fn publish_ingress(
        &self,
        partition: usize,
        envelope: TxEnvelope,
    ) -> Result<(), IngressError> {
        self.ingress_tx
            .get(partition)
            .ok_or_else(|| {
                IngressError::PartitionUnavailable(format!("partition {partition} out of range"))
            })?
            .send(envelope)
            .map_err(|e| IngressError::PartitionUnavailable(e.to_string()))
    }

    async fn publish_receipt_cache(&self, cached: CachedReceipt) -> Result<(), IngressError> {
        self.published_cache.lock().await.push(cached.clone());
        // Receiver count of 0 is fine — receipt-cache is best-effort.
        let _ = self.receipt_cache_bus.send(cached);
        Ok(())
    }
}

impl IngressSubscription for MockChannels {
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt> {
        self.receipt_bus.subscribe()
    }
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark> {
        self.watermark_bus.subscribe()
    }
    fn subscribe_receipt_cache(&self) -> broadcast::Receiver<CachedReceipt> {
        self.receipt_cache_bus.subscribe()
    }
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary> {
        self.block_boundary_bus.subscribe()
    }
}

// ============================================================================
// InMemoryStateDb — implements `kardamom_types::StateDatabase` for tests.
//
// Real production proxy gets the libmdbx-backed impl shipped by S6 once that
// crate lands. The proxy only uses the receipt + tx_hash_index read paths
// (per S0 D-Sh4), but the trait demands the full `basic`/`storage`/`code_by_hash`
// surface — we stub those out with a sane default.
// ============================================================================

/// In-memory `StateDatabase` for tests, benches, and the v0 default proxy
/// configuration until S6 lands. Only `get_tx_position` and `get_receipt`
/// carry real semantics; account/storage queries return empty values.
#[derive(Default, Clone)]
pub struct InMemoryStateDb {
    inner: Arc<std::sync::RwLock<StateDbInner>>,
}

#[derive(Default)]
struct StateDbInner {
    tx_hash_index: HashMap<B256, BPosition>,
    receipts: HashMap<BPosition, Receipt>,
}

impl InMemoryStateDb {
    pub fn new() -> Self {
        Self::default()
    }

    /// Test helper: record both index and receipt for an executed tx.
    pub fn record(&self, tx_hash: B256, position: BPosition, receipt: Receipt) {
        let mut g = self.inner.write().unwrap();
        g.tx_hash_index.insert(tx_hash, position);
        g.receipts.insert(position, receipt);
    }
}

#[derive(Debug, thiserror::Error)]
#[error("in-memory state db error")]
pub struct InMemoryStateError;

impl kardamom_types::StateError for InMemoryStateError {}

impl kardamom_types::StateDatabase for InMemoryStateDb {
    type Error = InMemoryStateError;

    fn basic(
        &self,
        _address: alloy_primitives::Address,
    ) -> Result<Option<(u64, alloy_primitives::U256, B256)>, Self::Error> {
        Ok(None)
    }

    fn storage(
        &self,
        _address: alloy_primitives::Address,
        _key: B256,
    ) -> Result<alloy_primitives::U256, Self::Error> {
        Ok(alloy_primitives::U256::ZERO)
    }

    fn code_by_hash(&self, _code_hash: B256) -> Result<bytes::Bytes, Self::Error> {
        Ok(bytes::Bytes::new())
    }

    fn get_receipt(&self, pos: BPosition) -> Result<Option<Receipt>, Self::Error> {
        Ok(self.inner.read().unwrap().receipts.get(&pos).cloned())
    }

    fn get_tx_position(&self, tx_hash: B256) -> Result<Option<BPosition>, Self::Error> {
        Ok(self
            .inner
            .read()
            .unwrap()
            .tx_hash_index
            .get(&tx_hash)
            .copied())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;
    use bytes::Bytes;
    use kardamom_types::StateDatabase;

    #[tokio::test]
    async fn mock_routes_to_partition() {
        let (mock, mut rx) = MockChannels::new(4);
        let env = TxEnvelope {
            correlation_id: 1,
            raw_tx: Bytes::new(),
            sender: Address::ZERO,
            tx_hash: B256::ZERO,
        };
        mock.publish_ingress(2, env.clone()).await.unwrap();
        let received = rx[2].recv().await.unwrap();
        assert_eq!(received.correlation_id, 1);
        // Other partitions stay empty.
        assert!(rx[0].try_recv().is_err());
    }

    #[test]
    fn in_memory_state_db_round_trip() {
        let db = InMemoryStateDb::new();
        let tx_hash = B256::repeat_byte(0xAB);
        let pos = BPosition {
            term_id: 1,
            term_offset: 99,
        };
        let receipt = Receipt {
            tx_idx: pos,
            tx_hash,
            status: true,
            gas_used: 21_000,
            logs: Vec::new(),
            write_set_hash: B256::ZERO,
        };
        db.record(tx_hash, pos, receipt.clone());
        assert_eq!(db.get_tx_position(tx_hash).unwrap(), Some(pos));
        assert_eq!(db.get_receipt(pos).unwrap(), Some(receipt));
    }
}
