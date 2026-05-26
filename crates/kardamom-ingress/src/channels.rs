//! Pub/sub trait surface the proxy talks to.
//!
//! In production these traits will be implemented by adapters that wrap the
//! real Aeron publishers/subscribers from [`kardamom_log`]. For unit and
//! integration tests we provide [`MockChannels`], a fully in-process
//! implementation that uses `tokio::sync::mpsc` (for partition publish, which
//! has a single consumer per partition) and `tokio::sync::broadcast` (for the
//! receipt-cache / quorum-watermark / block-boundary fan-out streams).
//!
//! Wire types come exclusively from [`kardamom_types`]; this
//! module defines **no new wire types**.

use std::collections::HashMap;
use std::sync::Arc;

use alloy_primitives::B256;
use async_trait::async_trait;
use tokio::sync::{Mutex, broadcast, mpsc};

use kardamom_types::{
    BPosition, BlockBoundary, CachedReceipt, FsyncWatermark, QuorumWatermark, Receipt, TxEnvelope,
};

use crate::error::IngressError;

/// Publisher surface. The proxy writes validated `TxEnvelope`s onto the
/// sender-sharded tx_data streams (`partition_for(envelope.sender, K)`
/// gives the shard index), and republishes `CachedReceipt` entries to the
/// receipt-cache channel.
#[async_trait]
pub trait IngressPublication: Send + Sync + 'static {
    /// Publish `envelope` onto `channel_A[shard]`. Multiple proxies can
    /// concurrently publish to the same shard's A stream — Aeron's shared
    /// publication semantics serialize them into one canonical byte order.
    async fn publish_tx_data(&self, shard: usize, envelope: TxEnvelope)
    -> Result<(), IngressError>;

    /// Publish a `CachedReceipt` onto the receipt-cache channel.
    async fn publish_receipt_cache(&self, cached: CachedReceipt) -> Result<(), IngressError>;
}

/// Subscriber surface. The proxy subscribes to the receipt-cache channel (for
/// release), the quorum-watermark stream (for I2 ack gating), the tx_receipts
/// `Receipt` stream (for metrics + cache replay), and the tx_receipts
///`BlockBoundary` stream (, for `eth_blockNumber`).
pub trait IngressSubscription: Send + Sync + 'static {
    /// Stream of `Receipt`s observed on tx_receipts. Surface used for metrics
    /// and bookkeeping; the receipt-cache stream is the source of truth for
    /// client release.
    fn subscribe_receipts(&self) -> broadcast::Receiver<Receipt>;
    /// Stream of `QuorumWatermark` snapshots.
    fn subscribe_watermark(&self) -> broadcast::Receiver<QuorumWatermark>;
    /// Stream of `FsyncWatermark` snapshots from the *local* recorder
    /// (the per-recorder watermark stream for the host this proxy runs on).
    /// Used by ack policies that gate on local fsync.
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark>;
    /// Stream of `CachedReceipt` messages (executor → proxy nonce cache).
    fn subscribe_receipt_cache(&self) -> broadcast::Receiver<CachedReceipt>;
    /// Stream of `BlockBoundary` markers on tx_receipts; backs `eth_blockNumber`.
    fn subscribe_block_boundaries(&self) -> broadcast::Receiver<BlockBoundary>;
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
    pub receipt_cache_bus: broadcast::Sender<CachedReceipt>,
    pub block_boundary_bus: broadcast::Sender<BlockBoundary>,
    pub published_cache: Arc<Mutex<Vec<CachedReceipt>>>,
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
        let (receipt_cache_bus, _) = broadcast::channel(1024);
        let (block_boundary_bus, _) = broadcast::channel(1024);
        (
            Self {
                tx_data_tx: tx_vec,
                receipt_bus,
                watermark_bus,
                local_fsync_bus,
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
    fn subscribe_local_fsync_watermark(&self) -> broadcast::Receiver<FsyncWatermark> {
        self.local_fsync_bus.subscribe()
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
//, but the trait demands the full `basic`/`storage`/`code_by_hash`
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
