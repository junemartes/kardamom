//! `IngressProxy`: wires rate-limit, sig-verify, routing, pending-receipts,
//! receipt-cache, and the watermark/block-boundary watchers into a single
//! process.
//!
//! Module layout: this file owns the struct, construction, and accessors;
//! [`submit`] owns the client-facing submit path; [`watchers`] owns the
//! background stream watchers spawned by [`IngressProxy::new`].

mod submit;
mod watchers;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::{Address, B256};
use tokio::sync::broadcast;

use kardamom_types::{Receipt, TxError};

use crate::channels::{IngressPublication, IngressSubscription};
use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::pending::PendingReceipts;
use crate::rate_limit::PerIpLimiter;
use crate::receipt_cache::ReceiptCache;
use crate::routing::partition_for;
use crate::seen_receipts::SeenReceipts;
use crate::sig_verify::BatchVerifier;
use crate::tx_error_dedup::TxErrorDedup;

/// Drains a `broadcast::Receiver<T>` and forwards each item to `f`, swallowing
/// `Lagged` and exiting on `Closed`. Used by the four proxy watcher tasks.
fn spawn_broadcast_watcher<T, F, Fut>(mut rx: broadcast::Receiver<T>, mut f: F)
where
    T: Clone + Send + 'static,
    F: FnMut(T) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(item) => f(item).await,
                Err(broadcast::error::RecvError::Closed) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
            }
        }
    });
}

/// Pack a replica id + per-replica sequence into a globally-unique opaque
/// `correlation_id`: top 16 bits `ingress_id`, low 48 bits `seq`. See
/// [`IngressProxy::next_correlation_id`].
#[inline]
pub fn pack_correlation_id(ingress_id: u16, seq: u64) -> u64 {
    ((ingress_id as u64) << 48) | (seq & 0x0000_FFFF_FFFF_FFFF)
}

/// Extract the originating `ingress_id` from a packed `correlation_id`.
#[inline]
pub fn ingress_id_of(correlation_id: u64) -> u16 {
    (correlation_id >> 48) as u16
}

/// Output of the shared submit-path head: identity of a decoded, verified
/// submission plus a receipt-cache hit if this is a resubmission.
struct ValidatedSubmission {
    sender: Address,
    nonce: u64,
    tx_hash: B256,
    cached: Option<Receipt>,
}

/// Handle returned by `IngressProxy::start`. Drop it to shut down listeners
/// gracefully.
pub struct IngressHandle {
    /// The actual bound address of the jsonrpsee server (resolved from
    /// `0` if the caller asked for an ephemeral port).
    pub jsonrpc_addr: std::net::SocketAddr,
    pub jsonrpc_handle: jsonrpsee::server::ServerHandle,
}

/// Composed orchestrator. Cheaply clonable (everything inside is `Arc` or a
/// trait object behind an `Arc`).
pub struct IngressProxy<P, S>
where
    P: IngressPublication + Clone,
    S: IngressSubscription + Clone,
{
    pub(crate) cfg: IngressConfig,
    pub(crate) rate_limiter: Arc<PerIpLimiter>,
    pub(crate) verifier: Arc<BatchVerifier>,
    pub(crate) pending: Arc<PendingReceipts>,
    pub(crate) cache: Arc<ReceiptCache>,
    /// First-wins tx-hash dedup for the tx_receipts MDS fan-in: drops the
    /// duplicate receipt copies the N executor replicas emit so a tx's
    /// must-deliver ack fires exactly once. No-op on the single-executor IPC
    /// path. See [`crate::seen_receipts`].
    pub(crate) seen_receipts: Arc<SeenReceipts>,
    /// Consumer-side tx_errors dedup for P racing sequencer replicas: drops
    /// the twin's duplicate copy of each per-tx rejection, and suppresses a
    /// rejection once a success for the same `(sender, nonce)` was observed.
    /// No-op on a single-replica (P=1) deployment. See
    /// [`crate::tx_error_dedup`].
    pub(crate) tx_error_dedup: Arc<TxErrorDedup>,
    pub(crate) publication: P,
    pub(crate) subscription: S,
    pub(crate) correlation_seq: Arc<AtomicU64>,
    ///highest `BlockBoundary.block_number` observed on
    /// tx_receipts. Read by `eth_blockNumber`. `AtomicU64` is plenty —
    /// monotonic, single-writer (the BlockBoundary watcher), many readers.
    pub(crate) latest_block_number: Arc<AtomicU64>,
    /// Post-dedup receipt re-broadcast: the tx_receipts watcher forwards each
    /// *first-seen* receipt here, so `kardamom_subscribeReceipts` sessions see
    /// exactly one copy per tx rather than the raw N-replica MDS fan-in.
    pub(crate) receipt_feed: broadcast::Sender<Receipt>,
    /// Post-dedup tx-error re-broadcast (same pattern as `receipt_feed`).
    pub(crate) tx_error_feed: broadcast::Sender<TxError>,
}

/// Capacity of the deduped receipt/error re-broadcast feeds. A subscriber
/// that lags more than this many items receives a `Lagged` notification and
/// must fall back to `eth_getTransactionReceipt` for the gap. 32k: at 8192
/// a subscriber stalled ~1.7s at 4,800 tx/s overflowed the ring — the
/// leading suspect for the ~0.1% silent feed misses under sustained load;
/// 32k tolerates ~7s at that rate for ~10MB of buffered receipts.
const FEED_CAPACITY: usize = 32 * 1024;

impl<P, S> Clone for IngressProxy<P, S>
where
    P: IngressPublication + Clone,
    S: IngressSubscription + Clone,
{
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            rate_limiter: self.rate_limiter.clone(),
            verifier: self.verifier.clone(),
            pending: self.pending.clone(),
            cache: self.cache.clone(),
            seen_receipts: self.seen_receipts.clone(),
            tx_error_dedup: self.tx_error_dedup.clone(),
            publication: self.publication.clone(),
            subscription: self.subscription.clone(),
            correlation_seq: self.correlation_seq.clone(),
            latest_block_number: self.latest_block_number.clone(),
            receipt_feed: self.receipt_feed.clone(),
            tx_error_feed: self.tx_error_feed.clone(),
        }
    }
}

impl<P, S> IngressProxy<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub fn new(cfg: IngressConfig, publication: P, subscription: S) -> Self {
        let rate_limiter = Arc::new(PerIpLimiter::new(
            cfg.rate_limit_per_ip_per_sec,
            cfg.rate_limit_burst,
        ));
        let verifier = Arc::new(BatchVerifier::new(
            cfg.sig_verify_batch_depth,
            cfg.sig_verify_flush_window,
        ));
        let pending = Arc::new(PendingReceipts::new(cfg.ack_policy));
        let cache = Arc::new(ReceiptCache::new(cfg.receipt_cache_capacity));
        let seen_receipts = Arc::new(SeenReceipts::default());
        let tx_error_dedup = Arc::new(TxErrorDedup::default());
        let me = Self {
            cfg,
            rate_limiter,
            verifier,
            pending,
            cache,
            seen_receipts,
            tx_error_dedup,
            publication,
            subscription,
            correlation_seq: Arc::new(AtomicU64::new(0)),
            latest_block_number: Arc::new(AtomicU64::new(0)),
            receipt_feed: broadcast::channel(FEED_CAPACITY).0,
            tx_error_feed: broadcast::channel(FEED_CAPACITY).0,
        };
        me.spawn_tx_receipts_watcher();
        me.spawn_tx_errors_watcher();
        // Only subscribe to watermark streams the configured policy needs.
        if me.cfg.ack_policy.requires_quorum() {
            me.spawn_quorum_watermark_watcher();
        }
        if me.cfg.ack_policy.requires_local_fsync() {
            me.spawn_local_fsync_watermark_watcher();
        }
        me.spawn_block_boundary_watcher();
        me
    }

    /// Highest `BlockBoundary.block_number` observed on tx_receipts. Backs
    /// `eth_blockNumber`.
    #[inline]
    pub fn latest_block_number(&self) -> u64 {
        self.latest_block_number.load(Ordering::Acquire)
    }

    /// Next globally-unique `correlation_id` for this replica.
    ///
    /// `correlation_id` is an opaque pass-through (stamped into `TxRef`, carried
    /// into the batcher frame, logged) — nothing dedups or orders on it. The
    /// per-process counter alone collides across active/active replicas, so we
    /// namespace it: the top 16 bits are `ingress_id`, the low 48 bits a
    /// per-replica monotonic sequence. 48 bits ≈ 2.8e14 txs before wrap, and
    /// wrap is benign. The `(replica, sequence)` pair is globally unique.
    #[inline]
    pub fn next_correlation_id(&self) -> u64 {
        let seq = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
        pack_correlation_id(self.cfg.ingress_id, seq)
    }

    /// Lookup a receipt by `tx_hash` in the in-memory tx_receipts index.
    /// The executor publishes the enriched `Receipt` onto tx_receipts; ingress
    /// subscribes and answers `eth_getTransactionReceipt` straight from RAM
    /// with no state-DB join. Returns `None` for txs that have not yet been
    /// observed (including those evicted from the bounded cache).
    pub fn lookup_receipt_by_hash(&self, tx_hash: B256) -> Option<Receipt> {
        self.cache.lookup_by_tx_hash(tx_hash)
    }

    /// Live feed of *deduped* receipts (one copy per tx, post MDS fan-in
    /// dedup). Backs `kardamom_subscribeReceipts`.
    pub fn subscribe_receipt_feed(&self) -> broadcast::Receiver<Receipt> {
        self.receipt_feed.subscribe()
    }

    /// Live feed of *deduped* sequencer tx rejections. Backs the error
    /// frames on `kardamom_subscribeReceipts`.
    pub fn subscribe_tx_error_feed(&self) -> broadcast::Receiver<TxError> {
        self.tx_error_feed.subscribe()
    }

    /// Resolves the partition this proxy would route `sender` to. Used by
    /// tests + tooling.
    #[inline]
    pub fn partition_for(&self, sender: alloy_primitives::Address) -> u32 {
        partition_for(sender, self.cfg.partition_count_m)
    }

    /// Read-only access to the configured `IngressConfig`.
    #[inline]
    pub fn config(&self) -> &IngressConfig {
        &self.cfg
    }

    /// Start all configured listeners (jsonrpsee HTTP+WS, optional TCP,
    /// optional UDS).
    pub async fn start(self) -> Result<IngressHandle, IngressError>
    where
        P: 'static,
        S: 'static,
    {
        let (jsonrpc_addr, jsonrpc_handle) =
            crate::json_rpc::start_jsonrpc_server(self.clone(), self.cfg.jsonrpc_bind).await?;
        #[cfg(feature = "binary-protocol")]
        {
            if let Some(addr) = self.cfg.binary_tcp_bind {
                crate::binary::spawn_tcp_listener(self.clone(), addr);
            }
            if let Some(path) = self.cfg.binary_uds_path.clone() {
                // Best-effort unlink stale socket.
                let _ = std::fs::remove_file(&path);
                crate::binary::spawn_uds_listener(self.clone(), &path)
                    .map_err(|e| IngressError::Internal(format!("uds bind: {e}")))?;
            }
        }
        Ok(IngressHandle {
            jsonrpc_addr,
            jsonrpc_handle,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ingress_id_of, pack_correlation_id};

    #[test]
    fn correlation_id_packs_ingress_id_in_high_bits() {
        // Low 48 bits are the sequence; top 16 bits are the replica id.
        assert_eq!(pack_correlation_id(0, 0), 0);
        assert_eq!(pack_correlation_id(0, 41), 41);
        assert_eq!(pack_correlation_id(7, 0), 7u64 << 48);
        assert_eq!(pack_correlation_id(7, 5), (7u64 << 48) | 5);
        // Round-trips the replica id out.
        for id in [0u16, 1, 7, 256, u16::MAX] {
            for seq in [0u64, 1, 1_000_000, (1u64 << 48) - 1] {
                let c = pack_correlation_id(id, seq);
                assert_eq!(ingress_id_of(c), id, "id={id} seq={seq}");
                assert_eq!(c & 0x0000_FFFF_FFFF_FFFF, seq, "seq preserved");
            }
        }
    }

    #[test]
    fn distinct_replicas_never_collide_for_any_sequence() {
        // Two replicas emitting the same local sequence produce distinct ids.
        for seq in [0u64, 1, 99, 1u64 << 40] {
            assert_ne!(pack_correlation_id(1, seq), pack_correlation_id(2, seq));
        }
    }
}
