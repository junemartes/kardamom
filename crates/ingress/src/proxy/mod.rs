//! `IngressProxy` wires rate-limit, sig-verify, routing, pending-receipts,
//! receipt-cache, and the watermark and block-boundary watchers into a
//! single process.
//!
//! Module layout: this file owns the struct, construction, and accessors.
//! [`submit`] owns the client-facing submit path. [`watchers`] owns the
//! background stream watchers that [`IngressProxy::new`] spawns.

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

/// Drains a `broadcast::Receiver<T>` and forwards each item to `f`. Skips
/// `Lagged` and exits on `Closed`. The four proxy watcher tasks use this.
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

/// Packs a replica id and a per-replica sequence into a globally unique,
/// opaque `correlation_id`: the top 16 bits are `ingress_id`, and the low
/// 48 bits are `seq`. See [`IngressProxy::next_correlation_id`].
#[inline]
pub fn pack_correlation_id(ingress_id: u16, seq: u64) -> u64 {
    ((ingress_id as u64) << 48) | (seq & 0x0000_FFFF_FFFF_FFFF)
}

/// Extracts the originating `ingress_id` from a packed `correlation_id`.
#[inline]
pub fn ingress_id_of(correlation_id: u64) -> u16 {
    (correlation_id >> 48) as u16
}

/// Output of the shared submit-path head: the identity of a decoded,
/// verified submission, plus a receipt-cache hit if this is a
/// resubmission.
struct ValidatedSubmission {
    sender: Address,
    nonce: u64,
    tx_hash: B256,
    cached: Option<Receipt>,
}

/// Handle returned by `IngressProxy::start`. Drop it to shut down the
/// listeners gracefully.
pub struct IngressHandle {
    /// The actual bound address of the jsonrpsee server. This is resolved
    /// from `0` if the caller asked for an ephemeral port.
    pub jsonrpc_addr: std::net::SocketAddr,
    pub jsonrpc_handle: jsonrpsee::server::ServerHandle,
}

/// Composed orchestrator. Cheap to clone, since everything inside is an
/// `Arc`, or a trait object behind an `Arc`.
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
    /// First-wins tx-hash dedup for the tx_receipts MDS fan-in. Drops the
    /// duplicate receipt copies that the N executor replicas emit, so a
    /// tx's must-deliver ack fires exactly once. This is a no-op on the
    /// single-executor IPC path. See [`crate::seen_receipts`].
    pub(crate) seen_receipts: Arc<SeenReceipts>,
    /// Consumer-side tx_errors dedup for P racing sequencer replicas.
    /// Drops the twin's duplicate copy of each per-tx rejection, and
    /// suppresses a rejection once a success for the same
    /// `(sender, nonce)` was observed. This is a no-op on a
    /// single-replica (P=1) deployment. See [`crate::tx_error_dedup`].
    pub(crate) tx_error_dedup: Arc<TxErrorDedup>,
    pub(crate) publication: P,
    pub(crate) subscription: S,
    pub(crate) correlation_seq: Arc<AtomicU64>,
    /// The highest `BlockBoundary.block_number` observed on tx_receipts.
    /// `eth_blockNumber` reads this. `AtomicU64` is enough here: the
    /// value only increases, one writer, the BlockBoundary watcher, sets
    /// it, and many readers read it.
    pub(crate) latest_block_number: Arc<AtomicU64>,
    /// Post-dedup receipt re-broadcast. The tx_receipts watcher forwards
    /// each first-seen receipt here, so `kardamom_subscribeReceipts`
    /// sessions see exactly one copy per tx, instead of the raw
    /// N-replica MDS fan-in.
    pub(crate) receipt_feed: broadcast::Sender<Receipt>,
    /// Post-dedup tx-error re-broadcast, the same pattern as
    /// `receipt_feed`.
    pub(crate) tx_error_feed: broadcast::Sender<TxError>,
}

/// Capacity of the deduped receipt and error re-broadcast feeds. A
/// subscriber that lags more than this many items gets a `Lagged`
/// notification, and must fall back to `eth_getTransactionReceipt` for
/// the gap. At 8192, a subscriber stalled for about 1.7s at 4,800 tx/s
/// overflowed the ring; this was the leading suspect for the small share
/// of silent feed misses under sustained load. 32k tolerates about 7s at
/// that rate, for about 10MB of buffered receipts.
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
        // Subscribe only to the watermark streams the configured policy needs.
        if me.cfg.ack_policy.requires_quorum() {
            me.spawn_quorum_watermark_watcher();
        }
        if me.cfg.ack_policy.requires_local_fsync() {
            me.spawn_local_fsync_watermark_watcher();
        }
        me.spawn_block_boundary_watcher();
        me
    }

    /// The highest `BlockBoundary.block_number` observed on tx_receipts.
    /// Backs `eth_blockNumber`.
    #[inline]
    pub fn latest_block_number(&self) -> u64 {
        self.latest_block_number.load(Ordering::Acquire)
    }

    /// Returns the next globally unique `correlation_id` for this
    /// replica.
    ///
    /// `correlation_id` is an opaque pass-through value. It gets stamped
    /// into `TxRef`, carried into the batcher frame, and logged. Nothing
    /// dedups or orders on it. A per-process counter alone would collide
    /// across active/active replicas, so this namespaces it: the top 16
    /// bits are `ingress_id`, and the low 48 bits are a per-replica,
    /// increasing sequence. 48 bits allow about 2.8e14 txs before
    /// wrapping, and a wrap is harmless. The `(replica, sequence)` pair
    /// is globally unique.
    #[inline]
    pub fn next_correlation_id(&self) -> u64 {
        let seq = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
        pack_correlation_id(self.cfg.ingress_id, seq)
    }

    /// Looks up a receipt by `tx_hash` in the in-memory tx_receipts
    /// index. The executor publishes the enriched `Receipt` onto
    /// tx_receipts. Ingress subscribes, and answers
    /// `eth_getTransactionReceipt` straight from RAM, with no state-DB
    /// join. Returns `None` for a tx that has not yet been observed,
    /// including one evicted from the bounded cache.
    pub fn lookup_receipt_by_hash(&self, tx_hash: B256) -> Option<Receipt> {
        self.cache.lookup_by_tx_hash(tx_hash)
    }

    /// Live feed of deduped receipts, one copy per tx, after MDS fan-in
    /// dedup. Backs `kardamom_subscribeReceipts`.
    pub fn subscribe_receipt_feed(&self) -> broadcast::Receiver<Receipt> {
        self.receipt_feed.subscribe()
    }

    /// Live feed of deduped sequencer tx rejections. Backs the error
    /// frames on `kardamom_subscribeReceipts`.
    pub fn subscribe_tx_error_feed(&self) -> broadcast::Receiver<TxError> {
        self.tx_error_feed.subscribe()
    }

    /// Resolves the partition this proxy would route `sender` to. Tests
    /// and tooling use this.
    #[inline]
    pub fn partition_for(&self, sender: alloy_primitives::Address) -> u32 {
        partition_for(sender, self.cfg.partition_count_m)
    }

    /// Read-only access to the configured `IngressConfig`.
    #[inline]
    pub fn config(&self) -> &IngressConfig {
        &self.cfg
    }

    /// Starts every configured listener: jsonrpsee HTTP and WS, an
    /// optional TCP listener, and an optional UDS listener.
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
                // This is a best-effort unlink of a stale socket.
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
        // The low 48 bits are the sequence, and the top 16 bits are the
        // replica id.
        assert_eq!(pack_correlation_id(0, 0), 0);
        assert_eq!(pack_correlation_id(0, 41), 41);
        assert_eq!(pack_correlation_id(7, 0), 7u64 << 48);
        assert_eq!(pack_correlation_id(7, 5), (7u64 << 48) | 5);
        // This round-trips the replica id back out.
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
        // Two replicas that emit the same local sequence must produce
        // distinct ids.
        for seq in [0u64, 1, 99, 1u64 << 40] {
            assert_ne!(pack_correlation_id(1, seq), pack_correlation_id(2, seq));
        }
    }
}
