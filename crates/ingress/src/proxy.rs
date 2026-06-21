//! `IngressProxy`: wires rate-limit, sig-verify, routing, pending-receipts,
//! receipt-cache, and the watermark/block-boundary watchers into a single
//! process.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_primitives::{B256, Bytes as AlloyBytes};
use alloy_rlp::Decodable;
use tokio::sync::broadcast;

use kardamom_types::{BlockBoundary, Receipt, StateDatabase, TxEnvelope, TxError};

use crate::channels::{IngressPublication, IngressSubscription};
use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::pending::{PendingReceipts, ReceiptResponse};
use crate::rate_limit::PerIpLimiter;
use crate::receipt_cache::ReceiptCache;
use crate::routing::partition_for;
use crate::seen_receipts::SeenReceipts;
use crate::sig_verify::BatchVerifier;

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
pub struct IngressProxy<P, S, DB>
where
    P: IngressPublication + Clone,
    S: IngressSubscription + Clone,
    DB: StateDatabase + 'static,
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
    pub(crate) publication: P,
    pub(crate) subscription: S,
    pub(crate) correlation_seq: Arc<AtomicU64>,
    ///highest `BlockBoundary.block_number` observed on
    /// tx_receipts. Read by `eth_blockNumber`. `AtomicU64` is plenty —
    /// monotonic, single-writer (the BlockBoundary watcher), many readers.
    pub(crate) latest_block_number: Arc<AtomicU64>,
    /// State-DB handle reserved for `eth_getBalance` / `eth_getTransactionCount`
    /// (still stubbed pending the S6 reader interface). The receipt path is
    /// now served entirely from the in-memory [`ReceiptCache`].
    pub(crate) state_db: Arc<DB>,
}

impl<P, S, DB> Clone for IngressProxy<P, S, DB>
where
    P: IngressPublication + Clone,
    S: IngressSubscription + Clone,
    DB: StateDatabase + 'static,
{
    fn clone(&self) -> Self {
        Self {
            cfg: self.cfg.clone(),
            rate_limiter: self.rate_limiter.clone(),
            verifier: self.verifier.clone(),
            pending: self.pending.clone(),
            cache: self.cache.clone(),
            seen_receipts: self.seen_receipts.clone(),
            publication: self.publication.clone(),
            subscription: self.subscription.clone(),
            correlation_seq: self.correlation_seq.clone(),
            latest_block_number: self.latest_block_number.clone(),
            state_db: self.state_db.clone(),
        }
    }
}

impl<P, S, DB> IngressProxy<P, S, DB>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
    DB: StateDatabase + 'static,
{
    pub fn new(cfg: IngressConfig, publication: P, subscription: S, state_db: Arc<DB>) -> Self {
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
        let me = Self {
            cfg,
            rate_limiter,
            verifier,
            pending,
            cache,
            seen_receipts,
            publication,
            subscription,
            correlation_seq: Arc::new(AtomicU64::new(0)),
            latest_block_number: Arc::new(AtomicU64::new(0)),
            state_db,
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

    fn spawn_block_boundary_watcher(&self) {
        let rx = self.subscription.subscribe_block_boundaries();
        let latest = self.latest_block_number.clone();
        spawn_broadcast_watcher(rx, move |b: BlockBoundary| {
            let latest = latest.clone();
            // `fetch_max` keeps the counter monotonic without a lock.
            async move {
                latest.fetch_max(b.block_number, Ordering::AcqRel);
            }
        });
    }

    fn spawn_tx_errors_watcher(&self) {
        // The sequencer emits a `TxError` on the tx_errors channel when it
        // rejects an inbound tx (duplicate / past-nonce today). Match it
        // against parked submissions by `(sender, nonce)` and release the
        // client immediately with a JSON-RPC error rather than letting them
        // wait for a receipt that will never arrive.
        let rx = self.subscription.subscribe_tx_errors();
        let pending = self.pending.clone();
        spawn_broadcast_watcher(rx, move |err: TxError| {
            let pending = pending.clone();
            async move {
                pending.on_tx_error(err.sender, err.nonce, err.reason).await;
            }
        });
    }

    fn spawn_tx_receipts_watcher(&self) {
        // The enriched `Receipt` carries `from` + `nonce` + `tx_hash` directly,
        // so a single tx_receipts subscription is enough to populate both the
        // (sender, nonce) retry-dedup index and the tx_hash → Receipt index
        // used by `eth_getTransactionReceipt`. It also drives client release
        // through `pending.on_receipt`.
        //
        // MDS FAN-IN DEDUP (first-wins by tx hash): with the multi-destination
        // subscription, ALL N executor replicas replay the same canonical order
        // and emit IDENTICAL receipts, so each receipt arrives up to N times on
        // this stream. We dedup by `tx_hash` here so a tx's must-deliver ack
        // fires exactly once: the first receipt for a hash is processed, every
        // later copy is dropped before it reaches `pending`/`cache`. (Both
        // `pending.on_receipt` — keyed by (sender, nonce), removed on resolve —
        // and `cache.insert` are already individually idempotent/panic-free, so
        // this set is the explicit, cheap first line that also avoids redundant
        // work; it is a no-op in the single-executor IPC path.)
        let rx = self.subscription.subscribe_receipts();
        let pending = self.pending.clone();
        let cache = self.cache.clone();
        let seen = self.seen_receipts.clone();
        spawn_broadcast_watcher(rx, move |receipt: Receipt| {
            let pending = pending.clone();
            let cache = cache.clone();
            let seen = seen.clone();
            async move {
                // First-wins: `insert` returns false if the hash was already
                // present, i.e. this is a duplicate replica copy → drop it.
                if !seen.insert(receipt.tx_hash) {
                    metrics::counter!(crate::metrics::RECEIPT_DUPLICATE_TOTAL).increment(1);
                    return;
                }
                let sender = receipt.from;
                let nonce = receipt.nonce;
                cache.insert(receipt.clone());
                pending.on_receipt(sender, nonce, receipt).await;
            }
        });
    }

    fn spawn_quorum_watermark_watcher(&self) {
        let rx = self.subscription.subscribe_watermark();
        let pending = self.pending.clone();
        spawn_broadcast_watcher(rx, move |w| {
            let pending = pending.clone();
            async move { pending.update_quorum_watermark(w).await }
        });
    }

    fn spawn_local_fsync_watermark_watcher(&self) {
        let rx = self.subscription.subscribe_local_fsync_watermark();
        let pending = self.pending.clone();
        spawn_broadcast_watcher(rx, move |w| {
            let pending = pending.clone();
            async move { pending.update_local_watermark(w).await }
        });
    }

    /// Hot path for both JSON-RPC and binary submissions. Returns the
    /// receipt once both `(sender, nonce, receipt)` and the quorum watermark
    /// are satisfied.
    pub async fn submit_raw(
        &self,
        client_ip: IpAddr,
        raw_tx: AlloyBytes,
    ) -> Result<ReceiptResponse, IngressError> {
        metrics::counter!(crate::metrics::TX_RECEIVED_TOTAL).increment(1);

        if let Err(e) = self.rate_limiter.check(client_ip) {
            let _ = e; // unit error
            metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "rate-limited")
                .increment(1);
            return Err(IngressError::RateLimited(client_ip.to_string()));
        }

        let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref()).map_err(|e| {
            metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "decode-error")
                .increment(1);
            IngressError::Decode(e.to_string())
        })?;
        let nonce = env.nonce();

        //: the proxy is the *only* place `sender` and
        // `tx_hash` are computed. Both fields are stamped into the envelope
        // before any downstream consumer observes the tx, and the sig-verify
        // failure path returns *before* we publish to Aeron.
        let (sender, tx_hash) = self.verifier.recover(env, raw_tx.clone()).await.map_err(
            |e| {
                if matches!(e, IngressError::SignatureInvalid) {
                    metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "signature-invalid")
                        .increment(1);
                } else {
                    metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "internal")
                        .increment(1);
                }
                e
            },
        )?;

        if let Some(prev) = self.cache.lookup(sender, nonce) {
            // A resubmission served from the receipt cache succeeds; count it
            // so received == accepted + rejected holds on every path.
            metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
            return Ok(ReceiptResponse { receipt: prev });
        }

        // Park *before* publishing — the receipt can arrive on the cache
        // channel before we'd otherwise have registered, especially under load.
        let wait = self.pending.register(sender, nonce);

        // Update queue depth after parking.
        metrics::gauge!(crate::metrics::QUEUE_DEPTH).set(self.pending.len() as f64);

        // Publish onto tx_data[shard]. The shard is selected by sender-
        // address hash (`partition_for(sender, K)`) so every tx from a given
        // sender lands on the same shard's A stream, which lets the P
        // sequencers per shard nonce-order them consistently. The envelope
        // carries the canonical `tx_hash` so downstream consumers can dedup
        // and re-emit it without recomputing (S0).
        let shard = partition_for(sender, self.cfg.partition_count_m) as usize;
        let correlation_id = self.next_correlation_id();
        self.publication
            .publish_tx_data(
                shard,
                TxEnvelope {
                    correlation_id,
                    raw_tx: raw_tx.0.clone(),
                    sender,
                    tx_hash,
                },
            )
            .await
            .inspect_err(|_| {
                metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "partition-unavailable")
                    .increment(1);
            })?;

        let result = wait
            .await_with_timeout(self.cfg.pending_receipt_timeout)
            .await;

        // Update queue depth after the wait completes (slot removed on receipt or timeout).
        metrics::gauge!(crate::metrics::QUEUE_DEPTH).set(self.pending.len() as f64);

        // Count accepted/rejected on the terminal outcome (not at publish
        // time) so a single submission never increments both.
        match &result {
            Ok(_) => {
                metrics::counter!(crate::metrics::TX_ACCEPTED_TOTAL).increment(1);
            }
            Err(IngressError::Timeout) => {
                metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "timeout")
                    .increment(1);
            }
            Err(IngressError::Duplicate(_)) => {
                metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "duplicate")
                    .increment(1);
            }
            Err(_) => {
                metrics::counter!(crate::metrics::TX_REJECTED_TOTAL, "reason" => "internal")
                    .increment(1);
            }
        }

        result
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
        DB: 'static,
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
