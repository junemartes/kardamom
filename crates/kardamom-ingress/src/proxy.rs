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
use bytes::Bytes;
use tokio::sync::broadcast;

use kardamom_types::{BPosition, BlockBoundary, CachedReceipt, Receipt, StateDatabase, TxEnvelope};

use crate::channels::{IngressPublication, IngressSubscription};
use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::pending::{PendingReceipts, ReceiptResponse};
use crate::rate_limit::PerIpLimiter;
use crate::receipt_cache::ReceiptCache;
use crate::routing::partition_for;
use crate::sig_verify::BatchVerifier;

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
    pub(crate) publication: P,
    pub(crate) subscription: S,
    pub(crate) correlation_seq: Arc<AtomicU64>,
    /// Per S0 D-Sh5: highest `BlockBoundary.block_number` observed on
    /// channel C. Read by `eth_blockNumber`. `AtomicU64` is plenty —
    /// monotonic, single-writer (the BlockBoundary watcher), many readers.
    pub(crate) latest_block_number: Arc<AtomicU64>,
    /// Per S0 D-Sh4: state-DB handle for `eth_getTransactionReceipt(hash)`
    /// via `get_tx_position(tx_hash)` → `get_receipt(position)`. S6 owns the
    /// `tx_hash_index` libmdbx table. v0 + tests use the in-memory impl in
    /// [`crate::channels::InMemoryStateDb`].
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
        let pending = Arc::new(PendingReceipts::new());
        let cache = Arc::new(ReceiptCache::new(cfg.receipt_cache_capacity));
        cache.spawn_consumer(&subscription);
        let me = Self {
            cfg,
            rate_limiter,
            verifier,
            pending,
            cache,
            publication,
            subscription,
            correlation_seq: Arc::new(AtomicU64::new(0)),
            latest_block_number: Arc::new(AtomicU64::new(0)),
            state_db,
        };
        me.spawn_receipt_cache_watcher();
        me.spawn_watermark_watcher();
        me.spawn_block_boundary_watcher();
        me
    }

    /// Highest `BlockBoundary.block_number` observed on channel C. Backs
    /// `eth_blockNumber` per S0 D-Sh5.
    #[inline]
    pub fn latest_block_number(&self) -> u64 {
        self.latest_block_number.load(Ordering::Acquire)
    }

    /// Lookup a receipt by `tx_hash` via the state-DB `tx_hash_index` table
    /// per S0 D-Sh4. Returns `None` if the tx has not yet been committed.
    pub fn lookup_receipt_by_hash(&self, tx_hash: B256) -> Option<Receipt> {
        let pos = self.state_db.get_tx_position(tx_hash).ok().flatten()?;
        self.state_db.get_receipt(pos).ok().flatten()
    }

    fn spawn_block_boundary_watcher(&self) {
        let mut rx: broadcast::Receiver<BlockBoundary> =
            self.subscription.subscribe_block_boundaries();
        let latest = self.latest_block_number.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(b) => {
                        // Monotonic: never go backwards. `fetch_max` is
                        // portable and lock-free.
                        latest.fetch_max(b.block_number, Ordering::AcqRel);
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }

    fn spawn_receipt_cache_watcher(&self) {
        // The executor publishes `CachedReceipt { sender, nonce, tx_hash,
        // receipt }` on the receipt-cache channel. This is the proxy's
        // authoritative `(sender, nonce, receipt)` binding for client
        // release (channel C alone carries `tx_hash` + `tx_idx` but not
        // sender; receipt-cache is the join).
        let mut rx: broadcast::Receiver<CachedReceipt> =
            self.subscription.subscribe_receipt_cache();
        let pending = self.pending.clone();
        let cache = self.cache.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(c) => {
                        cache.insert(c.sender, c.nonce, c.receipt.clone());
                        pending.on_receipt(c.sender, c.nonce, c.receipt).await;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
        });
    }

    fn spawn_watermark_watcher(&self) {
        let mut rx = self.subscription.subscribe_watermark();
        let pending = self.pending.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(w) => pending.update_watermark(w).await,
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                }
            }
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
        // 1. Rate limit (cheap reject).
        self.rate_limiter
            .check(client_ip)
            .map_err(|_| IngressError::RateLimited(client_ip.to_string()))?;

        // 2. Decode envelope (NOT yet sig-verified).
        let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref())
            .map_err(|e| IngressError::Decode(e.to_string()))?;
        let nonce = env.nonce();

        // 3. Batched sig verify — produces (sender, tx_hash) together.
        //    Per S0 D-Sh3 + D-Sh4 the proxy is the *only* place either field
        //    is computed. Failure here returns
        //    IngressError::SignatureInvalid → JSON-RPC -32602 BEFORE we
        //    publish to Aeron.
        let (sender, tx_hash) = self.verifier.recover(env, raw_tx.clone()).await?;

        // 4. Cache lookup — idempotent retry?
        if let Some(prev) = self.cache.lookup(sender, nonce) {
            return Ok(ReceiptResponse { receipt: prev });
        }

        // 5. Park *before* publishing — the receipt could arrive on the
        //    cache channel before we'd otherwise have registered, especially
        //    under high load.
        let wait = self.pending.register(sender, nonce);

        // 6. Publish to partition. The envelope carries the canonical
        //    `tx_hash` so the executor copies it straight into
        //    `Receipt.tx_hash` (S0 D-Sh4 — no recomputation downstream).
        let partition = partition_for(sender, self.cfg.partition_count_m) as usize;
        let correlation_id = self.correlation_seq.fetch_add(1, Ordering::Relaxed);
        // Convert from alloy_primitives::Bytes (newtype around bytes::Bytes)
        // to the bare bytes::Bytes that `TxEnvelope` carries.
        let raw_tx_bytes: Bytes = raw_tx.0.clone();
        self.publication
            .publish_ingress(
                partition,
                TxEnvelope {
                    correlation_id,
                    raw_tx: raw_tx_bytes,
                    sender,
                    tx_hash,
                },
            )
            .await?;

        // 7. Wait on the parked oneshot with timeout.
        wait.await_with_timeout(self.cfg.pending_receipt_timeout)
            .await
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

    /// Latest quorum watermark observed (returned via the pending map's
    /// internal cache). Returns `None` until the first snapshot arrives.
    pub async fn latest_watermark(&self) -> Option<BPosition> {
        // Re-implemented here rather than exposing PendingReceipts internals;
        // we use the proxy's own watch by spawning a one-shot consumer for
        // tests / debugging. For v0 we just return None until needed —
        // proxies don't otherwise surface the watermark.
        None
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
