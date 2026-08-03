//! JSON-RPC server over HTTP and WebSocket via jsonrpsee.
//!
//! Method set is the minimal v0 Ethereum subset:
//! - `eth_chainId`
//! - `eth_blockNumber` (served from the tx_receipts `BlockBoundary` watcher
//!   in the proxy)
//! - `eth_sendRawTransaction`
//! - `eth_getTransactionReceipt` (state-DB `tx_hash_index` lookup)
//! - `eth_getBalance` / `eth_getTransactionCount` — return a clear error
//!   ("deferred to S6 state writer") rather than "method not found".

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use alloy_primitives::{Address, B256, Bytes, Log, LogData, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionReceipt};
use jsonrpsee::core::{RpcResult, SubscriptionResult};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{PendingSubscriptionSink, Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use tokio::sync::broadcast;

use crate::channels::{IngressPublication, IngressSubscription};
use crate::error::IngressError;
use crate::proxy::IngressProxy;

tokio::task_local! {
    /// Set by the HTTP middleware for the lifetime of each request.
    pub(crate) static PEER_ADDR: std::cell::Cell<Option<IpAddr>>;
}

#[rpc(server, namespace = "eth")]
pub trait IngressEthApi {
    #[method(name = "chainId")]
    async fn chain_id(&self) -> RpcResult<U256>;

    #[method(name = "blockNumber")]
    async fn block_number(&self) -> RpcResult<U256>;

    #[method(name = "getBalance")]
    async fn balance(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256>;

    #[method(name = "getTransactionCount")]
    async fn nonce(&self, addr: Address, block: BlockNumberOrTag) -> RpcResult<U256>;

    #[method(name = "sendRawTransaction")]
    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256>;

    #[method(name = "getTransactionReceipt")]
    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>>;
}

/// Event stream payload for `kardamom_subscribeReceipts`. One subscription
/// carries three frame kinds: executed-tx receipts, sequencer rejections
/// (the tx will never receipt), and a lag marker emitted when the subscriber
/// fell further behind than the feed buffer — the gap must then be recovered
/// by polling `eth_getTransactionReceipt`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReceiptEvent {
    /// A tx executed; its enriched receipt was observed on tx_receipts.
    #[serde(rename_all = "camelCase")]
    Receipt { receipt: Box<TransactionReceipt> },
    /// The sequencer rejected the tx.
    #[serde(rename_all = "camelCase")]
    TxError {
        sender: Address,
        nonce: u64,
        reason: String,
        expected_nonce: Option<u64>,
    },
    /// The subscriber lagged; `skipped` feed items were dropped.
    #[serde(rename_all = "camelCase")]
    Lagged { skipped: u64 },
}

/// Kardamom-native extensions: fast-ack submission + push receipt delivery.
/// The Eth-compatible `eth_sendRawTransaction` parks the caller until the
/// receipt arrives, which costs one held connection per in-flight tx; this
/// namespace decouples the two so any number of txs can be in flight over a
/// single connection.
#[rpc(server, namespace = "kardamom")]
pub trait IngressKardamomApi {
    /// Fast-ack submission: validates, publishes to tx_data, and returns the
    /// canonical tx hash immediately. Delivery is observed via
    /// `kardamom_subscribeReceipts` or `eth_getTransactionReceipt`.
    #[method(name = "sendRawTransactionAsync")]
    async fn send_raw_transaction_async(&self, bytes: Bytes) -> RpcResult<B256>;

    /// Push feed of deduped receipts and sequencer rejections (WebSocket
    /// only). `senders` filters to the given addresses; `None` or `[]`
    /// streams everything.
    #[subscription(name = "subscribeReceipts", item = ReceiptEvent)]
    async fn subscribe_receipts(&self, senders: Option<Vec<Address>>) -> SubscriptionResult;
}

pub struct IngressHandlers<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    proxy: IngressProxy<P, S>,
}

impl<P, S> IngressHandlers<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    pub fn new(proxy: IngressProxy<P, S>) -> Self {
        Self { proxy }
    }
}

#[async_trait::async_trait]
impl<P, S> IngressEthApiServer for IngressHandlers<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    async fn chain_id(&self) -> RpcResult<U256> {
        Ok(U256::from(self.proxy.config().chain_id))
    }

    async fn block_number(&self) -> RpcResult<U256> {
        //the proxy's tx_receipts watcher (spawned in
        // `IngressProxy::new`) maintains `latest_block_number: AtomicU64`.
        Ok(U256::from(self.proxy.latest_block_number()))
    }

    async fn balance(&self, _addr: Address, _block: BlockNumberOrTag) -> RpcResult<U256> {
        Err(ErrorObjectOwned::from(IngressError::Internal(
            "eth_getBalance deferred to S6 state writer".into(),
        )))
    }

    async fn nonce(&self, _addr: Address, _block: BlockNumberOrTag) -> RpcResult<U256> {
        Err(ErrorObjectOwned::from(IngressError::Internal(
            "eth_getTransactionCount deferred to S6 state writer".into(),
        )))
    }

    async fn send_raw_transaction(&self, bytes: Bytes) -> RpcResult<B256> {
        let client_ip = PEER_ADDR
            .try_with(|c| c.get())
            .ok()
            .flatten()
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        let res = self
            .proxy
            .submit_raw(client_ip, bytes)
            .await
            .map_err(ErrorObjectOwned::from)?;
        Ok(res.receipt.tx_hash)
    }

    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>> {
        // Served from the in-memory `ReceiptCache` (populated off the
        // tx_receipts stream) — the ingress holds no state DB. Returns
        // `null` per JSON-RPC convention if not yet committed.
        Ok(self.proxy.lookup_receipt_by_hash(hash).map(receipt_to_rpc))
    }
}

#[async_trait::async_trait]
impl<P, S> IngressKardamomApiServer for IngressHandlers<P, S>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    async fn send_raw_transaction_async(&self, bytes: Bytes) -> RpcResult<B256> {
        let client_ip = PEER_ADDR
            .try_with(|c| c.get())
            .ok()
            .flatten()
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        self.proxy
            .submit_raw_async(client_ip, bytes)
            .await
            .map_err(ErrorObjectOwned::from)
    }

    async fn subscribe_receipts(
        &self,
        pending: PendingSubscriptionSink,
        senders: Option<Vec<Address>>,
    ) -> SubscriptionResult {
        // Tap the feeds *before* accepting so nothing published in between
        // is missed.
        let mut receipts = self.proxy.subscribe_receipt_feed();
        let mut errors = self.proxy.subscribe_tx_error_feed();
        let sink = pending.accept().await?;
        let filter: Option<std::collections::HashSet<Address>> = senders
            .filter(|s| !s.is_empty())
            .map(|s| s.into_iter().collect());

        loop {
            let event = tokio::select! {
                () = sink.closed() => break,
                r = receipts.recv() => match r {
                    Ok(rcpt) => {
                        if filter.as_ref().is_some_and(|f| !f.contains(&rcpt.from)) {
                            continue;
                        }
                        ReceiptEvent::Receipt {
                            receipt: Box::new(receipt_to_rpc(rcpt)),
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        ReceiptEvent::Lagged { skipped: n }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
                e = errors.recv() => match e {
                    Ok(err) => {
                        if filter.as_ref().is_some_and(|f| !f.contains(&err.sender)) {
                            continue;
                        }
                        let (reason, expected_nonce) = describe_tx_error(&err.reason);
                        ReceiptEvent::TxError {
                            sender: err.sender,
                            nonce: err.nonce,
                            reason,
                            expected_nonce,
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        ReceiptEvent::Lagged { skipped: n }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                },
            };
            let msg = serde_json::value::to_raw_value(&event)
                .map_err(|e| format!("serialize subscription event: {e}"))?;
            if sink.send(msg).await.is_err() {
                break; // subscriber went away
            }
        }
        Ok(())
    }
}

/// Human/machine-readable description of a sequencer rejection for the
/// subscription stream. Exhaustive on purpose: a new `TxErrorReason` variant
/// must decide its wire shape here.
fn describe_tx_error(reason: &kardamom_types::TxErrorReason) -> (String, Option<u64>) {
    match reason {
        kardamom_types::TxErrorReason::DuplicatedTx { expected_nonce } => {
            ("duplicated-tx".to_string(), Some(*expected_nonce))
        }
        kardamom_types::TxErrorReason::Evicted { expected_nonce } => {
            ("evicted".to_string(), Some(*expected_nonce))
        }
    }
}

/// Adapter from our internal `kardamom_types::Receipt` to alloy's
/// `TransactionReceipt`. The internal type carries the canonical B-position
/// and `write_set_hash` that the public Eth API does not need; everything
/// else is now populated by the executor at execution time and ingress just
/// reshapes the fields.
///
/// `block_hash` stays `None` in v0: the slim `BlockBoundary` has no state
/// commitment, so there is no meaningful hash to return. JSON-RPC permits
/// `null` here.
fn receipt_to_rpc(r: kardamom_types::Receipt) -> TransactionReceipt {
    let block_number = r.block_number;
    let logs: Vec<alloy_rpc_types_eth::Log> = r
        .logs
        .into_iter()
        .enumerate()
        .map(|(log_index, wl)| alloy_rpc_types_eth::Log {
            inner: Log {
                address: wl.address,
                data: LogData::new_unchecked(
                    wl.topics,
                    alloy_primitives::Bytes::copy_from_slice(wl.data.as_ref()),
                ),
            },
            block_hash: None,
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(r.tx_hash),
            transaction_index: Some(r.transaction_index),
            log_index: Some(log_index as u64),
            removed: false,
        })
        .collect();
    let logs_bloom = alloy_primitives::logs_bloom(logs.iter().map(|l| &l.inner));
    let with_bloom = alloy_consensus::ReceiptWithBloom {
        receipt: alloy_consensus::Receipt {
            status: alloy_consensus::Eip658Value::Eip658(r.status),
            cumulative_gas_used: r.cumulative_gas_used,
            logs,
        },
        logs_bloom,
    };
    // Report the tx's REAL EIP-2718 type. This was hardcoded to Legacy, so
    // every deposit surfaced to clients as a type-0x00 transaction —
    // bridge tooling identifies deposits by the 0x7E byte.
    let inner = match r.tx_type {
        kardamom_types::TX_TYPE_LEGACY => alloy_rpc_types_eth::ReceiptEnvelope::Legacy(with_bloom),
        0x01 => alloy_rpc_types_eth::ReceiptEnvelope::Eip2930(with_bloom),
        0x02 => alloy_rpc_types_eth::ReceiptEnvelope::Eip1559(with_bloom),
        0x03 => alloy_rpc_types_eth::ReceiptEnvelope::Eip4844(with_bloom),
        // Deposits (0x7E) have no alloy-eth envelope variant — the OP type
        // lives in op-alloy. Legacy is the closest structural carrier; the
        // authoritative deposit marker for kardamom consumers is
        // `Receipt::is_deposit()` on the native stream.
        _ => alloy_rpc_types_eth::ReceiptEnvelope::Legacy(with_bloom),
    };
    TransactionReceipt {
        inner,
        transaction_hash: r.tx_hash,
        transaction_index: Some(r.transaction_index),
        block_hash: None,
        block_number: Some(r.block_number),
        gas_used: r.gas_used,
        effective_gas_price: r.effective_gas_price,
        blob_gas_used: None,
        blob_gas_price: None,
        from: r.from,
        to: r.to,
        contract_address: r.contract_address,
    }
}

/// Start the jsonrpsee server. Returns the bound `SocketAddr` and a
/// `ServerHandle` whose drop shuts the server down. An HTTP middleware
/// extracts the peer IP and stores it in the `PEER_ADDR` task-local for the
/// duration of each request.
pub async fn start_jsonrpc_server<P, S>(
    proxy: IngressProxy<P, S>,
    addr: SocketAddr,
) -> Result<(SocketAddr, ServerHandle), IngressError>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    // A parked submit_raw holds its connection until the receipt arrives, so
    // the connection cap — not the handler — becomes the throughput limit the
    // moment it is smaller than offered-rate × receipt-latency. Take it from
    // config instead of jsonrpsee's default (100); see
    // `IngressConfig::rpc_max_connections`.
    let server_cfg = jsonrpsee::server::ServerConfig::builder()
        .max_connections(proxy.cfg.rpc_max_connections)
        .build();
    let server = Server::builder()
        .set_config(server_cfg)
        .set_http_middleware(tower::ServiceBuilder::new().layer(peer_addr_layer::PeerAddrLayer))
        .build(addr)
        .await
        .map_err(|e| IngressError::Internal(format!("jsonrpsee bind: {e}")))?;
    let local = server
        .local_addr()
        .map_err(|e| IngressError::Internal(format!("local_addr: {e}")))?;
    let mut module = IngressEthApiServer::into_rpc(IngressHandlers::new(proxy.clone()));
    module
        .merge(IngressKardamomApiServer::into_rpc(IngressHandlers::new(
            proxy,
        )))
        .map_err(|e| IngressError::Internal(format!("rpc module merge: {e}")))?;
    Ok((local, server.start(module)))
}

/// Tiny tower layer that pulls the peer's `SocketAddr` (set on the
/// request's `extensions` by jsonrpsee's HTTP transport) and stores its IP
/// in the [`PEER_ADDR`] task-local for the lifetime of the request. If the
/// extension is missing — e.g. unit-tests with custom transports — the
/// handler falls back to loopback.
mod peer_addr_layer {
    use std::future::Future;
    use std::net::{IpAddr, Ipv4Addr};
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tower::{Layer, Service};

    use super::PEER_ADDR;

    #[derive(Clone, Default)]
    pub struct PeerAddrLayer;

    impl<S> Layer<S> for PeerAddrLayer {
        type Service = PeerAddrService<S>;
        fn layer(&self, inner: S) -> Self::Service {
            PeerAddrService { inner }
        }
    }

    #[derive(Clone)]
    pub struct PeerAddrService<S> {
        inner: S,
    }

    impl<S, Body> Service<hyper::Request<Body>> for PeerAddrService<S>
    where
        S: Service<hyper::Request<Body>> + Clone + Send + 'static,
        S::Future: Send + 'static,
        Body: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
            let ip: IpAddr = req
                .extensions()
                .get::<std::net::SocketAddr>()
                .map(|s| s.ip())
                .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
            // `Service::call` traditionally requires that we use the cloned
            // inner ready service — the standard tower idiom.
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);
            Box::pin(async move {
                PEER_ADDR
                    .scope(std::cell::Cell::new(Some(ip)), inner.call(req))
                    .await
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::MockChannels;
    use crate::config::IngressConfig;
    use crate::proxy::IngressProxy;
    use jsonrpsee::core::client::ClientT;
    use jsonrpsee::http_client::HttpClientBuilder;
    use jsonrpsee::rpc_params;

    #[tokio::test]
    async fn chain_id_round_trips() {
        let cfg = IngressConfig {
            chain_id: 31337,
            ..IngressConfig::default()
        };
        let (mock, _rx) = MockChannels::new(8);
        let proxy = IngressProxy::new(cfg, mock.clone(), mock);
        let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let (local, handle) = start_jsonrpc_server(proxy, bind).await.unwrap();
        let client = HttpClientBuilder::default()
            .build(format!("http://{local}"))
            .unwrap();
        let id: U256 = client.request("eth_chainId", rpc_params![]).await.unwrap();
        assert_eq!(id, U256::from(31337u64));
        handle.stop().unwrap();
    }
}
