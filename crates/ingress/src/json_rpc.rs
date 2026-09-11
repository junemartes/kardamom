//! JSON-RPC server over HTTP and WebSocket, through jsonrpsee.
//!
//! The method set is the minimal v0 Ethereum subset:
//! - `eth_chainId`.
//! - `eth_blockNumber`, served from the `tx_receipts` `BlockBoundary`
//!   watcher in the proxy.
//! - `eth_sendRawTransaction`.
//! - `eth_getTransactionReceipt`, a state-DB `tx_hash_index` lookup.
//! - `eth_getBalance` and `eth_getTransactionCount` return a clear error,
//!   "deferred to S6 state writer," instead of "method not found."

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use alloy_primitives::{Address, B256, Bytes, Log, LogData, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionReceipt};
use jsonrpsee::core::{RpcResult, SubscriptionResult};
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{PendingSubscriptionSink, Server, ServerHandle, SubscriptionSink};
use jsonrpsee::types::ErrorObjectOwned;
use tokio::sync::broadcast;

use crate::channels::{IngressPublication, IngressSubscription, ProxyBackend};
use crate::error::IngressError;
use crate::proxy::IngressProxy;
use kardamom_types::{Receipt, TxError};

tokio::task_local! {
    /// Set by the HTTP middleware for the lifetime of each request.
    pub(crate) static PEER_ADDR: std::cell::Cell<Option<IpAddr>>;
}

/// Reads the request's peer IP from the [`PEER_ADDR`] task-local, set by
/// [`peer_addr_layer`], falling back to loopback if unset — for example
/// in unit tests that use a custom transport with no peer-addr
/// middleware.
fn client_ip() -> IpAddr {
    PEER_ADDR
        .try_with(std::cell::Cell::get)
        .ok()
        .flatten()
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
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
/// carries three frame kinds: an executed-tx receipt, a sequencer
/// rejection, meaning the tx will never receipt, and a lag marker, sent
/// when the subscriber fell further behind than the feed buffer. The
/// client must then recover the gap by polling
/// `eth_getTransactionReceipt`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ReceiptEvent {
    /// A tx executed, and its enriched receipt was observed on `tx_receipts`.
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
    /// The subscriber lagged. `skipped` feed items were dropped.
    #[serde(rename_all = "camelCase")]
    Lagged { skipped: u64 },
}

/// Kardamom-native extensions: fast-ack submission and push receipt
/// delivery. The Eth-compatible `eth_sendRawTransaction` parks the caller
/// until the receipt arrives, which costs one held connection per
/// in-flight tx. This namespace decouples the two, so any number of txs
/// can be in flight over a single connection.
#[rpc(server, namespace = "kardamom")]
pub trait IngressKardamomApi {
    /// Fast-ack submission. Validates, publishes to `tx_data`, and returns
    /// the canonical tx hash right away. The client observes delivery
    /// through `kardamom_subscribeReceipts` or
    /// `eth_getTransactionReceipt`.
    #[method(name = "sendRawTransactionAsync")]
    async fn send_raw_transaction_async(&self, bytes: Bytes) -> RpcResult<B256>;

    /// Push feed of deduped receipts and sequencer rejections, WebSocket
    /// only. `senders` filters to the given addresses. `None` or `[]`
    /// streams everything.
    #[subscription(name = "subscribeReceipts", item = ReceiptEvent)]
    async fn subscribe_receipts(&self, senders: Option<Vec<Address>>) -> SubscriptionResult;
}

pub(crate) struct IngressHandlers<Backend: ProxyBackend> {
    proxy: IngressProxy<Backend::Pub, Backend::Sub>,
}

impl<Backend: ProxyBackend> IngressHandlers<Backend> {
    #[must_use]
    pub(crate) fn new(proxy: IngressProxy<Backend::Pub, Backend::Sub>) -> Self {
        Self { proxy }
    }
}

#[async_trait::async_trait]
impl<Backend: ProxyBackend> IngressEthApiServer for IngressHandlers<Backend> {
    async fn chain_id(&self) -> RpcResult<U256> {
        Ok(U256::from(self.proxy.config().chain_id.get()))
    }

    async fn block_number(&self) -> RpcResult<U256> {
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
        let res = self
            .proxy
            .submit_raw(client_ip(), bytes)
            .await
            .map_err(ErrorObjectOwned::from)?;
        Ok(res.receipt.tx_hash)
    }

    async fn transaction_receipt(&self, hash: B256) -> RpcResult<Option<TransactionReceipt>> {
        // This is served from the in-memory `ReceiptCache`, populated
        // off the tx_receipts stream, since the ingress holds no state
        // DB. Returns `null`, by JSON-RPC convention, if not yet
        // committed.
        Ok(self
            .proxy
            .lookup_receipt_by_hash(hash)
            .map(|r| RpcReceipt::from(&r).0))
    }
}

#[async_trait::async_trait]
impl<Backend: ProxyBackend> IngressKardamomApiServer for IngressHandlers<Backend> {
    async fn send_raw_transaction_async(&self, bytes: Bytes) -> RpcResult<B256> {
        self.proxy
            .submit_raw_async(client_ip(), bytes)
            .await
            .map_err(ErrorObjectOwned::from)
    }

    async fn subscribe_receipts(
        &self,
        pending: PendingSubscriptionSink,
        senders: Option<Vec<Address>>,
    ) -> SubscriptionResult {
        // This taps the feeds before accepting, so nothing published in
        // between is missed.
        let receipts = self.proxy.subscribe_receipt_feed();
        let errors = self.proxy.subscribe_tx_error_feed();
        let sink = pending.accept().await?;
        let filter = SenderFilter(
            senders
                .filter(|s| !s.is_empty())
                .map(|s| s.into_iter().collect()),
        );
        ReceiptSubscription {
            receipts,
            errors,
            filter,
            sink,
        }
        .run()
        .await?;
        Ok(())
    }
}

/// A sender allow-list for `kardamom_subscribeReceipts`. `None` streams
/// every sender.
struct SenderFilter(Option<std::collections::HashSet<Address>>);

impl SenderFilter {
    fn allows(&self, addr: Address) -> bool {
        self.0.as_ref().is_none_or(|f| f.contains(&addr))
    }
}

/// One feed poll's outcome, for [`ReceiptSubscription::next_event`]'s
/// `select!` arms: an event to send, a filtered-out item to skip, or the
/// feed closing, which ends the subscription.
enum NextEvent {
    Event(ReceiptEvent),
    FilteredOut,
    FeedClosed,
}

/// Owns one `kardamom_subscribeReceipts` session end to end: the two
/// upstream feeds, the sender filter, and the sink the events go out on.
struct ReceiptSubscription {
    receipts: broadcast::Receiver<Receipt>,
    errors: broadcast::Receiver<TxError>,
    filter: SenderFilter,
    sink: SubscriptionSink,
}

impl ReceiptSubscription {
    /// Forwards events to the sink until the sink closes, a feed
    /// disconnects, or the subscriber goes away.
    async fn run(mut self) -> Result<(), String> {
        while let Some(event) = self.next_event().await {
            if !self.send_event(&event).await? {
                break; // The subscriber went away.
            }
        }
        Ok(())
    }

    /// Waits for the next receipt or tx-error frame, applying the
    /// filter, or a lag marker if either feed fell behind. Returns
    /// `None` once the sink closes or a feed disconnects, which ends the
    /// subscription.
    async fn next_event(&mut self) -> Option<ReceiptEvent> {
        loop {
            let next = tokio::select! {
                () = self.sink.closed() => return None,
                r = self.receipts.recv() => self.on_receipt_result(r),
                e = self.errors.recv() => self.on_error_result(e),
            };
            match next {
                NextEvent::Event(event) => return Some(event),
                NextEvent::FilteredOut => {}
                NextEvent::FeedClosed => return None,
            }
        }
    }

    /// One `receipts` feed poll, for [`Self::next_event`]'s `select!` arm.
    fn on_receipt_result(&self, r: Result<Receipt, broadcast::error::RecvError>) -> NextEvent {
        match r {
            Ok(rcpt) if !self.filter.allows(rcpt.from) => NextEvent::FilteredOut,
            Ok(rcpt) => NextEvent::Event(ReceiptEvent::Receipt {
                receipt: Box::new(RpcReceipt::from(&rcpt).0),
            }),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                NextEvent::Event(ReceiptEvent::Lagged { skipped: n })
            }
            Err(broadcast::error::RecvError::Closed) => NextEvent::FeedClosed,
        }
    }

    /// One `errors` feed poll, for [`Self::next_event`]'s `select!` arm.
    fn on_error_result(&self, e: Result<TxError, broadcast::error::RecvError>) -> NextEvent {
        match e {
            Ok(err) if !self.filter.allows(err.sender) => NextEvent::FilteredOut,
            Ok(err) => {
                let (reason, expected_nonce) = describe_tx_error(&err.reason);
                NextEvent::Event(ReceiptEvent::TxError {
                    sender: err.sender,
                    nonce: err.nonce,
                    reason,
                    expected_nonce,
                })
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                NextEvent::Event(ReceiptEvent::Lagged { skipped: n })
            }
            Err(broadcast::error::RecvError::Closed) => NextEvent::FeedClosed,
        }
    }

    /// Serializes and sends one event to the subscriber. Returns
    /// `Ok(false)` if the subscriber went away, so the caller ends the
    /// loop instead of treating a closed sink as an error.
    async fn send_event(&self, event: &ReceiptEvent) -> Result<bool, String> {
        let msg = serde_json::value::to_raw_value(event)
            .map_err(|e| format!("serialize subscription event: {e}"))?;
        Ok(self.sink.send(msg).await.is_ok())
    }
}

/// Human- and machine-readable description of a sequencer rejection for
/// the subscription stream. This match is exhaustive on purpose: a new
/// `TxErrorReason` variant must decide its wire shape here.
fn describe_tx_error(reason: &kardamom_types::TxErrorReason) -> (String, Option<u64>) {
    match reason {
        kardamom_types::TxErrorReason::DuplicatedTx { expected_nonce } => {
            ("duplicated-tx".to_string(), Some(*expected_nonce))
        }
        kardamom_types::TxErrorReason::Evicted { expected_nonce } => {
            ("evicted".to_string(), Some(*expected_nonce))
        }
        kardamom_types::TxErrorReason::Expired { expected_nonce } => {
            ("expired".to_string(), Some(*expected_nonce))
        }
    }
}

/// Adapts the internal `kardamom_types::Receipt` to alloy's
/// `TransactionReceipt`. The internal type carries the canonical
/// B-position and `write_set_hash`, which the public Eth API does not
/// need. The executor populates the fields; ingress only reshapes them.
///
/// `block_hash` stays `None` in v0. The slim `BlockBoundary` has no
/// state commitment, so there is no meaningful hash to return. JSON-RPC
/// allows `null` here.
///
/// A thin newtype, rather than a bare `TransactionReceipt`, because
/// `kardamom_types::Receipt` and `alloy_rpc_types_eth::TransactionReceipt`
/// are both foreign to this crate: the orphan rule blocks `impl
/// From<&Receipt> for TransactionReceipt` directly, but allows it for a
/// local wrapper type.
struct RpcReceipt(TransactionReceipt);

impl From<&kardamom_types::Receipt> for RpcReceipt {
    fn from(r: &kardamom_types::Receipt) -> Self {
        let logs: Vec<alloy_rpc_types_eth::Log> = r
            .logs
            .iter()
            .enumerate()
            .map(|(log_index, wl)| alloy_rpc_types_eth::Log {
                inner: Log {
                    address: wl.address,
                    data: LogData::new_unchecked(
                        wl.topics.clone(),
                        alloy_primitives::Bytes::copy_from_slice(wl.data.as_ref()),
                    ),
                },
                block_hash: None,
                block_number: Some(r.block_number),
                block_timestamp: None,
                transaction_hash: Some(r.tx_hash),
                transaction_index: Some(r.transaction_index),
                // `usize` to `u64` is widening on every supported target.
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
        // Maps the tx's real EIP-2718 type to alloy's `ReceiptEnvelope`.
        // Bridge tooling identifies deposits by the 0x7E byte.
        let inner = match r.tx_type {
            kardamom_types::TX_TYPE_LEGACY => {
                alloy_rpc_types_eth::ReceiptEnvelope::Legacy(with_bloom)
            }
            0x01 => alloy_rpc_types_eth::ReceiptEnvelope::Eip2930(with_bloom),
            0x02 => alloy_rpc_types_eth::ReceiptEnvelope::Eip1559(with_bloom),
            0x03 => alloy_rpc_types_eth::ReceiptEnvelope::Eip4844(with_bloom),
            // Deposits (0x7E) have no alloy-eth envelope variant; the OP
            // type lives in op-alloy. Legacy is the closest structural
            // carrier. The authoritative deposit marker for kardamom
            // consumers is `Receipt::is_deposit()` on the native stream.
            _ => alloy_rpc_types_eth::ReceiptEnvelope::Legacy(with_bloom),
        };
        Self(TransactionReceipt {
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
        })
    }
}

/// Starts the jsonrpsee server. Returns the bound `SocketAddr` and a
/// `ServerHandle`; dropping the handle shuts the server down. An HTTP
/// middleware extracts the peer IP and stores it in the `PEER_ADDR`
/// task-local for the duration of each request.
///
/// # Errors
///
/// Returns `IngressError::Internal` if the server fails to bind, or if
/// the two RPC modules fail to merge.
pub async fn start_jsonrpc_server<P, S>(
    proxy: IngressProxy<P, S>,
    addr: SocketAddr,
) -> Result<(SocketAddr, ServerHandle), IngressError>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
{
    // A parked submit_raw holds its connection until the receipt arrives.
    // So the connection cap, not the handler, becomes the throughput
    // limit as soon as it is smaller than the offered rate times the
    // receipt latency. This takes the cap from config instead of
    // jsonrpsee's default of 100; see
    // `IngressConfig::rpc_max_connections`.
    let server_cfg = jsonrpsee::server::ServerConfig::builder()
        .max_connections(proxy.cfg.rpc_max_connections)
        .build();
    let server = Server::builder()
        .set_config(server_cfg)
        .set_http_middleware(tower::ServiceBuilder::new().layer(peer_addr_layer::PeerAddrLayer))
        .build(addr)
        .await
        .map_err(|e| IngressError::internal("jsonrpsee bind", e))?;
    let local = server
        .local_addr()
        .map_err(|e| IngressError::internal("local_addr", e))?;
    let mut module = IngressEthApiServer::into_rpc(IngressHandlers::<(P, S)>::new(proxy.clone()));
    module
        .merge(IngressKardamomApiServer::into_rpc(
            IngressHandlers::<(P, S)>::new(proxy),
        ))
        .map_err(|e| IngressError::internal("rpc module merge", e))?;
    Ok((local, server.start(module)))
}

/// Tiny tower layer. It pulls the peer's `SocketAddr`, set on the
/// request's `extensions` by jsonrpsee's HTTP transport, and stores its
/// IP in the [`PEER_ADDR`] task-local for the lifetime of the request. If
/// the extension is missing, for example in unit tests with custom
/// transports, the handler falls back to loopback.
mod peer_addr_layer {
    use std::net::{IpAddr, Ipv4Addr};
    use std::task::{Context, Poll};

    use tower::{Layer, Service};

    use super::PEER_ADDR;

    #[derive(Clone, Default)]
    pub(super) struct PeerAddrLayer;

    impl<S> Layer<S> for PeerAddrLayer {
        type Service = PeerAddrService<S>;
        fn layer(&self, inner: S) -> Self::Service {
            PeerAddrService { inner }
        }
    }

    #[derive(Clone)]
    pub(super) struct PeerAddrService<S> {
        inner: S,
    }

    /// Every `tower::Service` this layer wraps, collapsed to one bound.
    trait HttpService<Body>: Service<hyper::Request<Body>> + Clone + Send + 'static
    where
        Self::Future: Send + 'static,
    {
    }

    impl<S, Body> HttpService<Body> for S
    where
        S: Service<hyper::Request<Body>> + Clone + Send + 'static,
        S::Future: Send + 'static,
        Body: Send + 'static,
    {
    }

    impl<S, Body> Service<hyper::Request<Body>> for PeerAddrService<S>
    where
        S: HttpService<Body>,
        // `HttpService`'s own `where Self::Future: Send` bound applies to
        // its implementors, but is not implied back at every use site, so
        // this restates it.
        S::Future: Send + 'static,
        Body: Send + 'static,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future =
            tokio::task::futures::TaskLocalFuture<std::cell::Cell<Option<IpAddr>>, S::Future>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, req: hyper::Request<Body>) -> Self::Future {
            let ip: IpAddr = req
                .extensions()
                .get::<std::net::SocketAddr>()
                .map_or(IpAddr::V4(Ipv4Addr::LOCALHOST), std::net::SocketAddr::ip);
            // `Service::call` traditionally requires the cloned inner
            // ready service. This is the standard tower idiom.
            let clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, clone);
            PEER_ADDR.scope(std::cell::Cell::new(Some(ip)), inner.call(req))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::IngressConfig;
    use crate::test_support::{TestServer, http_client, start_test_server};
    use jsonrpsee::core::client::ClientT;
    use jsonrpsee::rpc_params;

    #[tokio::test]
    async fn chain_id_round_trips() {
        let cfg = IngressConfig {
            chain_id: std::num::NonZeroU64::new(31337).unwrap(),
            ..IngressConfig::default()
        };
        let TestServer { addr, handle, .. } = start_test_server(cfg).await;
        let client = http_client(addr);
        let id: U256 = client.request("eth_chainId", rpc_params![]).await.unwrap();
        assert_eq!(id, U256::from(31337u64));
        handle.stop().unwrap();
    }
}
