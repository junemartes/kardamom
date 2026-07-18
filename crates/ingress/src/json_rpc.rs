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
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;

use kardamom_types::StateDatabase;

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

pub struct IngressHandlers<P, S, DB>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
    DB: StateDatabase + 'static,
{
    proxy: IngressProxy<P, S, DB>,
}

impl<P, S, DB> IngressHandlers<P, S, DB>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
    DB: StateDatabase + 'static,
{
    pub fn new(proxy: IngressProxy<P, S, DB>) -> Self {
        Self { proxy }
    }
}

#[async_trait::async_trait]
impl<P, S, DB> IngressEthApiServer for IngressHandlers<P, S, DB>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
    DB: StateDatabase + 'static,
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
        //state-DB `tx_hash_index` lookup. S6 will own the
        // libmdbx-backed impl; v0 + tests use `InMemoryStateDb`. Returns
        // `null` per JSON-RPC convention if not yet committed.
        Ok(self.proxy.lookup_receipt_by_hash(hash).map(receipt_to_rpc))
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
    TransactionReceipt {
        inner: alloy_rpc_types_eth::ReceiptEnvelope::Legacy(alloy_consensus::ReceiptWithBloom {
            receipt: alloy_consensus::Receipt {
                status: alloy_consensus::Eip658Value::Eip658(r.status),
                cumulative_gas_used: r.cumulative_gas_used,
                logs,
            },
            logs_bloom,
        }),
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
pub async fn start_jsonrpc_server<P, S, DB>(
    proxy: IngressProxy<P, S, DB>,
    addr: SocketAddr,
) -> Result<(SocketAddr, ServerHandle), IngressError>
where
    P: IngressPublication + Clone + 'static,
    S: IngressSubscription + Clone + 'static,
    DB: StateDatabase + 'static,
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
    let module = IngressHandlers::new(proxy).into_rpc();
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
    use crate::channels::{InMemoryStateDb, MockChannels};
    use crate::config::IngressConfig;
    use crate::proxy::IngressProxy;
    use jsonrpsee::core::client::ClientT;
    use jsonrpsee::http_client::HttpClientBuilder;
    use jsonrpsee::rpc_params;
    use std::sync::Arc;

    #[tokio::test]
    async fn chain_id_round_trips() {
        let cfg = IngressConfig {
            chain_id: 31337,
            ..IngressConfig::default()
        };
        let (mock, _rx) = MockChannels::new(8);
        let state_db = Arc::new(InMemoryStateDb::new());
        let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);
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
