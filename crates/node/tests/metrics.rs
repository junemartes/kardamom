//! Integration test: drive a real Node and check the metric registry contains
//! the expected names with non-zero counts.

use std::net::SocketAddr;
use std::sync::OnceLock;

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_rpc_types_eth::BlockNumberOrTag;
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

use kardamom_node::genesis::{AllocEntry, Genesis};
use kardamom_node::metrics as kmetrics;
use kardamom_node::{Node, rpc};

/// One global recorder per test process. Subsequent calls return the same
/// handle so parallel tests don't race on `install_recorder`.
fn recorder() -> &'static PrometheusHandle {
    static H: OnceLock<PrometheusHandle> = OnceLock::new();
    H.get_or_init(|| {
        PrometheusBuilder::new()
            .set_buckets(kmetrics::DURATION_BUCKETS)
            .unwrap()
            .install_recorder()
            .expect("install recorder")
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn rpc_calls_populate_prometheus_registry() {
    let handle = recorder();

    let signer = PrivateKeySigner::random();
    let from = signer.address();
    let to = Address::from([0x22u8; 20]);
    let chain_id = 1;

    let node = Node::new(&Genesis {
        chain_id,
        alloc: [(
            from,
            AllocEntry {
                balance: U256::from(10u64).pow(U256::from(18u64)),
                code: None,
                nonce: 0,
            },
        )]
        .into_iter()
        .collect(),
    });

    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = jsonrpsee::server::Server::builder()
        .build(addr)
        .await
        .unwrap();
    let bound = server.local_addr().unwrap();
    let server_handle = server.start({
        use rpc::EthApiServer;
        rpc::EthHandlers::new(node.clone()).into_rpc()
    });

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", bound))
        .unwrap();

    // chainId — handler-only timing.
    let _: U256 = client.request("eth_chainId", rpc_params![]).await.unwrap();
    // send_raw_transaction — full stage pipeline.
    let mut tx = TxLegacy {
        chain_id: Some(chain_id),
        nonce: 0,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(to),
        value: U256::from(1u64),
        input: Bytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
    let envelope: TxEnvelope = tx.into_signed(sig).into();
    let mut raw = Vec::new();
    envelope.encode_2718(&mut raw);
    let _: alloy_primitives::B256 = client
        .request("eth_sendRawTransaction", rpc_params![Bytes::from(raw)])
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let body = handle.render();
    assert!(
        body.contains(kmetrics::REQUESTS_TOTAL),
        "missing requests_total:\n{body}"
    );
    assert!(
        body.contains(kmetrics::RPC_DURATION_SECONDS),
        "missing rpc_duration:\n{body}"
    );
    assert!(
        body.contains(kmetrics::RPC_STAGE_DURATION_SECONDS),
        "missing stage_duration:\n{body}"
    );
    assert!(
        body.contains(kmetrics::BLOCK_NUMBER),
        "missing block_number:\n{body}"
    );
    assert!(
        body.contains("stage=\"execute\""),
        "no execute stage sample observed:\n{body}",
    );
    assert!(
        body.contains("method=\"eth_sendRawTransaction\""),
        "no sendRawTransaction method label observed:\n{body}",
    );

    let _ = server_handle.stop();
    server_handle.stopped().await;
}

#[tokio::test]
async fn err_outcome_label_appears_on_failed_call() {
    let handle = recorder();

    let node = Node::new(&Genesis {
        chain_id: 1,
        alloc: Default::default(),
    });
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let server = jsonrpsee::server::Server::builder()
        .build(addr)
        .await
        .unwrap();
    let bound = server.local_addr().unwrap();
    let server_handle = server.start({
        use rpc::EthApiServer;
        rpc::EthHandlers::new(node).into_rpc()
    });

    let client = HttpClientBuilder::default()
        .build(format!("http://{}", bound))
        .unwrap();

    let result: Result<U256, _> = client
        .request(
            "eth_getBalance",
            rpc_params![Address::ZERO, BlockNumberOrTag::Number(1)],
        )
        .await;
    assert!(result.is_err(), "non-latest block tag must error");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let body = handle.render();
    assert!(body.contains("outcome=\"err\""), "no err outcome:\n{body}");

    let _ = server_handle.stop();
    server_handle.stopped().await;
}
