//! `kardamom_sendRawTransactionAsync` and `kardamom_subscribeReceipts`:
//! fast-ack submission with push receipt delivery.
//!
//! The Eth-compatible `eth_sendRawTransaction` parks its HTTP connection
//! until the receipt arrives. So in-flight txs are about equal to held
//! connections, and the server's connection cap becomes the throughput
//! ceiling. These tests check the decoupled contract: the async submit
//! acks with the tx hash before any receipt exists, and receipts stream,
//! deduped and filterable, over one WebSocket subscription.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use jsonrpsee::core::client::{ClientT, Subscription, SubscriptionClientT};
use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use serde_json::Value;

use kardamom_ingress::channels::MockChannels;
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::json_rpc::start_jsonrpc_server;
use kardamom_ingress::proxy::IngressProxy;
use kardamom_types::{BPosition, Receipt};

use alloy_signer_local::PrivateKeySigner;

const SHARDS: usize = 8;

async fn start_stack() -> (
    MockChannels,
    Vec<tokio::sync::mpsc::UnboundedReceiver<kardamom_types::TxEnvelope>>,
    SocketAddr,
    jsonrpsee::server::ServerHandle,
) {
    let cfg = IngressConfig {
        chain_id: 1,
        ..IngressConfig::default()
    };
    let (mock, rx) = MockChannels::new(SHARDS);
    let proxy = IngressProxy::new(cfg, mock.clone(), mock.clone());
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (local, handle) = start_jsonrpc_server(proxy, bind).await.unwrap();
    (mock, rx, local, handle)
}

fn receipt_for(sender: Address, nonce: u64, tx_hash: B256, idx: i32) -> Receipt {
    Receipt {
        tx_idx: BPosition {
            term_id: 0,
            term_offset: idx,
        },
        tx_hash,
        status: true,
        gas_used: 21_000,
        from: sender,
        nonce,
        ..Default::default()
    }
}

/// With `pending_shed_depth: 0`, a shed-everything test hook, every
/// submission must get the retryable Overloaded error instead of being
/// parked. Overload must become graceful degradation, not a backlog.
#[tokio::test]
async fn overloaded_ingress_sheds_submissions_with_a_clear_error() {
    let cfg = IngressConfig {
        chain_id: 1,
        pending_shed_depth: 0,
        ..IngressConfig::default()
    };
    let (mock, _rx) = MockChannels::new(SHARDS);
    let proxy = IngressProxy::new(cfg, mock.clone(), mock.clone());
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let (local, _handle) = start_jsonrpc_server(proxy, bind).await.unwrap();
    let client = HttpClientBuilder::default()
        .build(format!("http://{local}"))
        .unwrap();

    let signer = PrivateKeySigner::random();
    let (_env, raw, _addr) = common::sign_legacy_tx(&signer, 0);
    let res: Result<B256, _> = client
        .request("kardamom_sendRawTransactionAsync", rpc_params![raw])
        .await;
    let err = res.expect_err("submission must be shed");
    let msg = err.to_string();
    assert!(
        msg.contains("overloaded"),
        "error must name the overload: {msg}"
    );
}

/// The async submit must return the canonical tx hash before any receipt
/// is published. It must not park, unlike `eth_sendRawTransaction`.
#[tokio::test]
async fn async_submit_acks_before_any_receipt_exists() {
    let (_mock, mut rx, local, _handle) = start_stack().await;
    let client = HttpClientBuilder::default()
        .build(format!("http://{local}"))
        .unwrap();

    let signer = PrivateKeySigner::random();
    let (env, raw, _addr) = common::sign_legacy_tx(&signer, 0);

    // A 2s bound is generous for an in-process round trip, and far below
    // the parked path's receipt timeout. A regression to parking fails
    // this test.
    let hash: B256 = tokio::time::timeout(
        Duration::from_secs(2),
        client.request("kardamom_sendRawTransactionAsync", rpc_params![raw]),
    )
    .await
    .expect("async submit must ack without a receipt")
    .expect("submit should succeed");
    assert_eq!(hash, *env.tx_hash(), "ack carries the canonical tx hash");

    // The envelope must be on a tx_data shard.
    let mut published = None;
    for shard_rx in &mut rx {
        if let Ok(e) = shard_rx.try_recv() {
            published = Some(e);
            break;
        }
    }
    let published = published.expect("envelope published to a tx_data shard");
    assert_eq!(published.tx_hash, hash);
}

/// Receipts stream over the subscription exactly once per tx. The raw MDS
/// fan-in delivers up to N replica copies, but subscribers must see only
/// one. The optional sender filter drops receipts from other senders.
#[tokio::test]
async fn subscription_streams_deduped_and_filtered_receipts() {
    let (mock, _rx, local, _handle) = start_stack().await;
    let ws = WsClientBuilder::default()
        .build(format!("ws://{local}"))
        .await
        .unwrap();

    let watched = Address::repeat_byte(0xAA);
    let other = Address::repeat_byte(0xBB);
    let mut sub: Subscription<Value> = ws
        .subscribe(
            "kardamom_subscribeReceipts",
            rpc_params![Some(vec![watched])],
            "kardamom_unsubscribeReceipts",
        )
        .await
        .unwrap();

    // This sends a receipt from another sender, which gets filtered, then
    // a duplicate replica copy around the watched receipt. Only one event
    // must come out for the watched receipt.
    let h_other = B256::repeat_byte(0x01);
    let h_watch = B256::repeat_byte(0x02);
    let h_watch2 = B256::repeat_byte(0x03);
    mock.receipt_bus
        .send(receipt_for(other, 0, h_other, 1))
        .unwrap();
    mock.receipt_bus
        .send(receipt_for(watched, 0, h_watch, 2))
        .unwrap();
    mock.receipt_bus
        .send(receipt_for(watched, 0, h_watch, 2))
        .unwrap(); // Replica duplicate.
    mock.receipt_bus
        .send(receipt_for(watched, 1, h_watch2, 3))
        .unwrap();

    let first = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("first notification")
        .unwrap()
        .unwrap();
    assert_eq!(first["type"], "receipt");
    assert_eq!(
        first["receipt"]["transactionHash"],
        format!("{h_watch:#x}"),
        "foreign-sender receipt must have been filtered out"
    );

    let second = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("second notification")
        .unwrap()
        .unwrap();
    assert_eq!(
        second["receipt"]["transactionHash"],
        format!("{h_watch2:#x}"),
        "duplicate replica copy must not produce a second event"
    );
}

/// Sequencer rejections surface as `txError` frames, so an async
/// submitter learns its tx will never receipt, instead of waiting
/// forever.
#[tokio::test]
async fn subscription_streams_tx_errors() {
    let (mock, _rx, local, _handle) = start_stack().await;
    let ws = WsClientBuilder::default()
        .build(format!("ws://{local}"))
        .await
        .unwrap();
    let mut sub: Subscription<Value> = ws
        .subscribe(
            "kardamom_subscribeReceipts",
            rpc_params![None::<Vec<Address>>],
            "kardamom_unsubscribeReceipts",
        )
        .await
        .unwrap();

    let sender = Address::repeat_byte(0xCC);
    mock.tx_error_bus
        .send(kardamom_types::TxError {
            sender,
            nonce: 7,
            reason: kardamom_types::TxErrorReason::DuplicatedTx { expected_nonce: 9 },
        })
        .unwrap();

    let ev = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("error notification")
        .unwrap()
        .unwrap();
    assert_eq!(ev["type"], "txError");
    assert_eq!(ev["sender"], format!("{sender:#x}"));
    assert_eq!(ev["nonce"], 7);
    assert_eq!(ev["reason"], "duplicated-tx");
    assert_eq!(ev["expectedNonce"], 9);
}
