//! `kardamom_sendRawTransactionAsync` and `kardamom_subscribeReceipts`:
//! fast-ack submission with push receipt delivery.
//!
//! The Eth-compatible `eth_sendRawTransaction` parks its HTTP connection
//! until the receipt arrives. So in-flight txs are about equal to held
//! connections, and the server's connection cap becomes the throughput
//! ceiling. These tests check the decoupled contract: the async submit
//! acks with the tx hash before any receipt exists, and receipts stream,
//! deduped and filterable, over one WebSocket subscription.

use std::num::NonZeroU64;
use std::time::Duration;

use alloy_primitives::{Address, B256};
use jsonrpsee::core::client::{ClientT, Subscription, SubscriptionClientT};
use jsonrpsee::rpc_params;
use jsonrpsee::ws_client::WsClientBuilder;
use serde_json::Value;

use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::test_support::{
    SignedTx, TestServer, http_client, receipt, sign_legacy_tx, start_test_server,
};

use alloy_signer_local::PrivateKeySigner;

const CHAIN_ID: NonZeroU64 = NonZeroU64::new(1).unwrap();

/// `IngressConfig::default()` with `chain_id: 1`, the config every test
/// in this file starts from.
fn chain_one() -> IngressConfig {
    IngressConfig {
        chain_id: CHAIN_ID,
        ..IngressConfig::default()
    }
}

/// Awaits the next subscription frame, with a 5s timeout. `what` names the
/// frame in the panic message on timeout, disconnect, or decode error.
async fn next_event(sub: &mut Subscription<Value>, what: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap_or_else(|| panic!("subscription ended waiting for {what}"))
        .unwrap_or_else(|e| panic!("{what}: {e}"))
}

/// With `pending_shed_depth: 0`, a shed-everything test hook, every
/// submission must get the retryable Overloaded error instead of being
/// parked. Overload must become graceful degradation, not a backlog.
#[tokio::test]
async fn overloaded_ingress_sheds_submissions_with_a_clear_error() {
    let cfg = IngressConfig {
        pending_shed_depth: 0,
        ..chain_one()
    };
    // `_mock`, `_shard_rx`, and `_handle` are bound, not skipped with `..`:
    // an unbound field of a destructured struct drops immediately, and
    // dropping `handle` stops the server before the test can use it.
    let TestServer {
        mock: _mock,
        shard_rx: _shard_rx,
        addr,
        handle: _handle,
    } = start_test_server(cfg).await;
    let client = http_client(addr);

    let signer = PrivateKeySigner::random();
    let SignedTx { raw, .. } = sign_legacy_tx(&signer, 0);
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
    let TestServer {
        mock: _mock,
        mut shard_rx,
        addr,
        handle: _handle,
    } = start_test_server(chain_one()).await;
    let client = http_client(addr);

    let signer = PrivateKeySigner::random();
    let SignedTx { env, raw, .. } = sign_legacy_tx(&signer, 0);

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
    let published = shard_rx
        .iter_mut()
        .find_map(|rx| rx.try_recv().ok())
        .expect("envelope published to a tx_data shard");
    assert_eq!(published.tx_hash, hash);
}

/// Receipts stream over the subscription exactly once per tx. The raw MDS
/// fan-in delivers up to N replica copies, but subscribers must see only
/// one. The optional sender filter drops receipts from other senders.
#[tokio::test]
async fn subscription_streams_deduped_and_filtered_receipts() {
    let TestServer {
        mock,
        shard_rx: _shard_rx,
        addr,
        handle: _handle,
    } = start_test_server(chain_one()).await;
    let ws = WsClientBuilder::default()
        .build(format!("ws://{addr}"))
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
        .send(receipt(other, 0, h_other, 1))
        .unwrap();
    mock.receipt_bus
        .send(receipt(watched, 0, h_watch, 2))
        .unwrap();
    mock.receipt_bus
        .send(receipt(watched, 0, h_watch, 2))
        .unwrap(); // Replica duplicate.
    mock.receipt_bus
        .send(receipt(watched, 1, h_watch2, 3))
        .unwrap();

    let first = next_event(&mut sub, "first notification").await;
    assert_eq!(first["type"], "receipt");
    assert_eq!(
        first["receipt"]["transactionHash"],
        format!("{h_watch:#x}"),
        "foreign-sender receipt must have been filtered out"
    );

    let second = next_event(&mut sub, "second notification").await;
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
    let TestServer {
        mock,
        shard_rx: _shard_rx,
        addr,
        handle: _handle,
    } = start_test_server(chain_one()).await;
    let ws = WsClientBuilder::default()
        .build(format!("ws://{addr}"))
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

    let ev = next_event(&mut sub, "error notification").await;
    assert_eq!(ev["type"], "txError");
    assert_eq!(ev["sender"], format!("{sender:#x}"));
    assert_eq!(ev["nonce"], 7);
    assert_eq!(ev["reason"], "duplicated-tx");
    assert_eq!(ev["expectedNonce"], 9);
}
