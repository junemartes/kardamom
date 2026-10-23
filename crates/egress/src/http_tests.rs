use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};

use super::Http;
use crate::drops::Drops;
use crate::merge::{Head, Merge, MergeConfig};
use crate::owner::{FramePublisher, Input, Owner, OwnerConfig, Parts};
use crate::receipts::Receipts;
use crate::store::Store;

struct NoLive;

impl FramePublisher for NoLive {
    fn publish(&self, _frame: &[u8]) {}
}

/// An egress with `frames` stored, on a free port. Returns its address.
async fn egress(dir: &std::path::Path, frames: &[Vec<u8>]) -> std::net::SocketAddr {
    let inputs = frames
        .iter()
        .zip(0u64..)
        .map(|(frame, seq)| Input::Frame {
            session: 1,
            message: [seq.to_le_bytes().as_slice(), frame].concat(),
        })
        .collect();
    egress_inputs(dir, inputs).await
}

/// An egress that took `taken` before its first read.
async fn egress_inputs(dir: &std::path::Path, taken: Vec<Input>) -> std::net::SocketAddr {
    let genesis = Head {
        next_index: 0,
        next_block: 1,
        last: None,
    };
    let store = Store::open(dir, NonZeroU64::new(10).unwrap(), genesis).unwrap();
    let merge = Merge::new(
        MergeConfig {
            max_waiting: NonZeroUsize::new(64).unwrap(),
            compared: NonZeroUsize::new(64).unwrap(),
        },
        store.head(),
    );
    let owner = Owner::new(
        OwnerConfig {
            sync_every: Duration::from_millis(10),
            keep_blocks: NonZeroU64::new(1_000).unwrap(),
        },
        Parts {
            merge,
            store,
            receipts: Receipts::new(NonZeroUsize::new(16).unwrap()),
            drops: Drops::new(NonZeroUsize::new(16).unwrap()),
            live: NoLive,
            receipts_live: NoLive,
        },
    );
    let (inputs, rx) = crossbeam_channel::bounded(64);
    taken
        .into_iter()
        .try_for_each(|input| inputs.send(input))
        .unwrap();
    std::thread::spawn(move || owner.run(&rx));
    let http = Http::bind("127.0.0.1:0".parse().unwrap(), inputs)
        .await
        .unwrap();
    let addr = http.local_addr().unwrap();
    tokio::spawn(http.serve());
    addr
}

/// Split a body of `[len:u32][frame]` records.
fn frames_of(mut body: &[u8]) -> Vec<Vec<u8>> {
    std::iter::from_fn(|| {
        let (len, rest) = body.split_first_chunk::<4>()?;
        let (frame, rest) = rest.split_at(usize::try_from(u32::from_le_bytes(*len)).ok()?);
        body = rest;
        Some(frame.to_vec())
    })
    .collect()
}

#[tokio::test]
async fn a_range_read_answers_the_frames_after_the_cursor_and_the_head() {
    let dir = tempfile::tempdir().unwrap();
    let frames = vec![
        encode_egress_record(0, &[1; 40]).unwrap(),
        encode_egress_boundary(1, 1, 0, 0),
        encode_egress_record(1, &[2; 40]).unwrap(),
    ];
    let addr = egress(dir.path(), &frames).await;
    let response = reqwest::get(format!("http://{addr}/v1/frames?from_index=1&from_block=1"))
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let header = |name: &str| response.headers()[name].to_str().unwrap().to_owned();
    assert_eq!(header("x-egress-next-index"), "2");
    assert_eq!(header("x-egress-next-block"), "2");
    assert_eq!(header("x-egress-after-index"), "2");
    assert_eq!(header("x-egress-after-block"), "2");
    let body = response.bytes().await.unwrap();
    assert_eq!(frames_of(&body), frames[1..]);
}

#[tokio::test]
async fn a_bad_query_is_400_and_health_is_ok() {
    let dir = tempfile::tempdir().unwrap();
    let addr = egress(dir.path(), &[]).await;
    let bad = reqwest::get(format!("http://{addr}/v1/frames?from_index=x"))
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    let health = reqwest::get(format!("http://{addr}/health")).await.unwrap();
    assert_eq!(health.status(), 200);
}

/// A client of the executor query reads a receipt from the egress with no
/// change: the same call, the same answer.
#[tokio::test]
async fn the_receipt_call_answers_as_the_executor_query_does() {
    let dir = tempfile::tempdir().unwrap();
    let receipt = kardamom_types::Receipt {
        tx_hash: alloy_primitives::B256::repeat_byte(4),
        block_number: 7,
        tx_idx: kardamom_types::BPosition::from_index(3),
        ..kardamom_types::Receipt::default()
    };
    let batch = kardamom_types::ReceiptBatch {
        receipts: vec![receipt.clone()],
        accounts: Vec::new(),
    };
    let frame = kardamom_log::aeron_live::ReceiptFrame::Batch(batch);
    let addr = egress_inputs(dir.path(), vec![Input::Receipts(frame)]).await;
    let client = reqwest::Client::new();
    let ask = |hash| {
        client
            .post(format!("http://{addr}/"))
            .body(kardamom_cache::query::receipt_request_body(hash))
            .send()
    };
    let found = ask(receipt.tx_hash).await.unwrap();
    assert_eq!(found.headers()["x-state-block"], "7");
    let body = found.text().await.unwrap();
    assert_eq!(
        kardamom_cache::query::parse_receipt_answer(&body).unwrap(),
        Some(receipt)
    );
    let missing = ask(alloy_primitives::B256::ZERO)
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(
        kardamom_cache::query::parse_receipt_answer(&missing).unwrap(),
        None
    );
}

/// The status call answers each of the three states, and the receipt call
/// answers `null` for a dropped hash.
#[tokio::test]
async fn the_status_call_answers_executed_dropped_and_unknown() {
    use alloy_primitives::B256;
    let dir = tempfile::tempdir().unwrap();
    let executed = kardamom_types::Receipt {
        tx_hash: B256::repeat_byte(4),
        block_number: 7,
        tx_idx: kardamom_types::BPosition::from_index(3),
        ..kardamom_types::Receipt::default()
    };
    let batch = kardamom_types::ReceiptBatch {
        receipts: vec![executed.clone()],
        accounts: Vec::new(),
    };
    let dropped = kardamom_types::TxDrop {
        tx_hash: B256::repeat_byte(5),
        sender: alloy_primitives::Address::repeat_byte(0xA1),
        nonce: 9,
        reason: kardamom_types::DropReason::NonceTooLow,
    };
    let addr = egress_inputs(
        dir.path(),
        vec![
            Input::Receipts(kardamom_log::aeron_live::ReceiptFrame::Batch(batch)),
            Input::Drop(dropped.clone()),
        ],
    )
    .await;
    let client = reqwest::Client::new();
    let call = |method: &'static str, hash: B256| {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": method, "params": [hash],
        });
        let request = client.post(format!("http://{addr}/")).json(&body).send();
        async move {
            let answer: serde_json::Value = request.await.unwrap().json().await.unwrap();
            answer["result"].clone()
        }
    };
    let status = "kardamom_getTransactionStatus";
    assert_eq!(
        call(status, executed.tx_hash).await,
        serde_json::json!({"status": "executed"})
    );
    assert_eq!(
        call(status, dropped.tx_hash).await,
        serde_json::json!({
            "status": "dropped",
            "reason": "nonce too low",
            "sender": dropped.sender,
            "nonce": "0x9",
        })
    );
    assert_eq!(
        call(status, B256::ZERO).await,
        serde_json::json!({"status": "unknown"})
    );
    assert_eq!(
        call("eth_getTransactionReceipt", dropped.tx_hash).await,
        serde_json::Value::Null
    );
}
