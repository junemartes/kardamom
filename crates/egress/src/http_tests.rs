use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};

use crate::owner::{Input, Join};
use crate::test_support::TestEgress;

/// An egress with `frames` stored, on a free port. Returns its address.
async fn egress(dir: &std::path::Path, frames: &[Vec<u8>]) -> std::net::SocketAddr {
    egress_with(dir, frames, Vec::new()).await
}

/// An egress with `frames` stored and `receipts` taken.
async fn egress_with(
    dir: &std::path::Path,
    frames: &[Vec<u8>],
    receipts: Vec<kardamom_log::aeron_live::ReceiptFrame>,
) -> std::net::SocketAddr {
    let egress = TestEgress::spawn(dir, Join::FromPeer).await;
    egress.send_copy(1, 0, frames);
    receipts
        .into_iter()
        .try_for_each(|frame| egress.inputs.send(Input::Receipts(frame)))
        .unwrap();
    egress.addr
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
    let frames = vec![kardamom_log::aeron_live::ReceiptFrame::Batch(batch)];
    let addr = egress_with(dir.path(), &[], frames).await;
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
