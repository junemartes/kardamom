//! The references client against a fake query endpoint: the range
//! check, the endpoint walk, and the refusals.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use alloy_primitives::B256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;

/// A query endpoint that serves the blocks it was given, as the state
/// crate's endpoint does: `null` for a block it does not hold.
pub(crate) struct FakeStore {
    addr: SocketAddr,
    _task: tokio::task::JoinHandle<()>,
}

impl FakeStore {
    pub(crate) async fn serve(rows: BTreeMap<u64, serde_json::Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let rows = Arc::new(rows);
        let task = tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let rows = rows.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = sock.read(&mut buf).await.unwrap();
                    let text = String::from_utf8_lossy(&buf[..n]);
                    let body = text.rsplit("\r\n\r\n").next().unwrap_or("");
                    let request: serde_json::Value = serde_json::from_str(body).unwrap();
                    let number = request["params"][0].as_u64().unwrap();
                    let result = rows
                        .get(&number)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let reply = serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": result})
                        .to_string();
                    let head = format!(
                        "HTTP/1.0 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        reply.len()
                    );
                    sock.write_all(head.as_bytes()).await.unwrap();
                    sock.write_all(reply.as_bytes()).await.unwrap();
                });
            }
        });
        Self { addr, _task: task }
    }

    pub(crate) fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

/// The JSON of a block whose transactions sit at the end of its range,
/// on shard 0, session 7, at archive positions `1000 + tx_idx`.
pub(crate) fn block_json(number: u64, end: u64, txs: u64) -> serde_json::Value {
    let refs: Vec<serde_json::Value> = (0..txs)
        .map(|i| {
            let tx_idx = end - txs + i;
            serde_json::json!({
                "tx_hash": B256::repeat_byte(u8::try_from(tx_idx).unwrap() + 1),
                "tx_idx": tx_idx,
                "shard_id": 0,
                "session_id": 7,
                "position": 1000 + tx_idx,
            })
        })
        .collect();
    serde_json::json!({
        "block_number": number,
        "end_tx_idx": end,
        "l1_origin": 40 + number,
        "l2_timestamp": 1_700_000_000 + number,
        "refs": refs,
    })
}

fn cursor(next_index: u64, next_block: u64) -> BatchCursor {
    BatchCursor {
        next_index,
        next_block,
        last_batch_index: 3,
    }
}

/// A contiguous range comes back in order with its boundaries and its
/// references.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_contiguous_range_is_read_in_order() {
    let rows = [
        (11, block_json(11, 12, 2)),
        (12, block_json(12, 12, 0)),
        (13, block_json(13, 15, 3)),
    ]
    .into_iter()
    .collect();
    let store = FakeStore::serve(rows).await;
    let client = RefsStore::new(vec![store.url()]);
    let blocks = client.blocks(cursor(10, 11), 13).await.unwrap();
    assert_eq!(
        blocks.iter().map(|b| b.block_number).collect::<Vec<_>>(),
        vec![11, 12, 13]
    );
    assert_eq!(blocks[2].end_tx_idx, 15);
    assert_eq!(blocks[2].l1_origin, 53);
    assert_eq!(blocks[2].refs.len(), 3);
    assert_eq!(blocks[2].refs[0].position, 1012);
    assert_eq!(blocks[2].refs[0].session_id, 7);
}

/// A block no endpoint holds ends the read with its number.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_block_is_named() {
    let rows = [(11, block_json(11, 11, 0)), (13, block_json(13, 13, 0))]
        .into_iter()
        .collect();
    let store = FakeStore::serve(rows).await;
    let client = RefsStore::new(vec![store.url()]);
    let err = client.blocks(cursor(11, 11), 13).await.unwrap_err();
    assert!(err.to_string().contains("holds block 12"), "{err:#}");
}

/// An end index that does not cover the block's transactions is a
/// refusal, never a silent skip: the sealer would refuse the resume it
/// names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_end_index_behind_the_transactions_is_refused() {
    let rows = [(11, block_json(11, 12, 0)), (12, block_json(12, 13, 3))]
        .into_iter()
        .collect();
    let store = FakeStore::serve(rows).await;
    let client = RefsStore::new(vec![store.url()]);
    let err = client.blocks(cursor(12, 11), 12).await.unwrap_err();
    assert!(err.to_string().contains("ends at index 13"), "{err:#}");
}

/// The walk skips an endpoint that is down and takes the block from the
/// next one; a floor behind the cursor is refused before any read; no
/// endpoint at all names the block it cannot read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_down_endpoint_is_skipped_and_a_floor_behind_the_cursor_is_refused() {
    let rows = [(11, block_json(11, 11, 0))].into_iter().collect();
    let store = FakeStore::serve(rows).await;
    let unused = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    // The port closed again when the listener dropped, so the connection
    // is refused.
    let client = RefsStore::new(vec![format!("http://{unused}"), store.url()]);
    let blocks = client.blocks(cursor(11, 11), 11).await.unwrap();
    assert_eq!(blocks.len(), 1);

    let err = client.blocks(cursor(20, 20), 11).await.unwrap_err();
    assert!(err.to_string().contains("already at block 20"), "{err:#}");

    let none = RefsStore::new(Vec::new());
    assert!(none.is_empty());
    let err = none.blocks(cursor(11, 11), 11).await.unwrap_err();
    assert!(err.to_string().contains("holds block 11"), "{err:#}");
}
