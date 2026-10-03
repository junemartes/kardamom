//! The payload store client against a fake query endpoint: the range
//! check, the endpoint walk, and the property that matters, that a
//! recovered block packs to the bytes the live block packs to.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_types::kar1::{BlockCursor, BlockHead, BlockRecords, TxFrame, encode_block};
use kardamom_types::{BPosition, TxEnvelope};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::*;
use crate::batcher::{BatcherConfig, pack_blocks};

/// A query endpoint that serves the rows it was given, as the state
/// crate's endpoint does: `null` for a block it does not hold.
pub(crate) struct FakeStore {
    addr: SocketAddr,
    _task: tokio::task::JoinHandle<()>,
}

impl FakeStore {
    pub(crate) async fn serve(rows: BTreeMap<u64, Vec<u8>>) -> Self {
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
                        .map(|bytes| format!("0x{}", alloy_primitives::hex::encode(bytes)));
                    let reply =
                        serde_json::json!({"jsonrpc": "2.0", "id": 1, "result": result}).to_string();
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

fn envelope(seed: u8) -> TxEnvelope {
    TxEnvelope {
        correlation_id: u64::from(seed),
        raw_tx: Bytes::from(vec![seed; 16]),
        sender: Address::repeat_byte(seed),
        tx_hash: B256::repeat_byte(seed),
        max_inclusion_block: 99,
    }
}

/// A live closed block as the feed loop builds it from the sealer.
fn live_block(number: u64, end: u64, txs: u8) -> ClosedBlock {
    ClosedBlock {
        block_number: number,
        l2_timestamp: 1_700_000_000 + number,
        end_tx_idx: BPosition::from_index(end),
        l1_origin: 40 + number,
        remote_epochs: Vec::new(),
        txs: (0..txs)
            .map(|i| RecordedTx {
                position: BPosition::from_index(end - u64::from(txs) + u64::from(i)),
                envelope: envelope(i + 1),
            })
            .collect(),
    }
}

/// The row the state writer stores for `block`.
pub(crate) fn stored_row(block: &ClosedBlock) -> Vec<u8> {
    let head = BlockHead {
        block_number: block.block_number,
        l2_timestamp: block.l2_timestamp,
        cursor: BlockCursor {
            end_tx_idx: block.end_tx_idx.as_index(),
            l1_origin: block.l1_origin,
        },
    };
    let records = BlockRecords {
        remote_epochs: block.remote_epochs.clone(),
        txs: block.txs.iter().map(|t| TxFrame::from(&t.envelope)).collect(),
    };
    encode_block(head, &records).unwrap()
}

fn cursor(next_index: u64, next_block: u64) -> BatchCursor {
    BatchCursor {
        next_index,
        next_block,
        last_batch_index: 3,
    }
}

/// The recovered range packs to the same payload and commitment as the
/// live blocks: the store's bytes are the posted bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovered_range_packs_to_the_live_bytes() {
    let live: Vec<ClosedBlock> = vec![
        live_block(11, 12, 2),
        live_block(12, 12, 0),
        live_block(13, 15, 3),
    ];
    let rows = live
        .iter()
        .map(|b| (b.block_number, stored_row(b)))
        .collect();
    let store = FakeStore::serve(rows).await;
    let client = PayloadStore::new(vec![store.url()]);

    let recovered = client.blocks(cursor(10, 11), 13).await.unwrap();
    assert_eq!(
        recovered.iter().map(|b| b.block_number).collect::<Vec<_>>(),
        vec![11, 12, 13]
    );
    let cfg = BatcherConfig {
        compress: false,
        ..BatcherConfig::default()
    };
    let from_live = pack_blocks(&cfg, &live).unwrap();
    let from_store = pack_blocks(&cfg, &recovered).unwrap();
    assert_eq!(from_store.payload, from_live.payload);
    assert_eq!(from_store.records_commitment, from_live.records_commitment);
    assert_eq!(
        (from_store.l2_block_start, from_store.l2_block_end),
        (11, 13)
    );
    assert_eq!(recovered[2].end_tx_idx.as_index(), 15);
    assert_eq!(recovered[2].l1_origin, 53);
}

/// A block no endpoint holds ends the recovery with its number: the
/// range stays a gap the operator can see.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_block_is_named() {
    let rows = [11, 13]
        .into_iter()
        .map(|n| (n, stored_row(&live_block(n, n, 0))))
        .collect();
    let store = FakeStore::serve(rows).await;
    let client = PayloadStore::new(vec![store.url()]);
    let err = client.blocks(cursor(11, 11), 13).await.unwrap_err();
    assert!(err.to_string().contains("holds block 12"), "{err:#}");
}

/// An end index that does not cover the block's records is a refusal,
/// never a silent skip: the sealer would refuse the resume it names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_end_index_behind_the_records_is_refused() {
    let short = live_block(12, 13, 3);
    let rows = [(11, stored_row(&live_block(11, 12, 0))), (12, stored_row(&short))]
        .into_iter()
        .collect();
    let store = FakeStore::serve(rows).await;
    let client = PayloadStore::new(vec![store.url()]);
    let err = client.blocks(cursor(12, 11), 12).await.unwrap_err();
    assert!(err.to_string().contains("ends at index 13"), "{err:#}");
}

/// The walk skips an endpoint that is down and takes the block from the
/// next one; a floor behind the cursor is refused before any read.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_down_endpoint_is_skipped_and_a_floor_behind_the_cursor_is_refused() {
    let rows = [(11, stored_row(&live_block(11, 11, 0)))]
        .into_iter()
        .collect();
    let store = FakeStore::serve(rows).await;
    let down = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let down_url = format!("http://{}", down.local_addr().unwrap());
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
    // The port closed again when the listener dropped, so the connection
    // is refused.
    let refused_url = format!("http://{unused}");
    let client = PayloadStore::new(vec![refused_url, store.url()]);
    let blocks = client.blocks(cursor(11, 11), 11).await.unwrap();
    assert_eq!(blocks.len(), 1);
    let _ = down_url;

    let err = client.blocks(cursor(20, 20), 11).await.unwrap_err();
    assert!(err.to_string().contains("already at block 20"), "{err:#}");

    let none = PayloadStore::new(Vec::new());
    assert!(none.is_empty());
    let err = none.blocks(cursor(11, 11), 11).await.unwrap_err();
    assert!(err.to_string().contains("holds block 11"), "{err:#}");
}
