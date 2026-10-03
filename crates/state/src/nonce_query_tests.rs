//! Tests of the query endpoint.

use std::io::{Read, Write};

use alloy_primitives::{U256, keccak256};
use kardamom_types::AccountChange;

use super::*;
use crate::env::StateEnvBuilder;
use crate::genesis::seed_genesis;

/// One HTTP/1.0 POST over a plain socket. Returns the whole response.
fn post(addr: SocketAddr, body: &str) -> String {
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write!(
        s,
        "POST / HTTP/1.0\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    out
}

fn query(addr: SocketAddr, method: &str, address: Address) -> String {
    post(
        addr,
        &format!(
            r#"{{"jsonrpc":"2.0","id":5,"method":"{method}","params":["{address}","latest"]}}"#
        ),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_the_committed_nonce_and_balance() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();
    let known = Address::repeat_byte(0x11);
    seed_genesis(
        &env,
        &[AccountChange {
            address: known,
            nonce: 7,
            balance: U256::from(0x1f4u64),
            code_hash: keccak256([]),
        }],
        &[],
    )
    .unwrap();
    let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env).unwrap();
    let addr = server.addr;

    let nonce_reply =
        tokio::task::spawn_blocking(move || query(addr, "eth_getTransactionCount", known))
            .await
            .unwrap();
    assert!(nonce_reply.starts_with("HTTP/1.0 200 OK"), "{nonce_reply}");
    assert!(nonce_reply.contains("x-state-block: "), "{nonce_reply}");
    assert!(
        nonce_reply.contains("x-state-tx-idx: 0\r\n"),
        "{nonce_reply}"
    );
    assert!(nonce_reply.contains(r#""result":"0x7""#), "{nonce_reply}");
    assert!(nonce_reply.contains(r#""id":5"#), "{nonce_reply}");

    let balance_reply = tokio::task::spawn_blocking(move || query(addr, "eth_getBalance", known))
        .await
        .unwrap();
    assert!(
        balance_reply.contains(r#""result":"0x1f4""#),
        "{balance_reply}"
    );

    let unknown = Address::repeat_byte(0x22);
    let unknown_reply = tokio::task::spawn_blocking(move || query(addr, "eth_getBalance", unknown))
        .await
        .unwrap();
    assert!(
        unknown_reply.contains(r#""result":"0x0""#),
        "{unknown_reply}"
    );
}

/// A state DB with one committed block that holds one receipt.
fn env_with_receipt(dir: &std::path::Path, receipt: &Receipt) -> StateEnv {
    use crate::writer::{StateWriter, TrieMode, WriteBatch};
    use kardamom_types::{BPosition, BlockBoundary, BlockDelta};

    let env = StateEnvBuilder::new(dir).open().unwrap();
    seed_genesis(&env, &[], &[]).unwrap();
    let mut handle = StateWriter::spawn_with_trie(env.clone(), TrieMode::Off).unwrap();
    let delta = BlockDelta {
        block_number: 1,
        accounts: vec![],
        storage: vec![],
        code: vec![],
        receipts: vec![receipt.clone()],
    };
    let boundary = BlockBoundary {
        block_number: 1,
        end_tx_idx: BPosition::from_index(receipt.tx_idx.as_index() + 1),
        l2_timestamp: 1_700_000_001,
        l1_origin: 0,
    };
    handle
        .delta_tx
        .send(WriteBatch::new(boundary, delta))
        .unwrap();
    handle.shutdown().unwrap();
    env
}

/// The ingress holds a receipt only in the memory of one process.
/// After a restart it asks here, and the state DB is the durable
/// copy: the stored bytes come back, and an unknown hash is `null`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_the_committed_receipt_by_hash_and_null_for_an_unknown_hash() {
    let receipt = Receipt {
        tx_idx: kardamom_types::BPosition::from_index(4),
        tx_hash: B256::repeat_byte(0xBE),
        status: true,
        gas_used: 21_000,
        nonce: 9,
        from: Address::repeat_byte(0x11),
        block_number: 1,
        ..Receipt::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let env = env_with_receipt(dir.path(), &receipt);
    let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env).unwrap();
    let addr = server.addr;
    let ask = move |hash: B256| {
        post(
            addr,
            &format!(
                r#"{{"jsonrpc":"2.0","id":5,"method":"eth_getTransactionReceipt","params":["{hash}"]}}"#
            ),
        )
    };

    let known = receipt.tx_hash;
    let reply = tokio::task::spawn_blocking(move || ask(known))
        .await
        .unwrap();
    assert!(reply.starts_with("HTTP/1.0 200 OK"), "{reply}");
    assert!(reply.contains("x-state-block: 1\r\n"), "{reply}");
    let stored = alloy_primitives::hex::encode(crate::schema::encode_receipt_value(&receipt));
    assert!(
        reply.contains(&format!(r#""result":"0x{stored}""#)),
        "{reply}"
    );

    let unknown = tokio::task::spawn_blocking(move || ask(B256::repeat_byte(0x01)))
        .await
        .unwrap();
    assert!(unknown.contains(r#""result":null"#), "{unknown}");

    let malformed = tokio::task::spawn_blocking(move || {
        post(
            addr,
            r#"{"jsonrpc":"2.0","id":5,"method":"eth_getTransactionReceipt","params":["0x12"]}"#,
        )
    })
    .await
    .unwrap();
    assert!(malformed.contains("-32602"), "{malformed}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_other_methods_and_bad_input() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();
    let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env).unwrap();
    let addr = server.addr;

    let wrong_method = tokio::task::spawn_blocking(move || {
        post(
            addr,
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":[]}"#,
        )
    })
    .await
    .unwrap();
    assert!(wrong_method.contains(r#""code":-32601"#), "{wrong_method}");

    let no_params = tokio::task::spawn_blocking(move || {
        post(
            addr,
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":[]}"#,
        )
    })
    .await
    .unwrap();
    assert!(no_params.contains(r#""code":-32602"#), "{no_params}");

    let bad_params = tokio::task::spawn_blocking(move || {
        post(
            addr,
            r#"{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["nope"]}"#,
        )
    })
    .await
    .unwrap();
    assert!(bad_params.contains(r#""code":-32602"#), "{bad_params}");

    let not_json = tokio::task::spawn_blocking(move || post(addr, "{{{"))
        .await
        .unwrap();
    assert!(not_json.starts_with("HTTP/1.0 400"), "{not_json}");

    let get = tokio::task::spawn_blocking(move || {
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        s.write_all(b"GET / HTTP/1.0\r\n\r\n").unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        out
    })
    .await
    .unwrap();
    assert!(get.starts_with("HTTP/1.0 404"), "{get}");
}

/// A state DB with two committed blocks. Block 1: one transaction,
/// ending at index 1. Block 2: a deposit at 1, then two transactions at
/// 2 and 3, ending at index 4. Every transaction has an archive
/// reference on session -9.
fn env_with_two_blocks(dir: &std::path::Path) -> StateEnv {
    use crate::writer::{StateWriter, WriteBatch};
    use kardamom_types::receipt::TX_TYPE_DEPOSIT;
    use kardamom_types::{BPosition, BlockBoundary, BlockDelta, TxRef};

    let env = StateEnvBuilder::new(dir).open().unwrap();
    seed_genesis(&env, &[], &[]).unwrap();
    let mut handle = StateWriter::spawn(env.clone()).unwrap();
    let receipt = |idx: u64, hash: u8, tx_type: u8| Receipt {
        tx_idx: BPosition::from_index(idx),
        tx_hash: B256::repeat_byte(hash),
        tx_type,
        block_number: 2,
        ..Receipt::default()
    };
    let first = BlockDelta {
        block_number: 1,
        receipts: vec![Receipt {
            block_number: 1,
            ..receipt(0, 0xA0, 0)
        }],
        ..BlockDelta::default()
    };
    let second = BlockDelta {
        block_number: 2,
        receipts: vec![
            receipt(1, 0xD1, TX_TYPE_DEPOSIT),
            receipt(2, 0xB2, 0),
            receipt(3, 0xB3, 0),
        ],
        ..BlockDelta::default()
    };
    let boundary = |number: u64, end: u64| BlockBoundary {
        block_number: number,
        end_tx_idx: BPosition::from_index(end),
        l2_timestamp: 1_700_000_000 + number,
        l1_origin: 40 + number,
    };
    let tx_ref = |hash: u8, shard: u8, position: u64| {
        TxRef::new(
            B256::repeat_byte(hash),
            shard,
            BPosition::from_index(position),
            -9,
        )
    };
    handle
        .delta_tx
        .send(WriteBatch::with_refs(
            boundary(1, 1),
            first,
            vec![tx_ref(0xA0, 0, 100)],
        ))
        .unwrap();
    handle
        .delta_tx
        .send(WriteBatch::with_refs(
            boundary(2, 4),
            second,
            vec![tx_ref(0xB2, 1, 200), tx_ref(0xB3, 0, 300)],
        ))
        .unwrap();
    handle.shutdown().unwrap();
    env
}

/// The batcher reads a block it can no longer replay from the sealer as
/// references: the block's boundary and, in canonical order, where the
/// bytes of each transaction are. A deposit is not listed. A block not
/// committed yet is `null`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_the_block_references_and_null_for_an_unknown_block() {
    let dir = tempfile::tempdir().unwrap();
    let env = env_with_two_blocks(dir.path());
    let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env).unwrap();
    let addr = server.addr;
    let ask = move |params: &'static str| {
        post(
            addr,
            &format!(
                r#"{{"jsonrpc":"2.0","id":5,"method":"kardamom_getBlockRefs","params":[{params}]}}"#
            ),
        )
    };

    let reply = tokio::task::spawn_blocking(move || ask("2")).await.unwrap();
    assert!(reply.starts_with("HTTP/1.0 200 OK"), "{reply}");
    assert!(reply.contains("x-state-block: 2\r\n"), "{reply}");
    let body = reply.rsplit("\r\n\r\n").next().unwrap();
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    let refs: crate::BlockRefs = serde_json::from_value(json["result"].clone()).unwrap();
    assert_eq!(
        refs,
        crate::BlockRefs {
            block_number: 2,
            end_tx_idx: 4,
            l1_origin: 42,
            l2_timestamp: 1_700_000_002,
            refs: vec![
                crate::BlockTxRef {
                    tx_hash: B256::repeat_byte(0xB2),
                    tx_idx: 2,
                    shard_id: 1,
                    session_id: -9,
                    position: 200,
                },
                crate::BlockTxRef {
                    tx_hash: B256::repeat_byte(0xB3),
                    tx_idx: 3,
                    shard_id: 0,
                    session_id: -9,
                    position: 300,
                },
            ],
        }
    );

    let first = tokio::task::spawn_blocking(move || ask(r#""0x1""#))
        .await
        .unwrap();
    assert!(first.contains(r#""end_tx_idx":1"#), "{first}");
    assert!(first.contains(r#""position":100"#), "{first}");

    let unknown = tokio::task::spawn_blocking(move || ask("7")).await.unwrap();
    assert!(unknown.contains(r#""result":null"#), "{unknown}");

    let malformed = tokio::task::spawn_blocking(move || ask(r#""one""#))
        .await
        .unwrap();
    assert!(malformed.contains("-32602"), "{malformed}");
}

/// A block whose transaction has no archive reference (a row written
/// before the reference existed, or a cross-chain message) cannot be
/// rebuilt from references, and the query says so instead of answering a
/// shorter list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_block_without_references_is_refused_not_shortened() {
    use crate::writer::{StateWriter, WriteBatch};
    use kardamom_types::{BPosition, BlockBoundary, BlockDelta};

    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();
    seed_genesis(&env, &[], &[]).unwrap();
    let mut handle = StateWriter::spawn(env.clone()).unwrap();
    let delta = BlockDelta {
        block_number: 1,
        receipts: vec![Receipt {
            tx_idx: BPosition::from_index(0),
            tx_hash: B256::repeat_byte(0xA0),
            block_number: 1,
            ..Receipt::default()
        }],
        ..BlockDelta::default()
    };
    let boundary = BlockBoundary {
        block_number: 1,
        end_tx_idx: BPosition::from_index(1),
        l2_timestamp: 1,
        l1_origin: 0,
    };
    handle
        .delta_tx
        .send(WriteBatch::new(boundary, delta))
        .unwrap();
    handle.shutdown().unwrap();
    let err = crate::committed_block_refs(&env, 1).unwrap_err();
    assert!(err.to_string().contains("no archive reference"), "{err}");
}
