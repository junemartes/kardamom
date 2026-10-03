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

    let balance_reply =
        tokio::task::spawn_blocking(move || query(addr, "eth_getBalance", known))
            .await
            .unwrap();
    assert!(
        balance_reply.contains(r#""result":"0x1f4""#),
        "{balance_reply}"
    );

    let unknown = Address::repeat_byte(0x22);
    let unknown_reply =
        tokio::task::spawn_blocking(move || query(addr, "eth_getBalance", unknown))
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

/// The batcher reads a block it can no longer replay from the sealer as
/// the bytes it would have posted: the stored one-block KAR1 payload
/// comes back as hex, and a block outside the window is `null`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serves_the_stored_block_payload_and_null_for_an_unknown_block() {
    use crate::writer::{StateWriter, WriteBatch};
    use kardamom_types::kar1::{BlockRecords, TxFrame, decode};
    use kardamom_types::{BPosition, BlockBoundary, BlockDelta};

    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();
    seed_genesis(&env, &[], &[]).unwrap();
    let mut handle = StateWriter::spawn(env.clone()).unwrap();
    let records = BlockRecords {
        remote_epochs: Vec::new(),
        txs: vec![TxFrame {
            correlation_id: 9,
            sender: Address::repeat_byte(0x11),
            tx_hash: B256::repeat_byte(0xBE),
            raw_tx: bytes::Bytes::from_static(b"raw"),
        }],
    };
    let boundary = BlockBoundary {
        block_number: 1,
        end_tx_idx: BPosition::from_index(2),
        l2_timestamp: 1_700_000_001,
        l1_origin: 4,
    };
    handle
        .delta_tx
        .send(WriteBatch::with_records(
            boundary,
            BlockDelta::default(),
            records.clone(),
        ))
        .unwrap();
    handle.shutdown().unwrap();

    let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env).unwrap();
    let addr = server.addr;
    let ask = move |params: &'static str| {
        post(
            addr,
            &format!(
                r#"{{"jsonrpc":"2.0","id":5,"method":"kardamom_getBlockPayload","params":[{params}]}}"#
            ),
        )
    };

    let reply = tokio::task::spawn_blocking(move || ask("1")).await.unwrap();
    assert!(reply.starts_with("HTTP/1.0 200 OK"), "{reply}");
    assert!(reply.contains("x-state-block: 1\r\n"), "{reply}");
    let body = reply.rsplit("\r\n\r\n").next().unwrap();
    let json: serde_json::Value = serde_json::from_str(body).unwrap();
    let hex = json["result"].as_str().unwrap().strip_prefix("0x").unwrap();
    let payload = decode(&alloy_primitives::hex::decode(hex).unwrap()).unwrap();
    assert_eq!(payload.blocks.len(), 1);
    assert_eq!(payload.blocks[0].block_number, 1);
    assert_eq!(payload.blocks[0].txs, records.txs);
    assert_eq!(payload.blocks[0].cursor.unwrap().end_tx_idx, 2);
    assert_eq!(payload.blocks[0].cursor.unwrap().l1_origin, 4);

    let as_hex = tokio::task::spawn_blocking(move || ask(r#""0x1""#))
        .await
        .unwrap();
    assert!(as_hex.contains(r#""result":"0x"#), "{as_hex}");

    let unknown = tokio::task::spawn_blocking(move || ask("7")).await.unwrap();
    assert!(unknown.contains(r#""result":null"#), "{unknown}");

    let malformed = tokio::task::spawn_blocking(move || ask(r#""one""#))
        .await
        .unwrap();
    assert!(malformed.contains("-32602"), "{malformed}");
}
