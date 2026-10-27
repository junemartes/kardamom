use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::{B256, hex, keccak256};
use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use jsonrpsee::types::ErrorObjectOwned;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::*;

const ACCOUNT: u32 = 3;
const NO_QUORUM: &str = "chain halted: sealer_no_quorum at sealer (no status frame for 14 s)";

enum Reply {
    Refuse(usize, ErrorObjectOwned),
    WrongHash,
    MissingReceipt,
    FailedReceipt,
    Slow,
}

#[derive(Debug)]
enum Event {
    Nonce,
    Submit(String),
    Receipt(String),
}

struct Scenario {
    reply: Reply,
    calls: AtomicUsize,
    events: mpsc::UnboundedSender<Event>,
}

impl Scenario {
    async fn delay(&self) {
        if matches!(self.reply, Reply::Slow) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn nonce(&self) -> String {
        self.events.send(Event::Nonce).unwrap();
        self.delay().await;
        "0x5".into()
    }

    async fn submit(&self, raw: String) -> Result<String, ErrorObjectOwned> {
        self.events.send(Event::Submit(raw.clone())).unwrap();
        self.delay().await;
        let count = self.calls.fetch_add(1, Ordering::SeqCst);
        match &self.reply {
            Reply::Refuse(limit, error) if count < *limit => Err(error.clone()),
            Reply::WrongHash => Ok(format!("{:#x}", B256::ZERO)),
            _ => Ok(format!("{:#x}", keccak256(hex::decode(raw).unwrap()))),
        }
    }

    async fn receipt(&self, hash: String) -> Value {
        self.events.send(Event::Receipt(hash.clone())).unwrap();
        self.delay().await;
        match self.reply {
            Reply::MissingReceipt => Value::Null,
            Reply::FailedReceipt => json!({"transactionHash":hash,"status":"0x0"}),
            _ => json!({"transactionHash":hash,"status":"0x1"}),
        }
    }
}

struct Fixture {
    rpc: Rpc,
    handle: ServerHandle,
    events: mpsc::UnboundedReceiver<Event>,
}

impl Fixture {
    async fn new(reply: Reply) -> Self {
        let (events, received) = mpsc::unbounded_channel();
        let mut module = RpcModule::new(Scenario {
            reply,
            calls: AtomicUsize::new(0),
            events,
        });
        module
            .register_async_method("eth_getTransactionCount", |_, scenario, _| async move {
                scenario.nonce().await
            })
            .unwrap();
        module
            .register_async_method("eth_sendRawTransaction", |params, scenario, _| async move {
                scenario.submit(params.one::<String>()?).await
            })
            .unwrap();
        module
            .register_async_method(
                "eth_getTransactionReceipt",
                |params, scenario, _| async move {
                    Ok::<_, ErrorObjectOwned>(scenario.receipt(params.one::<String>()?).await)
                },
            )
            .unwrap();
        let server = Server::builder().build("127.0.0.1:0").await.unwrap();
        let rpc = Rpc::new(&format!("http://{}", server.local_addr().unwrap()), 1).unwrap();
        let handle = server.start(module);
        Self {
            rpc,
            handle,
            events: received,
        }
    }

    fn drain(&mut self) -> Vec<Event> {
        std::iter::from_fn(|| self.events.try_recv().ok()).collect()
    }

    fn refusal(count: usize, code: i32, message: &str) -> Reply {
        Reply::Refuse(count, ErrorObjectOwned::owned::<()>(code, message, None))
    }
}

#[tokio::test]
async fn no_quorum_retries_identical_signed_bytes_and_requires_their_receipt() {
    let mut fixture = Fixture::new(Fixture::refusal(2, -32010, NO_QUORUM)).await;
    fixture
        .rpc
        .transfer_within(ACCOUNT, Duration::from_secs(5))
        .await
        .unwrap();
    let events = fixture.drain();
    assert_eq!(
        events.iter().filter(|e| matches!(e, Event::Nonce)).count(),
        1
    );
    let raw: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            Event::Submit(raw) => Some(raw),
            _ => None,
        })
        .collect();
    assert_eq!(raw.len(), 3);
    assert!(raw.iter().all(|r| *r == raw[0]));
    let bytes = hex::decode(raw[0]).unwrap();
    assert_eq!(
        TxEnvelope::decode_2718(&mut bytes.as_slice())
            .unwrap()
            .nonce(),
        5
    );
    let expected = format!("{:#x}", keccak256(bytes));
    assert!(matches!(events.last(), Some(Event::Receipt(hash)) if hash == &expected));
    fixture.handle.stop().unwrap();
}

#[tokio::test]
async fn another_halt_or_rpc_refusal_fails_without_retry() {
    for (code, message) in [
        (-32010, "chain halted: da_lag at sealer (lag 10)"),
        (
            -32010,
            "chain halted: sealer_no_quorum at executor (wrong root)",
        ),
        (-32000, NO_QUORUM),
        (-32602, "invalid transaction"),
    ] {
        let mut fixture = Fixture::new(Fixture::refusal(usize::MAX, code, message)).await;
        let error = fixture
            .rpc
            .transfer_within(ACCOUNT, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(message), "{error:#}");
        assert_eq!(
            fixture
                .drain()
                .iter()
                .filter(|e| matches!(e, Event::Submit(_)))
                .count(),
            1
        );
        fixture.handle.stop().unwrap();
    }
}

#[tokio::test]
async fn a_persistent_no_quorum_refusal_consumes_only_the_original_deadline() {
    let mut fixture = Fixture::new(Fixture::refusal(usize::MAX, -32010, NO_QUORUM)).await;
    let error = fixture
        .rpc
        .transfer_within(ACCOUNT, Duration::from_millis(100))
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("bound"));
    assert!(
        !fixture
            .drain()
            .iter()
            .any(|e| matches!(e, Event::Receipt(_)))
    );
    fixture.handle.stop().unwrap();
}

#[tokio::test]
async fn nonce_submit_and_receipt_delays_share_one_budget() {
    let fixture = Fixture::new(Reply::Slow).await;
    let error = fixture
        .rpc
        .transfer_within(ACCOUNT, Duration::from_millis(250))
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("bound"));
    fixture.handle.stop().unwrap();
}

#[tokio::test]
async fn a_wrong_submit_hash_or_unsuccessful_receipt_cannot_pass() {
    for reply in [
        Reply::WrongHash,
        Reply::MissingReceipt,
        Reply::FailedReceipt,
    ] {
        let fixture = Fixture::new(reply).await;
        assert!(
            fixture
                .rpc
                .transfer_within(ACCOUNT, Duration::from_millis(200))
                .await
                .is_err()
        );
        fixture.handle.stop().unwrap();
    }
}
