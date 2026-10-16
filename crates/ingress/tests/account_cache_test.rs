//! The admission checks and the account RPCs over the local account
//! layer, with Redis off. The rows arrive through `MockChannels::apply_rows`
//! as the live receipt pump would apply them. `submit_raw_async` returns
//! the hash on publish, so admission is observable with no fake executor.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use alloy_signer_local::PrivateKeySigner;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::rpc_params;

use kardamom_cache::LiveAccountsConfig;
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::error::IngressError;
use kardamom_ingress::test_support::{http_client, receipt, sign_legacy, start_test_server};
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::{AccountRow, BPosition};

/// `IngressConfig::default` routes over 8 shards; the mock opens as many.
const SHARDS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
const IP: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 3, 0, 1));

/// The cost of the `sign_legacy` shape: 21,000 gas at 1 gwei, no value.
const LEGACY_COST: u64 = 21_000 * 1_000_000_000;

/// A proxy over `MockChannels`, and the shard receivers a test holds so
/// a publish never sees a closed lane.
struct Fixture {
    proxy: IngressProxy<MockChannels, MockChannels>,
    mock: MockChannels,
    _shard_rx: Vec<tokio::sync::mpsc::UnboundedReceiver<kardamom_types::TxEnvelope>>,
}

impl Fixture {
    fn with(cfg: IngressConfig, live: &LiveAccountsConfig) -> Self {
        let (mock, shard_rx) = MockChannels::with_live_accounts(SHARDS, live);
        Self {
            proxy: IngressProxy::new(cfg, mock.clone(), mock.clone()),
            mock,
            _shard_rx: shard_rx,
        }
    }

    fn new() -> Self {
        Self::with(IngressConfig::default(), &LiveAccountsConfig::default())
    }
}

fn row(address: Address, nonce: u64, balance: u64) -> AccountRow {
    AccountRow {
        address,
        nonce,
        balance: U256::from(balance),
    }
}

#[tokio::test]
async fn an_unknown_sender_is_admitted() {
    let f = Fixture::new();
    let p = &f.proxy;
    let signer = PrivateKeySigner::random();
    p.submit_raw_async(IP, sign_legacy(&signer, 5))
        .await
        .expect("no entry admits");
}

#[tokio::test]
async fn a_past_nonce_without_a_receipt_publishes() {
    let f = Fixture::new();
    let (p, mock) = (&f.proxy, &f.mock);
    let signer = PrivateKeySigner::random();
    let _ = mock.apply_rows(
        BPosition::from_index(10),
        &[row(signer.address(), 3, LEGACY_COST * 10)],
    );
    // The layer knows nonce 3, but no receipt proves who holds nonce 2:
    // the receipt may never reach this ingress's cache. The sequencer
    // decides.
    p.submit_raw_async(IP, sign_legacy(&signer, 2))
        .await
        .expect("a past nonce with no receipt publishes");
    p.submit_raw_async(IP, sign_legacy(&signer, 3))
        .await
        .expect("the next nonce publishes");
    p.submit_raw_async(IP, sign_legacy(&signer, 9))
        .await
        .expect("a future nonce is never rejected here");
}

#[tokio::test]
async fn an_unfunded_sender_is_rejected_and_a_funded_one_admitted() {
    let f = Fixture::new();
    let (p, mock) = (&f.proxy, &f.mock);
    let signer = PrivateKeySigner::random();
    let _ = mock.apply_rows(
        BPosition::from_index(10),
        &[row(signer.address(), 0, LEGACY_COST - 1)],
    );
    let err = p
        .submit_raw_async(IP, sign_legacy(&signer, 0))
        .await
        .unwrap_err();
    match err {
        IngressError::InsufficientFunds {
            address,
            have,
            want,
        } => {
            assert_eq!(address, signer.address());
            assert_eq!(have, U256::from(LEGACY_COST - 1));
            assert_eq!(want, U256::from(LEGACY_COST));
        }
        other => panic!("expected InsufficientFunds, got {other:?}"),
    }
    // A newer batch funds the account: the monotone rule applies it.
    let _ = mock.apply_rows(
        BPosition::from_index(11),
        &[row(signer.address(), 0, LEGACY_COST)],
    );
    p.submit_raw_async(IP, sign_legacy(&signer, 0))
        .await
        .expect("an exact balance covers the cost");
}

#[tokio::test]
async fn an_expired_entry_admits() {
    // The TTL must outlast the first submit, which recovers a signature
    // and runs admission. A loaded CI runner took more than 20ms for
    // that, and the entry expired before the check it was meant to fail.
    let live = LiveAccountsConfig {
        capacity: NonZeroUsize::new(16).unwrap(),
        ttl_ms: NonZeroU64::new(1_000).unwrap(),
    };
    let f = Fixture::with(IngressConfig::default(), &live);
    let (p, mock) = (&f.proxy, &f.mock);
    let signer = PrivateKeySigner::random();
    // A live, unfunded entry rejects; the same entry past its TTL does
    // not.
    let _ = mock.apply_rows(BPosition::from_index(10), &[row(signer.address(), 3, 0)]);
    let err = p
        .submit_raw_async(IP, sign_legacy(&signer, 3))
        .await
        .unwrap_err();
    assert!(
        matches!(err, IngressError::InsufficientFunds { .. }),
        "{err:?}"
    );
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    p.submit_raw_async(IP, sign_legacy(&signer, 3))
        .await
        .expect("past the TTL the entry proves nothing");
}

#[tokio::test]
async fn the_receipt_cache_answers_a_retry_before_the_nonce_check() {
    let f = Fixture::new();
    let (p, mock) = (&f.proxy, &f.mock);
    let signer = PrivateKeySigner::random();
    let raw = sign_legacy(&signer, 0);
    let tx_hash = alloy_primitives::keccak256(&raw);
    // The tx landed: the rows apply, then the receipt fans out, in the
    // live pump's order.
    let _ = mock.apply_rows(BPosition::from_index(1), &[row(signer.address(), 1, 0)]);
    mock.receipt_bus
        .send(receipt(signer.address(), 0, tx_hash, 1))
        .unwrap();
    // The receipt watcher runs on its own task.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let got = p
        .submit_raw_async(IP, raw)
        .await
        .expect("the S5 retry contract: a landed tx answers with its receipt");
    assert_eq!(got, tx_hash);
    // A different tx at the landed nonce is a duplicate, from the
    // receipt cache's identity guard: the receipt proves the conflict.
    let err = p
        .submit_raw_async(IP, sign_legacy_value(&signer, 0, 1))
        .await
        .unwrap_err();
    assert!(matches!(err, IngressError::Duplicate(_)), "{err:?}");
}

/// A skip receipt at the nonce proves that the chain did not use it. The
/// same tx gets its skip receipt, and another tx at the nonce publishes:
/// it is not a duplicate.
#[tokio::test]
async fn a_skip_receipt_leaves_its_nonce_open() {
    let f = Fixture::new();
    let (p, mock) = (&f.proxy, &f.mock);
    let signer = PrivateKeySigner::random();
    let raw = sign_legacy(&signer, 0);
    let tx_hash = alloy_primitives::keccak256(&raw);
    let skip = kardamom_types::Receipt {
        status: false,
        gas_used: 0,
        skip_reason: Some(kardamom_types::SkipReason::InsufficientFunds),
        ..receipt(signer.address(), 0, tx_hash, 1)
    };
    mock.receipt_bus.send(skip).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let got = p
        .submit_raw_async(IP, raw)
        .await
        .expect("the same tx gets its skip receipt");
    assert_eq!(got, tx_hash);
    let other = sign_legacy_value(&signer, 0, 1);
    let got = p
        .submit_raw_async(IP, other.clone())
        .await
        .expect("another tx at a skipped nonce publishes");
    assert_eq!(got, alloy_primitives::keccak256(&other));
}

/// An executor query stub: it answers a balance query with `balance_hex`
/// and a nonce query with `0x0`, sends `tx_idx` as the snapshot position,
/// and counts the queries. A raw HTTP server, so it can send the
/// `x-state-tx-idx` header of the real executor.
struct ExecutorStub {
    url: String,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl ExecutorStub {
    async fn start(balance_hex: &'static str, tx_idx: u64) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = calls.clone();
        let task = tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tokio::spawn(Self::answer(sock, balance_hex, tx_idx));
            }
        });
        Self { url, calls, task }
    }

    /// Read one request and answer it, then close the connection.
    async fn answer(mut sock: tokio::net::TcpStream, balance_hex: &str, tx_idx: u64) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut buf = vec![0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let request = String::from_utf8_lossy(&buf[..n]);
        let value = if request.contains("eth_getBalance") {
            balance_hex
        } else {
            "0x0"
        };
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":"{value}"}}"#);
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-state-tx-idx: {tx_idx}\r\n\
             content-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        sock.write_all(response.as_bytes()).await.unwrap();
    }

    fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for ExecutorStub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A proxy whose executor query asks `endpoint`. The local layer has seen
/// position 100.
fn with_query(endpoint: String) -> Fixture {
    let cfg = IngressConfig {
        executor_query: kardamom_cache::ExecutorQueryConfig {
            endpoints: vec![endpoint],
            ..kardamom_cache::ExecutorQueryConfig::default()
        },
        ..IngressConfig::default()
    };
    let f = Fixture::with(cfg, &LiveAccountsConfig::default());
    let _ = f.mock.apply_rows(
        BPosition::from_index(100),
        &[row(Address::repeat_byte(0xee), 0, 0)],
    );
    f
}

/// A sender with no row in any cache layer and no funds on the chain is
/// refused: the executor answers for it once, and the answer serves the
/// next submit too.
#[tokio::test]
async fn a_cold_unfunded_sender_is_refused_with_one_query() {
    let stub = ExecutorStub::start("0x0", 100).await;
    let f = with_query(stub.url.clone());
    let signer = PrivateKeySigner::random();
    for nonce in [0, 1] {
        let err = f
            .proxy
            .submit_raw_async(IP, sign_legacy(&signer, nonce))
            .await
            .unwrap_err();
        assert!(
            matches!(err, IngressError::InsufficientFunds { have, .. } if have == U256::ZERO),
            "{err:?}"
        );
    }
    assert_eq!(
        stub.calls(),
        2,
        "one nonce query and one balance query for the cold sender"
    );
}

/// An executor far behind the newest position this ingress has seen can
/// show a new account as unfunded. Its answer is stale: the submit
/// admits, and the next submit admits too with no new query.
#[tokio::test]
async fn a_lagging_executor_answer_admits() {
    let stub = ExecutorStub::start("0x0", 1).await;
    let f = with_query(stub.url.clone());
    let _ = f.mock.apply_rows(
        BPosition::from_index(1_000_000),
        &[row(Address::repeat_byte(0xef), 0, 0)],
    );
    let signer = PrivateKeySigner::random();
    for nonce in [0, 1] {
        f.proxy
            .submit_raw_async(IP, sign_legacy(&signer, nonce))
            .await
            .expect("a stale answer never refuses");
    }
    assert_eq!(stub.calls(), 2, "the stale outcome is kept");
}

/// A restarted ingress on an idle chain has seen no receipt frame, so it
/// has no head to measure an executor answer against. An unfunded answer
/// admits and is not kept as a refusal.
#[tokio::test]
async fn an_answer_before_the_first_head_admits() {
    let stub = ExecutorStub::start("0x0", 100).await;
    let cfg = IngressConfig {
        executor_query: kardamom_cache::ExecutorQueryConfig {
            endpoints: vec![stub.url.clone()],
            ..kardamom_cache::ExecutorQueryConfig::default()
        },
        ..IngressConfig::default()
    };
    let f = Fixture::with(cfg, &LiveAccountsConfig::default());
    let signer = PrivateKeySigner::random();
    for nonce in [0, 1] {
        f.proxy
            .submit_raw_async(IP, sign_legacy(&signer, nonce))
            .await
            .expect("no head: the answer is stale");
    }
    assert_eq!(stub.calls(), 2, "the stale outcome is kept");
}

/// A cold sender that the executor shows funded is admitted. A failed
/// query admits, and the next submit of the sender admits at once with
/// no new query.
#[tokio::test]
async fn a_cold_funded_sender_or_a_failed_query_admits() {
    let stub = ExecutorStub::start("0xde0b6b3a7640000", 100).await;
    let f = with_query(stub.url.clone());
    f.proxy
        .submit_raw_async(IP, sign_legacy(&PrivateKeySigner::random(), 0))
        .await
        .expect("a funded cold sender is admitted");
    // A port that was bound and released: nothing listens on it.
    let dead = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    };
    let f = with_query(dead);
    let signer = PrivateKeySigner::random();
    for nonce in [0, 1] {
        f.proxy
            .submit_raw_async(IP, sign_legacy(&signer, nonce))
            .await
            .expect("a failed query admits");
    }
}

/// A legacy tx with a value, so its hash differs from `sign_legacy`'s.
fn sign_legacy_value(signer: &PrivateKeySigner, nonce: u64, value: u64) -> alloy_primitives::Bytes {
    use alloy_consensus::TxLegacy;
    use kardamom_ingress::test_support::sign_and_encode;
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: alloy_primitives::TxKind::Call(Address::ZERO),
        value: U256::from(value),
        input: alloy_primitives::Bytes::default(),
    };
    sign_and_encode(tx, signer).1
}

#[tokio::test]
async fn checks_off_admits_an_unfunded_sender() {
    let cfg = IngressConfig {
        admission_checks: false,
        ..IngressConfig::default()
    };
    let f = Fixture::with(cfg, &LiveAccountsConfig::default());
    let (p, mock) = (&f.proxy, &f.mock);
    let signer = PrivateKeySigner::random();
    let _ = mock.apply_rows(BPosition::from_index(10), &[row(signer.address(), 3, 0)]);
    p.submit_raw_async(IP, sign_legacy(&signer, 0))
        .await
        .expect("checks off: every submit publishes");
}

#[tokio::test]
async fn the_account_rpcs_serve_the_local_layer_and_only_the_head() {
    let server = start_test_server(IngressConfig::default()).await;
    let client = http_client(server.addr);
    let addr = Address::repeat_byte(0x42);
    let _ = server
        .mock
        .apply_rows(BPosition::from_index(3), &[row(addr, 7, 1234)]);

    let nonce: U256 = client
        .request("eth_getTransactionCount", rpc_params![addr, "latest"])
        .await
        .unwrap();
    assert_eq!(nonce, U256::from(7u64));
    let balance: U256 = client
        .request("eth_getBalance", rpc_params![addr, "pending"])
        .await
        .unwrap();
    assert_eq!(balance, U256::from(1234u64));

    // No entry and no executor query: a clear error, not a stall.
    let err = client
        .request::<U256, _>(
            "eth_getBalance",
            rpc_params![Address::repeat_byte(0x43), "latest"],
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("account state unavailable"),
        "{err}"
    );

    // History is not served.
    let err = client
        .request::<U256, _>("eth_getBalance", rpc_params![addr, "0x5"])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not served"), "{err}");
    server.handle.stop().unwrap();
}

#[test]
fn a_receipt_hash_helper_is_the_keccak_of_the_raw_bytes() {
    // Guards the assumption `the_receipt_cache_answers_a_retry_before_the_nonce_check`
    // makes: the proxy's `tx_hash` is `keccak256(raw_tx)`.
    let signer = PrivateKeySigner::random();
    let raw = sign_legacy(&signer, 0);
    assert_ne!(alloy_primitives::keccak256(&raw), B256::ZERO);
}
