//! `eth_getTransactionReceipt` behind the memory cache, with the egress as
//! the receipt query and an executor as the fallback. The egress keeps
//! only its newest receipts, so an old receipt must come from the state DB
//! of an executor. A new receipt must come from the egress with no second
//! query.

use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use alloy_primitives::{Address, B256};
use kardamom_cache::ExecutorQueryConfig;
use kardamom_ingress::config::IngressConfig;
use kardamom_ingress::test_support::receipt;
use kardamom_ingress::{IngressProxy, MockChannels};
use kardamom_types::{Receipt, TxEnvelope};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// `IngressConfig::default` routes over 8 shards; the mock opens as many.
const SHARDS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
const IP: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 4, 0, 1));

/// How a stub answers each receipt query.
#[derive(Clone)]
enum Reply {
    /// The state query of an executor, or the egress: the receipt it
    /// holds as rkyv hex, or `null`.
    Holds(Arc<HashMap<B256, Receipt>>),
    /// The egress with a busy store: `503` and a body that is no JSON.
    Busy,
    /// An endpoint that takes the request and never answers.
    Silent,
}

/// A raw HTTP stub of a receipt query endpoint. It counts the queries.
struct Stub {
    url: String,
    calls: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Stub {
    async fn start(reply: Reply) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let task = tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                counted.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(Self::answer(sock, reply.clone()));
            }
        });
        Self { url, calls, task }
    }

    /// A stub that holds `receipts`.
    async fn holding(receipts: &[&Receipt]) -> Self {
        let map = receipts.iter().map(|r| (r.tx_hash, (*r).clone())).collect();
        Self::start(Reply::Holds(Arc::new(map))).await
    }

    /// Read one request, answer it, and close the connection.
    async fn answer(mut sock: tokio::net::TcpStream, reply: Reply) {
        let request = Self::read_request(&mut sock).await;
        let response = match reply {
            Reply::Holds(map) => {
                let result = map
                    .iter()
                    .find(|(hash, _)| request.contains(&format!("{hash}")))
                    .map_or("null".to_string(), |(_, r)| {
                        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(r).unwrap();
                        format!("\"0x{}\"", alloy_primitives::hex::encode(&bytes))
                    });
                let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{result}}}"#);
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-state-block: 9\r\n\
                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
            }
            Reply::Busy => {
                let body = "the store is busy or gone\n";
                format!(
                    "HTTP/1.1 503 Service Unavailable\r\ncontent-length: {}\r\n\
                     connection: close\r\n\r\n{body}",
                    body.len()
                )
            }
            Reply::Silent => {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                return;
            }
        };
        sock.write_all(response.as_bytes()).await.unwrap();
    }

    /// The request up to the end of its JSON body.
    async fn read_request(sock: &mut tokio::net::TcpStream) -> String {
        let mut request = Vec::new();
        let mut buf = [0u8; 4096];
        while !request.ends_with(b"}") {
            let n = sock.read(&mut buf).await.unwrap();
            assert!(n > 0, "the client closed before its body");
            request.extend_from_slice(&buf[..n]);
        }
        String::from_utf8_lossy(&request).into_owned()
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A proxy over `MockChannels` with the receipt query and the executor
/// query on the given endpoints, and the shard receivers a test holds.
struct Fixture {
    proxy: IngressProxy<MockChannels, MockChannels>,
    _shard_rx: Vec<tokio::sync::mpsc::UnboundedReceiver<TxEnvelope>>,
}

impl Fixture {
    fn new(egress: Option<&str>, executor: &str) -> Self {
        let cfg = IngressConfig {
            receipt_query: ExecutorQueryConfig {
                endpoints: egress.into_iter().map(str::to_string).collect(),
                timeout_ms: NonZeroU64::new(300).unwrap(),
                ..ExecutorQueryConfig::default()
            },
            executor_query: ExecutorQueryConfig {
                endpoints: vec![executor.to_string()],
                ..ExecutorQueryConfig::default()
            },
            ..IngressConfig::default()
        };
        let (mock, shard_rx) = MockChannels::new(SHARDS);
        Self {
            proxy: IngressProxy::new(cfg, mock.clone(), mock),
            _shard_rx: shard_rx,
        }
    }

    async fn receipt(&self, hash: B256) -> Option<Receipt> {
        self.proxy.receipt_by_hash(IP, hash).await
    }
}

/// A receipt below the floor of the egress, and one inside it.
fn old_and_new() -> (Receipt, Receipt) {
    let sender = Address::repeat_byte(0x41);
    (
        receipt(sender, 0, B256::repeat_byte(0x0d), 1),
        receipt(sender, 1, B256::repeat_byte(0x0e), 2),
    )
}

/// The egress joined after the old transaction and answers `null` for it.
/// The executor answers it, and the answer fills the memory cache, so the
/// next poll asks no layer.
#[tokio::test]
async fn an_old_receipt_the_egress_lacks_comes_from_the_executor() {
    let (old, new) = old_and_new();
    let egress = Stub::holding(&[&new]).await;
    let executor = Stub::holding(&[&old, &new]).await;
    let f = Fixture::new(Some(&egress.url), &executor.url);

    assert_eq!(f.receipt(old.tx_hash).await, Some(old.clone()));
    assert_eq!(f.receipt(old.tx_hash).await, Some(old));

    assert_eq!((egress.calls(), executor.calls()), (1, 1));
}

/// The egress holds the new receipt: the read ends there, and the executor
/// gets no query.
#[tokio::test]
async fn a_new_receipt_comes_from_the_egress_with_no_second_query() {
    let (old, new) = old_and_new();
    let egress = Stub::holding(&[&new]).await;
    let executor = Stub::holding(&[&old, &new]).await;
    let f = Fixture::new(Some(&egress.url), &executor.url);

    assert_eq!(f.receipt(new.tx_hash).await, Some(new));

    assert_eq!((egress.calls(), executor.calls()), (1, 0));
}

/// A hash that no executor knows stays `null`: the fallback hides no real
/// miss.
#[tokio::test]
async fn a_hash_that_no_executor_knows_stays_null() {
    let (old, new) = old_and_new();
    let egress = Stub::holding(&[&new]).await;
    let executor = Stub::holding(&[&old, &new]).await;
    let f = Fixture::new(Some(&egress.url), &executor.url);

    assert_eq!(f.receipt(B256::repeat_byte(0x77)).await, None);

    assert_eq!((egress.calls(), executor.calls()), (1, 1));
}

/// An egress that is down, busy (`503`), or past its timeout gives no
/// receipt: the executor answers.
#[tokio::test]
async fn a_failed_egress_falls_back_to_the_executor() {
    let (old, _) = old_and_new();
    let executor = Stub::holding(&[&old]).await;
    let busy = Stub::start(Reply::Busy).await;
    let silent = Stub::start(Reply::Silent).await;
    // A port that was bound and released: nothing listens on it.
    let dead = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    };

    for egress in [&dead, &busy.url, &silent.url] {
        let f = Fixture::new(Some(egress), &executor.url);
        assert_eq!(f.receipt(old.tx_hash).await, Some(old.clone()), "{egress}");
    }

    assert_eq!((busy.calls(), silent.calls(), executor.calls()), (1, 1, 3));
}

/// With no receipt query endpoint, the executor is the only layer, as
/// before the egress existed.
#[tokio::test]
async fn with_no_egress_the_executor_answers_alone() {
    let (old, _) = old_and_new();
    let executor = Stub::holding(&[&old]).await;
    let f = Fixture::new(None, &executor.url);

    assert_eq!(f.receipt(old.tx_hash).await, Some(old));
    assert_eq!(f.receipt(B256::repeat_byte(0x77)).await, None);

    assert_eq!(executor.calls(), 2);
}
