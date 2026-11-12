use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

use super::*;

#[derive(Clone, Copy)]
enum Mode {
    Redis,
    NoLookup,
    RpcError,
}

struct Peer {
    reader: Reader,
    calls: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

struct Http {
    mode: Mode,
    calls: Arc<AtomicU64>,
    lookups: AtomicU64,
}

impl Peer {
    async fn start(name: &str, mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let calls = Arc::new(AtomicU64::new(0));
        let http = Http {
            mode,
            calls: calls.clone(),
            lookups: AtomicU64::new(10),
        };
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                http.serve(stream).await;
            }
        });
        Self {
            reader: Reader {
                name: name.into(),
                target: Target::bridged(Ipv4Addr::LOCALHOST, name, port),
                scrape: Scrape::new(),
                rpc: Rpc::new(&format!("http://127.0.0.1:{port}"), 1).unwrap(),
            },
            calls,
            task,
        }
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Http {
    async fn serve(&self, stream: TcpStream) {
        let mut stream = BufReader::new(stream);
        let mut first = String::new();
        stream.read_line(&mut first).await.unwrap();
        let mut length = 0;
        while let Some(value) = Self::header(&mut stream).await {
            length = length.max(value);
        }
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        let response = if first.starts_with("POST ") {
            self.rpc(&body)
        } else {
            format!(
                "{LOOKUPS}{{layer=\"redis\",outcome=\"miss\"}} {}\n{DEGRADED}{{reason=\"stale\"}} 7\n",
                self.lookups.load(Ordering::Acquire)
            )
        };
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.len()
        );
        stream.get_mut().write_all(header.as_bytes()).await.unwrap();
        stream
            .get_mut()
            .write_all(response.as_bytes())
            .await
            .unwrap();
    }

    async fn header(stream: &mut BufReader<TcpStream>) -> Option<usize> {
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        match line.as_str() {
            "\r\n" => None,
            _ => Some(
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map_or(0, |value| value.trim().parse::<usize>().unwrap()),
            ),
        }
    }

    fn rpc(&self, body: &[u8]) -> String {
        let request: serde_json::Value = serde_json::from_slice(body).unwrap();
        assert_eq!(request["method"], "eth_getBalance");
        self.calls.fetch_add(1, Ordering::AcqRel);
        match self.mode {
            Mode::Redis => {
                self.lookups.fetch_add(1, Ordering::AcqRel);
            }
            Mode::NoLookup => (),
            Mode::RpcError => {
                self.lookups.fetch_add(1, Ordering::AcqRel);
                return serde_json::json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"executor unavailable"}}).to_string();
            }
        }
        serde_json::json!({"jsonrpc":"2.0","id":request["id"],"result":"0x0"}).to_string()
    }
}

#[tokio::test]
async fn both_real_rpc_endpoints_must_prove_their_own_redis_read() {
    let healthy = Peer::start("ingress-0", Mode::Redis).await;
    let untouched = Peer::start("ingress-1", Mode::NoLookup).await;
    let recovery = Recovery::new(vec![healthy.reader.clone(), untouched.reader.clone()]).unwrap();
    assert!(recovery.observe(&recovery.read().await).is_none());
    assert_eq!(healthy.calls.load(Ordering::Acquire), 1);
    assert_eq!(untouched.calls.load(Ordering::Acquire), 1);
    assert!(recovery.last.borrow().contains("redis: 10"));
}

#[tokio::test]
async fn complete_http_recovery_passes_and_rpc_failure_cannot_pass_on_counters() {
    let first = Peer::start("ingress-0", Mode::Redis).await;
    let second = Peer::start("ingress-1", Mode::Redis).await;
    let recovery = Recovery::new(vec![first.reader.clone(), second.reader.clone()]).unwrap();
    assert_eq!(recovery.observe(&recovery.read().await), Some(()));
    let failing = Peer::start("ingress-1", Mode::RpcError).await;
    let recovery = Recovery::new(vec![first.reader.clone(), failing.reader.clone()]).unwrap();
    assert!(recovery.observe(&recovery.read().await).is_none());
    assert!(recovery.last.borrow().contains("executor unavailable"));
}
