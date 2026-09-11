//! A read-only account nonce query on the executor node.
//!
//! The sequencer asks an executor for the committed nonce of a cold
//! sender. The answer is a lower bound on the sender's next nonce. An
//! executor at any height gives a valid answer: a floor that lags the
//! truth only parks a transaction a little longer. See
//! `docs/specs/dynamic-sequencer-sizing.md`, section 3.4.
//!
//! The wire format is Ethereum JSON-RPC over HTTP/1.0, one request per
//! connection, with no HTTP dependency. This is the same shape as
//! [`crate::checkpoint_transfer`]. The ingress can proxy
//! `eth_getTransactionCount` to it later.
//!
//! ```text
//! POST / HTTP/1.0
//! content-length: <bytes>
//!
//! {"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["0x…","latest"]}
//!
//! HTTP/1.0 200 OK
//! content-type: application/json
//! x-state-block: <u64>
//! content-length: <bytes>
//!
//! {"jsonrpc":"2.0","id":1,"result":"0x2a"}
//! ```
//!
//! Each request opens a fresh [`StateSnapshot`] on `spawn_blocking` and
//! drops it before the response goes out. A snapshot pins a read-only
//! transaction, and the writer's page reclaim waits on old transactions.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::time::Duration;

use alloy_primitives::Address;
use kardamom_types::StateDatabase;
use kardamom_types::num::usize_to_u64;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{info, warn};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::snapshot::StateSnapshot;

const MAX_HEAD: usize = 8 * 1024;
const MAX_BODY: usize = 8 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Queries served, by `outcome` (`ok`, `bad_request`, `error`).
pub const NONCE_QUERIES: &str = "kardamom_state_nonce_queries_total";

/// The bound listener and its accept task.
pub struct NonceQueryServer {
    pub addr: SocketAddr,
    pub task: tokio::task::JoinHandle<()>,
}

/// Serve account nonce queries on `addr`, forever. Binding happens before
/// the task spawns, so a bad address fails startup with a clear error.
/// Call this inside a tokio runtime.
///
/// # Errors
///
/// Returns the bind error when `addr` cannot be bound.
pub fn serve_nonce_queries(addr: SocketAddr, env: StateEnv) -> std::io::Result<NonceQueryServer> {
    let std_listener = std::net::TcpListener::bind(addr)?;
    std_listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(std_listener)?;
    let addr = listener.local_addr()?;
    info!(%addr, "serving account nonce queries");
    let server = QueryServer { listener, env };
    let task = tokio::spawn(server.run());
    Ok(NonceQueryServer { addr, task })
}

/// The accept loop: one task per connection.
struct QueryServer {
    listener: TcpListener,
    env: StateEnv,
}

impl QueryServer {
    async fn run(self) {
        loop {
            self.accept_one().await;
        }
    }

    /// Accept one connection and serve it on its own task. An accept
    /// error is logged; the loop goes on.
    async fn accept_one(&self) {
        let stream = match self.listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                warn!(error = %e, "nonce query accept failed");
                return;
            }
        };
        let env = self.env.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_one(stream, env).await {
                warn!(error = %e, "nonce query connection failed");
            }
        });
    }
}

/// The committed nonce of `address`, and the block of the snapshot that
/// answered. An unknown account has nonce 0.
///
/// # Errors
///
/// Returns the state error when the snapshot cannot open or the read
/// fails.
pub fn committed_nonce(env: &StateEnv, address: Address) -> Result<(u64, u64), StateError> {
    let snapshot = StateSnapshot::open(env)?;
    let nonce = snapshot.basic(address)?.map_or(0, |(n, _, _)| n);
    Ok((nonce, snapshot.block_number()))
}

#[derive(serde::Deserialize)]
struct Request {
    #[serde(default)]
    id: serde_json::Value,
    #[serde(default)]
    method: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

/// The reply to one request: the HTTP status, the JSON body, and the
/// snapshot block when a query ran.
struct Reply {
    status: &'static str,
    body: String,
    block: Option<u64>,
}

impl Reply {
    /// A JSON-RPC error body under `status`, counted as `outcome`.
    fn error(
        status: &'static str,
        outcome: &'static str,
        id: &serde_json::Value,
        code: i64,
        message: &str,
    ) -> Self {
        metrics::counter!(NONCE_QUERIES, "outcome" => outcome).increment(1);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": code, "message": message },
        })
        .to_string();
        Self {
            status,
            body,
            block: None,
        }
    }

    /// A malformed request, before any JSON-RPC id is known.
    fn bad_request(code: i64, message: &str) -> Self {
        Self::error(
            "400 Bad Request",
            "bad_request",
            &serde_json::Value::Null,
            code,
            message,
        )
    }

    /// A well-formed request the server cannot answer: the JSON-RPC error
    /// rides a `200 OK`, as the protocol says.
    fn rpc_error(id: &serde_json::Value, code: i64, message: &str) -> Self {
        Self::error("200 OK", "bad_request", id, code, message)
    }

    /// A failed state read.
    fn internal(id: &serde_json::Value, message: &str) -> Self {
        Self::error("500 Internal Server Error", "error", id, -32603, message)
    }

    /// The committed `nonce` at `block`.
    fn ok(id: &serde_json::Value, nonce: u64, block: u64) -> Self {
        metrics::counter!(NONCE_QUERIES, "outcome" => "ok").increment(1);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": format!("{nonce:#x}"),
        })
        .to_string();
        Self {
            status: "200 OK",
            body,
            block: Some(block),
        }
    }

    /// Write the HTTP/1.0 response and close.
    async fn write<W: AsyncWriteExt + Unpin>(&self, wr: &mut W) -> std::io::Result<()> {
        let block_header = self
            .block
            .map(|b| format!("x-state-block: {b}\r\n"))
            .unwrap_or_default();
        let head = format!(
            "HTTP/1.0 {}\r\ncontent-type: application/json\r\n{block_header}\
             content-length: {}\r\nconnection: close\r\n\r\n",
            self.status,
            self.body.len()
        );
        timeout(IO_TIMEOUT, wr.write_all(head.as_bytes())).await??;
        timeout(IO_TIMEOUT, wr.write_all(self.body.as_bytes())).await??;
        timeout(IO_TIMEOUT, wr.flush()).await??;
        Ok(())
    }
}

/// The address a well-formed `eth_getTransactionCount` request names.
fn requested_address(request: &Request) -> Option<Address> {
    request
        .params
        .first()
        .and_then(serde_json::Value::as_str)
        .and_then(|s| s.parse::<Address>().ok())
}

/// Answer one request body.
async fn answer(env: &StateEnv, body: &[u8]) -> Reply {
    let Ok(request) = serde_json::from_slice::<Request>(body) else {
        return Reply::bad_request(-32700, "parse error");
    };
    if request.method != "eth_getTransactionCount" {
        return Reply::rpc_error(&request.id, -32601, "method not found");
    }
    let Some(address) = requested_address(&request) else {
        return Reply::rpc_error(
            &request.id,
            -32602,
            "invalid params: expected [address, tag]",
        );
    };
    let env = env.clone();
    let looked_up = tokio::task::spawn_blocking(move || committed_nonce(&env, address)).await;
    match looked_up {
        Ok(Ok((nonce, block))) => Reply::ok(&request.id, nonce, block),
        Ok(Err(e)) => {
            warn!(error = %e, %address, "nonce query: state read failed");
            Reply::internal(&request.id, "state read failed")
        }
        Err(e) => {
            warn!(error = %e, "nonce query: blocking task failed");
            Reply::internal(&request.id, "internal error")
        }
    }
}

/// The request head, parsed once: a `POST` with a body of a known,
/// bounded size.
struct RequestHead {
    content_length: NonZeroUsize,
}

impl RequestHead {
    /// Read the request line and the headers off `reader`. `None` for a
    /// request that is not a `POST`; an error reply for a body that is
    /// missing or oversized.
    async fn read<R: AsyncBufReadExt + Unpin>(
        reader: &mut R,
    ) -> std::io::Result<Result<Option<Self>, Reply>> {
        let mut line = String::new();
        timeout(IO_TIMEOUT, reader.read_line(&mut line)).await??;
        if !line.starts_with("POST ") {
            return Ok(Ok(None));
        }
        let mut content_length: Option<usize> = None;
        loop {
            line.clear();
            timeout(IO_TIMEOUT, reader.read_line(&mut line)).await??;
            if let ControlFlow::Break(()) = Self::read_header(&line, &mut content_length) {
                break;
            }
        }
        let body_fits = |n: usize| (1..=MAX_BODY).contains(&n);
        let head = content_length
            .filter(|n| body_fits(*n))
            .and_then(NonZeroUsize::new)
            .map(|content_length| Some(Self { content_length }))
            .ok_or_else(|| Reply::bad_request(-32600, "missing or oversized body"));
        Ok(head)
    }

    /// One header line. `Break` at the blank line that ends the head, or
    /// at the end of the stream. A `content-length` header sets
    /// `content_length`; an unparsable value leaves it unset.
    fn read_header(line: &str, content_length: &mut Option<usize>) -> ControlFlow<()> {
        let header = line.trim_end();
        if line.is_empty() || header.is_empty() {
            return ControlFlow::Break(());
        }
        let length = header
            .split_once(':')
            .filter(|(key, _)| key.trim().eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.trim().parse().ok());
        if length.is_some() {
            *content_length = length;
        }
        ControlFlow::Continue(())
    }
}

async fn serve_one(stream: TcpStream, env: StateEnv) -> std::io::Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd).take(usize_to_u64(MAX_HEAD.saturating_add(MAX_BODY)));
    let head = match RequestHead::read(&mut reader).await? {
        Ok(Some(head)) => head,
        Ok(None) => {
            return Reply {
                status: "404 Not Found",
                body: String::new(),
                block: None,
            }
            .write(&mut wr)
            .await;
        }
        Err(reply) => return reply.write(&mut wr).await,
    };
    let mut body = vec![0u8; head.content_length.get()];
    timeout(IO_TIMEOUT, reader.read_exact(&mut body)).await??;
    answer(&env, &body).await.write(&mut wr).await
}

#[cfg(test)]
mod tests {
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

    fn query(addr: SocketAddr, address: Address) -> String {
        post(
            addr,
            &format!(
                r#"{{"jsonrpc":"2.0","id":5,"method":"eth_getTransactionCount","params":["{address}","latest"]}}"#
            ),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serves_the_committed_nonce() {
        let dir = tempfile::tempdir().unwrap();
        let env = StateEnvBuilder::new(dir.path()).open().unwrap();
        let known = Address::repeat_byte(0x11);
        seed_genesis(
            &env,
            &[AccountChange {
                address: known,
                nonce: 7,
                balance: U256::ZERO,
                code_hash: keccak256([]),
            }],
            &[],
        )
        .unwrap();
        let server = serve_nonce_queries("127.0.0.1:0".parse().unwrap(), env).unwrap();
        let addr = server.addr;

        let known_reply = tokio::task::spawn_blocking(move || query(addr, known))
            .await
            .unwrap();
        assert!(known_reply.starts_with("HTTP/1.0 200 OK"), "{known_reply}");
        assert!(known_reply.contains("x-state-block: "), "{known_reply}");
        assert!(known_reply.contains(r#""result":"0x7""#), "{known_reply}");
        assert!(known_reply.contains(r#""id":5"#), "{known_reply}");

        let unknown = Address::repeat_byte(0x22);
        let unknown_reply = tokio::task::spawn_blocking(move || query(addr, unknown))
            .await
            .unwrap();
        assert!(
            unknown_reply.contains(r#""result":"0x0""#),
            "{unknown_reply}"
        );
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
                r#"{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":[]}"#,
            )
        })
        .await
        .unwrap();
        assert!(wrong_method.contains(r#""code":-32601"#), "{wrong_method}");

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
}
