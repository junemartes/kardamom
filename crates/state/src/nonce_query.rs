//! A read-only account query on the executor node: the committed nonce
//! or balance of one address.
//!
//! The sequencer asks an executor for the committed nonce of a cold
//! sender. The answer is a lower bound on the sender's next nonce. An
//! executor at any height gives a valid answer: a floor that lags the
//! truth only parks a transaction a little longer. See
//! `docs/specs/dynamic-sequencer-sizing.md`, section 3.4. The ingress and
//! the state mirror ask for the balance too, on a cache miss; see
//! `docs/specs/2026-09-13-redis-account-cache-design.md`.
//!
//! The wire format is Ethereum JSON-RPC over HTTP/1.0, one request per
//! connection, with no HTTP dependency. This is the same shape as
//! [`crate::checkpoint_transfer`].
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
//! x-state-tx-idx: <u64>
//! content-length: <bytes>
//!
//! {"jsonrpc":"2.0","id":1,"result":"0x2a"}
//! ```
//!
//! `x-state-tx-idx` is the canonical end position of the snapshot's last
//! committed block, as an index. A cache writes the answer back tagged
//! with it, so the answer never outranks a newer row.
//!
//! Each request opens a fresh [`StateSnapshot`] on `spawn_blocking` and
//! drops it before the response goes out. A snapshot pins a read-only
//! transaction, and the writer's page reclaim waits on old transactions.

use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::time::Duration;

use alloy_primitives::{Address, U256};
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

impl Drop for NonceQueryServer {
    /// End the accept loop, which frees the port and the server's clone
    /// of the state env. A process that opens its state again in place
    /// (a resync revolution) binds a new server on the same address.
    fn drop(&mut self) {
        self.task.abort();
    }
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

/// The committed state of one account, and where the snapshot that
/// answered stands. An unknown account has nonce 0 and balance 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommittedAccount {
    pub nonce: u64,
    pub balance: U256,
    /// The snapshot's block.
    pub block: u64,
    /// The canonical end position of the snapshot's last block, as an
    /// index.
    pub tx_idx: u64,
}

/// The committed state of `address`.
///
/// # Errors
///
/// Returns the state error when the snapshot cannot open or the read
/// fails.
pub fn committed_account(env: &StateEnv, address: Address) -> Result<CommittedAccount, StateError> {
    let snapshot = StateSnapshot::open(env)?;
    let (nonce, balance) = snapshot
        .basic(address)?
        .map_or((0, U256::ZERO), |(n, b, _)| (n, b));
    Ok(CommittedAccount {
        nonce,
        balance,
        block: snapshot.block_number(),
        tx_idx: snapshot.end_tx_position()?.as_index(),
    })
}

/// The two methods the server answers, parsed once at the boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Method {
    Nonce,
    Balance,
}

impl Method {
    fn parse(name: &str) -> Option<Self> {
        match name {
            "eth_getTransactionCount" => Some(Self::Nonce),
            "eth_getBalance" => Some(Self::Balance),
            _ => None,
        }
    }

    /// The metrics label.
    fn label(self) -> &'static str {
        match self {
            Self::Nonce => "nonce",
            Self::Balance => "balance",
        }
    }

    /// The JSON-RPC result: a hex quantity.
    fn result(self, account: &CommittedAccount) -> String {
        match self {
            Self::Nonce => format!("{:#x}", account.nonce),
            Self::Balance => format!("{:#x}", account.balance),
        }
    }
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
/// snapshot's block and end position when a query ran.
struct Reply {
    status: &'static str,
    body: String,
    state: Option<(u64, u64)>,
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
            state: None,
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

    /// The committed value `method` asked for, with the snapshot's
    /// position.
    fn ok(id: &serde_json::Value, method: Method, account: &CommittedAccount) -> Self {
        metrics::counter!(NONCE_QUERIES, "outcome" => "ok", "method" => method.label())
            .increment(1);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": method.result(account),
        })
        .to_string();
        Self {
            status: "200 OK",
            body,
            state: Some((account.block, account.tx_idx)),
        }
    }

    /// Write the HTTP/1.0 response and close.
    async fn write<W: AsyncWriteExt + Unpin>(&self, wr: &mut W) -> std::io::Result<()> {
        let state_headers = self
            .state
            .map(|(block, tx_idx)| {
                format!("x-state-block: {block}\r\nx-state-tx-idx: {tx_idx}\r\n")
            })
            .unwrap_or_default();
        let head = format!(
            "HTTP/1.0 {}\r\ncontent-type: application/json\r\n{state_headers}\
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

/// The address a well-formed request names.
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
    let Some(method) = Method::parse(&request.method) else {
        return Reply::rpc_error(&request.id, -32601, "method not found");
    };
    let Some(address) = requested_address(&request) else {
        return Reply::rpc_error(
            &request.id,
            -32602,
            "invalid params: expected [address, tag]",
        );
    };
    let env = env.clone();
    let looked_up = tokio::task::spawn_blocking(move || committed_account(&env, address)).await;
    match looked_up {
        Ok(Ok(account)) => Reply::ok(&request.id, method, &account),
        Ok(Err(e)) => {
            warn!(error = %e, %address, "account query: state read failed");
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
                state: None,
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
}
