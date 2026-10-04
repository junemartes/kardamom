//! A read-only query on the executor node: the committed nonce or
//! balance of one address, or the committed receipt of one transaction.
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
//! The ingress asks for a receipt by hash when its own receipt cache
//! misses. That cache lives in the memory of one ingress process, so a
//! restarted ingress holds no receipt from before its start, and the
//! state DB is the durable copy. The method is
//! `eth_getTransactionReceipt` with `[hash]`. The result is `null`, or
//! the rkyv bytes of the stored [`Receipt`] as `0x` hex, the format of
//! the `receipts` table, so the two sides share one type and no second
//! encoding.
//!
//! The batcher asks for a block's references when the sealer no longer
//! retains the block: `kardamom_getBlockRefs` with `[number]`, the number
//! as a JSON integer or a `0x` quantity. The result is `null`, or the
//! [`BlockRefs`] record as JSON: the block's canonical end, its L1 origin
//! and timestamp, and `(tx_hash, tx_idx, shard_id, session_id, position)`
//! for each transaction in canonical order. A committed block whose
//! references cannot rebuild its payload is the JSON-RPC error -32001,
//! with the cause: this node rebuilt the block from L1, or a transaction
//! other than a deposit has no reference.
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

use alloy_primitives::{Address, B256, U256};
use kardamom_types::num::usize_to_u64;
use kardamom_types::{Receipt, StateDatabase};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{info, warn};

use crate::env::StateEnv;
use crate::error::StateError;
use crate::snapshot::{BlockRefs, StateSnapshot};

const MAX_HEAD: usize = 8 * 1024;
const MAX_BODY: usize = 8 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Queries served, by `outcome` (`ok`, `bad_request`, `refused`, `error`).
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

/// The committed receipt of one transaction, and where the snapshot
/// that gave it stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedReceipt {
    /// `None` when the state holds no transaction with that hash.
    pub receipt: Option<Receipt>,
    /// The snapshot's block.
    pub block: u64,
    /// The canonical end position of the snapshot's last block, as an
    /// index.
    pub tx_idx: u64,
}

/// Read the committed receipt of `tx_hash` from a fresh snapshot.
///
/// # Errors
///
/// Returns [`StateError`] if the snapshot cannot open or a read fails.
pub fn committed_receipt(env: &StateEnv, tx_hash: B256) -> Result<CommittedReceipt, StateError> {
    let snapshot = StateSnapshot::open(env)?;
    let receipt = snapshot
        .get_tx_position(tx_hash)?
        .map(|position| snapshot.get_receipt(position))
        .transpose()?
        .flatten();
    Ok(CommittedReceipt {
        receipt,
        block: snapshot.block_number(),
        tx_idx: snapshot.end_tx_position()?.as_index(),
    })
}

/// The references of one block, and where the snapshot that gave them
/// stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedRefs {
    /// `None` when the block is not committed yet.
    pub refs: Option<BlockRefs>,
    /// The snapshot's block.
    pub block: u64,
    /// The canonical end position of the snapshot's last block, as an
    /// index.
    pub tx_idx: u64,
}

/// Read the references of block `number` from a fresh snapshot.
///
/// # Errors
///
/// Returns [`StateError`] if the snapshot cannot open or the read fails,
/// or when the block cannot be rebuilt from references.
pub fn committed_block_refs(env: &StateEnv, number: u64) -> Result<CommittedRefs, StateError> {
    let snapshot = StateSnapshot::open(env)?;
    Ok(CommittedRefs {
        refs: snapshot.block_refs(number)?,
        block: snapshot.block_number(),
        tx_idx: snapshot.end_tx_position()?.as_index(),
    })
}

/// One request, parsed once at the boundary: an account method with its
/// address, a receipt lookup with its hash, or a block's references with
/// its number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Query {
    Account(Method, Address),
    Receipt(B256),
    Refs(u64),
}

/// The JSON-RPC name of the receipt lookup.
const RECEIPT_METHOD: &str = "eth_getTransactionReceipt";
/// The JSON-RPC name of the block references lookup.
const REFS_METHOD: &str = "kardamom_getBlockRefs";
/// The JSON-RPC error code of a committed block whose references cannot
/// rebuild its payload. The message names the cause.
const NO_BLOCK_REFS: i64 = -32001;

/// A block number parameter: a JSON integer, or a `0x` quantity.
fn block_number_param(value: &serde_json::Value) -> Option<u64> {
    value.as_u64().or_else(|| {
        value
            .as_str()
            .and_then(|s| s.strip_prefix("0x"))
            .and_then(|hex| u64::from_str_radix(hex, 16).ok())
    })
}

impl Query {
    /// The query of `request`, or the JSON-RPC error code and message.
    fn parse(request: &Request) -> Result<Self, (i64, &'static str)> {
        if request.method == REFS_METHOD {
            return request
                .params
                .first()
                .and_then(block_number_param)
                .map(Self::Refs)
                .ok_or((-32602, "invalid params: expected [number]"));
        }
        let first = request.params.first().and_then(serde_json::Value::as_str);
        if request.method == RECEIPT_METHOD {
            return first
                .and_then(|s| s.parse::<B256>().ok())
                .map(Self::Receipt)
                .ok_or((-32602, "invalid params: expected [hash]"));
        }
        let method = Method::parse(&request.method).ok_or((-32601, "method not found"))?;
        first
            .and_then(|s| s.parse::<Address>().ok())
            .map(|address| Self::Account(method, address))
            .ok_or((-32602, "invalid params: expected [address, tag]"))
    }
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

    /// A committed block whose references cannot rebuild its payload.
    fn refused(id: &serde_json::Value, message: &str) -> Self {
        Self::error("200 OK", "refused", id, NO_BLOCK_REFS, message)
    }

    /// A failed state read.
    fn internal(id: &serde_json::Value, message: &str) -> Self {
        Self::error("500 Internal Server Error", "error", id, -32603, message)
    }

    /// The committed receipt as the hex of its stored bytes, or `null`,
    /// with the snapshot's position.
    fn receipt(id: &serde_json::Value, found: &CommittedReceipt) -> Self {
        metrics::counter!(NONCE_QUERIES, "outcome" => "ok", "method" => "receipt").increment(1);
        let result = found.receipt.as_ref().map(|r| {
            format!(
                "0x{}",
                alloy_primitives::hex::encode(crate::schema::encode_receipt_value(r))
            )
        });
        let body = serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string();
        Self {
            status: "200 OK",
            body,
            state: Some((found.block, found.tx_idx)),
        }
    }

    /// The block's references as JSON, or `null`, with the snapshot's
    /// position.
    fn refs(id: &serde_json::Value, found: &CommittedRefs) -> Self {
        metrics::counter!(NONCE_QUERIES, "outcome" => "ok", "method" => "refs").increment(1);
        let body =
            serde_json::json!({"jsonrpc": "2.0", "id": id, "result": found.refs}).to_string();
        Self {
            status: "200 OK",
            body,
            state: Some((found.block, found.tx_idx)),
        }
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

/// Answer one request body.
async fn answer(env: &StateEnv, body: &[u8]) -> Reply {
    let Ok(request) = serde_json::from_slice::<Request>(body) else {
        return Reply::bad_request(-32700, "parse error");
    };
    let query = match Query::parse(&request) {
        Ok(query) => query,
        Err((code, message)) => return Reply::rpc_error(&request.id, code, message),
    };
    let env = env.clone();
    let looked_up = tokio::task::spawn_blocking(move || query.read(&env)).await;
    match looked_up {
        Ok(Ok(found)) => found.reply(&request.id),
        Ok(Err(e @ StateError::NoBlockRefs { .. })) => Reply::refused(&request.id, &e.to_string()),
        Ok(Err(e)) => {
            warn!(error = %e, ?query, "state query: state read failed");
            Reply::internal(&request.id, "state read failed")
        }
        Err(e) => {
            warn!(error = %e, "state query: blocking task failed");
            Reply::internal(&request.id, "internal error")
        }
    }
}

/// What a [`Query`] read from the state. The receipt is boxed: it is
/// several times the account's size, and one query reads one of them.
enum Found {
    Account(Method, CommittedAccount),
    Receipt(Box<CommittedReceipt>),
    Refs(CommittedRefs),
}

impl Query {
    /// Read the answer from a fresh snapshot. Blocking: mdbx reads.
    fn read(self, env: &StateEnv) -> Result<Found, StateError> {
        match self {
            Self::Account(method, address) => {
                committed_account(env, address).map(|account| Found::Account(method, account))
            }
            Self::Receipt(tx_hash) => {
                committed_receipt(env, tx_hash).map(|found| Found::Receipt(Box::new(found)))
            }
            Self::Refs(number) => committed_block_refs(env, number).map(Found::Refs),
        }
    }
}

impl Found {
    fn reply(&self, id: &serde_json::Value) -> Reply {
        match self {
            Self::Account(method, account) => Reply::ok(id, *method, account),
            Self::Receipt(found) => Reply::receipt(id, found),
            Self::Refs(found) => Reply::refs(id, found),
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
#[path = "nonce_query_tests.rs"]
mod tests;
