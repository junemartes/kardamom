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
use std::time::Duration;

use alloy_primitives::Address;
use kardamom_types::StateDatabase;
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
pub fn serve_nonce_queries(addr: SocketAddr, env: StateEnv) -> std::io::Result<NonceQueryServer> {
    let std_listener = std::net::TcpListener::bind(addr)?;
    std_listener.set_nonblocking(true)?;
    let listener = TcpListener::from_std(std_listener)?;
    let addr = listener.local_addr()?;
    info!(%addr, "serving account nonce queries");
    let task = tokio::spawn(async move {
        loop {
            let stream = match listener.accept().await {
                Ok((s, _)) => s,
                Err(e) => {
                    warn!(error = %e, "nonce query accept failed");
                    continue;
                }
            };
            let env = env.clone();
            tokio::spawn(async move {
                if let Err(e) = serve_one(stream, env).await {
                    warn!(error = %e, "nonce query connection failed");
                }
            });
        }
    });
    Ok(NonceQueryServer { addr, task })
}

/// The committed nonce of `address`, and the block of the snapshot that
/// answered. An unknown account has nonce 0.
pub fn committed_nonce(env: &StateEnv, address: Address) -> Result<(u64, u64), StateError> {
    let snapshot = StateSnapshot::open(env)?;
    let nonce = snapshot.basic(address)?.map(|(n, _, _)| n).unwrap_or(0);
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

fn rpc_error(id: serde_json::Value, code: i64, message: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message },
    })
    .to_string()
}

/// Answer one request body. Returns the HTTP status, the JSON body, and
/// the snapshot block when a query ran.
async fn answer(env: &StateEnv, body: &[u8]) -> (&'static str, String, Option<u64>) {
    let request: Request = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(_) => {
            metrics::counter!(NONCE_QUERIES, "outcome" => "bad_request").increment(1);
            return (
                "400 Bad Request",
                rpc_error(serde_json::Value::Null, -32700, "parse error"),
                None,
            );
        }
    };
    if request.method != "eth_getTransactionCount" {
        metrics::counter!(NONCE_QUERIES, "outcome" => "bad_request").increment(1);
        return (
            "200 OK",
            rpc_error(request.id, -32601, "method not found"),
            None,
        );
    }
    let address = request
        .params
        .first()
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<Address>().ok());
    let Some(address) = address else {
        metrics::counter!(NONCE_QUERIES, "outcome" => "bad_request").increment(1);
        return (
            "200 OK",
            rpc_error(
                request.id,
                -32602,
                "invalid params: expected [address, tag]",
            ),
            None,
        );
    };
    let env = env.clone();
    let looked_up = tokio::task::spawn_blocking(move || committed_nonce(&env, address)).await;
    match looked_up {
        Ok(Ok((nonce, block))) => {
            metrics::counter!(NONCE_QUERIES, "outcome" => "ok").increment(1);
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "id": request.id,
                "result": format!("{nonce:#x}"),
            })
            .to_string();
            ("200 OK", body, Some(block))
        }
        Ok(Err(e)) => {
            metrics::counter!(NONCE_QUERIES, "outcome" => "error").increment(1);
            warn!(error = %e, %address, "nonce query: state read failed");
            (
                "500 Internal Server Error",
                rpc_error(request.id, -32603, "state read failed"),
                None,
            )
        }
        Err(e) => {
            metrics::counter!(NONCE_QUERIES, "outcome" => "error").increment(1);
            warn!(error = %e, "nonce query: blocking task failed");
            (
                "500 Internal Server Error",
                rpc_error(request.id, -32603, "internal error"),
                None,
            )
        }
    }
}

async fn serve_one(stream: TcpStream, env: StateEnv) -> std::io::Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd).take((MAX_HEAD + MAX_BODY) as u64);
    let mut line = String::new();
    timeout(IO_TIMEOUT, reader.read_line(&mut line)).await??;
    if !line.starts_with("POST ") {
        return write_response(&mut wr, "404 Not Found", "", None).await;
    }
    let mut content_length = 0usize;
    loop {
        line.clear();
        let n = timeout(IO_TIMEOUT, reader.read_line(&mut line)).await??;
        let header = line.trim_end();
        if n == 0 || header.is_empty() {
            break;
        }
        if let Some((key, value)) = header.split_once(':')
            && key.trim().eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    if content_length == 0 || content_length > MAX_BODY {
        return write_response(
            &mut wr,
            "400 Bad Request",
            &rpc_error(serde_json::Value::Null, -32600, "missing or oversized body"),
            None,
        )
        .await;
    }
    let mut body = vec![0u8; content_length];
    timeout(IO_TIMEOUT, reader.read_exact(&mut body)).await??;
    let (status, reply, block) = answer(&env, &body).await;
    write_response(&mut wr, status, &reply, block).await
}

async fn write_response<W: AsyncWriteExt + Unpin>(
    wr: &mut W,
    status: &str,
    body: &str,
    block: Option<u64>,
) -> std::io::Result<()> {
    let block_header = block
        .map(|b| format!("x-state-block: {b}\r\n"))
        .unwrap_or_default();
    let head = format!(
        "HTTP/1.0 {status}\r\ncontent-type: application/json\r\n{block_header}\
         content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    timeout(IO_TIMEOUT, wr.write_all(head.as_bytes())).await??;
    timeout(IO_TIMEOUT, wr.write_all(body.as_bytes())).await??;
    timeout(IO_TIMEOUT, wr.flush()).await??;
    Ok(())
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
