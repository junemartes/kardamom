//! A stand-in for the L1 light client: same JSON-RPC surface, in-process.
//!
//! The validator reads L1 through a verified endpoint (helios), not a
//! blindly trusted RPC (see
//! `deploy/cluster/nomad/l1-light-client.nomad.hcl`). This test cannot
//! exercise that real client: every kardamom test environment runs anvil
//! as L1, and anvil is execution-only, so there is no beacon chain for a
//! consensus light client to sync from.
//!
//! What this test can check is the half that is ours: the contract
//! between the validator and whatever serves it L1 data. This mock speaks
//! the same `eth_*` methods helios does (`eth_getBlockByNumber`,
//! `eth_getLogs`, `eth_blockNumber`), proxying to anvil so the data is
//! real. It can also be told to lie in specific ways.
//!
//! That second part is the point. A passthrough proves the validator
//! works through an interposed endpoint at all. The fault modes prove it
//! actually rejects a bad L1 view, instead of trusting whatever arrives.
//! Without them, "the validator verifies against L1" is an untested claim.
//!
//! This does not test helios's cryptography: the sync-committee signature
//! checks and the Merkle proofs against the beacon-authenticated roots. A
//! mock cannot stand in for that part, which needs a real network. A
//! green run here does not validate the light client.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// How the mock should misbehave. Every variant is a lie a real endpoint
/// could tell. Each one asks: does the validator notice?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Serve L1 faithfully. This is the baseline: verification must still
    /// pass with an endpoint interposed, or the fault cases prove
    /// nothing.
    None,
    /// Corrupt the `hash` of every block at or above `from_block`. The
    /// epoch on the canonical stream then names a hash that "L1"
    /// disagrees with.
    WrongBlockHash { from_block: u64 },
    /// Corrupt only `parentHash`, and leave each block's own hash intact.
    /// Each block still looks right on its own. Only chaining consecutive
    /// origins catches this, which is the reason the verifier does it.
    BrokenParentChain { from_block: u64 },
    /// Drop every deposit log. The censorship case, seen from the L1 side.
    SwallowLogs,
}

/// A running mock endpoint. Dropping it stops the server.
pub struct VerifiedL1 {
    addr: SocketAddr,
    fault: Arc<std::sync::Mutex<Fault>>,
    /// Requests served. This lets a test prove the validator actually
    /// went through here, instead of reaching anvil directly.
    served: Arc<AtomicU64>,
    _task: tokio::task::JoinHandle<()>,
}

impl VerifiedL1 {
    /// Bind on an ephemeral port and proxy to `upstream` (anvil).
    pub async fn spawn(upstream: &str) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("bind mock verified-L1")?;
        let addr = listener.local_addr()?;
        let client: HttpClient = HttpClientBuilder::default()
            .build(upstream)
            .context("connect mock verified-L1 to anvil")?;
        let fault = Arc::new(std::sync::Mutex::new(Fault::None));
        let served = Arc::new(AtomicU64::new(0));

        let task = tokio::spawn({
            let fault = fault.clone();
            let served = served.clone();
            async move {
                loop {
                    let Ok((mut sock, _)) = listener.accept().await else {
                        return;
                    };
                    let client = client.clone();
                    let fault = fault.clone();
                    let served = served.clone();
                    tokio::spawn(async move {
                        // One request per connection is enough for a mock.
                        // alloy opens as many connections as it needs.
                        if let Ok(Some(body)) = read_http_request(&mut sock).await {
                            // Copy the fault out before awaiting. Holding a
                            // std MutexGuard across an await would make the
                            // future non-Send.
                            let active = *fault.lock().unwrap();
                            let reply = handle(&client, &body, active).await;
                            served.fetch_add(1, Ordering::Relaxed);
                            let bytes = reply.to_string().into_bytes();
                            let head = format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                                bytes.len()
                            );
                            let _ = sock.write_all(head.as_bytes()).await;
                            let _ = sock.write_all(&bytes).await;
                            let _ = sock.flush().await;
                        }
                    });
                }
            }
        });

        Ok(Self {
            addr,
            fault,
            served,
            _task: task,
        })
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Start lying (or stop). Takes effect on the next request.
    pub fn set_fault(&self, f: Fault) {
        *self.fault.lock().unwrap() = f;
    }

    pub fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }
}

/// Read one HTTP request, returning its body. `None` on a clean close.
async fn read_http_request(sock: &mut tokio::net::TcpStream) -> Result<Option<Vec<u8>>> {
    let mut buf = Vec::with_capacity(2048);
    let mut chunk = [0u8; 1024];
    // Headers first: read until the blank line.
    let header_end = loop {
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(i) = find_subslice(&buf, b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = String::from_utf8_lossy(&buf[..header_end]).to_ascii_lowercase();
    let len: usize = headers
        .split("content-length:")
        .nth(1)
        .and_then(|s| s.split("\r\n").next())
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    while buf.len() < header_end + len {
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Ok(Some(buf[header_end..].to_vec()))
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

/// Proxy one JSON-RPC call upstream, then apply the active fault to the reply.
async fn handle(client: &HttpClient, body: &[u8], fault: Fault) -> serde_json::Value {
    let req: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return rpc_error(serde_json::Value::Null, &format!("bad request: {e}")),
    };
    let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
    let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
    let params = req
        .get("params")
        .cloned()
        .unwrap_or(serde_json::Value::Array(vec![]));

    // Params travel through as raw JSON. The mock is a pipe, not a parser.
    let params = match params {
        serde_json::Value::Array(a) => a,
        other => vec![other],
    };
    let mut rpc_args = jsonrpsee::core::params::ArrayParams::new();
    for p in &params {
        // insert() fails only on input that cannot serialize. These values
        // are already serde_json::Value, so it cannot fail here.
        let _ = rpc_args.insert(p);
    }
    let upstream: Result<serde_json::Value, _> = client.request(method, rpc_args).await;
    let mut result = match upstream {
        Ok(v) => v,
        Err(e) => return rpc_error(id, &format!("upstream: {e}")),
    };

    apply_fault(method, &params, &mut result, fault);
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Mutate a faithful upstream reply into the lie under test.
fn apply_fault(
    method: &str,
    params: &[serde_json::Value],
    result: &mut serde_json::Value,
    fault: Fault,
) {
    match fault {
        Fault::None => {}
        Fault::SwallowLogs if method == "eth_getLogs" => {
            *result = serde_json::Value::Array(vec![]);
        }
        Fault::WrongBlockHash { from_block }
            if method == "eth_getBlockByNumber"
                && block_at_or_after(params, result, from_block) =>
        {
            result["hash"] = serde_json::json!(format!("0x{}", "ee".repeat(32)));
        }
        Fault::BrokenParentChain { from_block }
            if method == "eth_getBlockByNumber"
                && block_at_or_after(params, result, from_block) =>
        {
            // The block's own hash stays correct. Only its ancestry is a
            // lie, so only chaining can see it.
            result["parentHash"] = serde_json::json!(format!("0x{}", "ab".repeat(32)));
        }
        _ => {}
    }
}

/// Whether this reply is a block at or after `from_block`.
fn block_at_or_after(
    _params: &[serde_json::Value],
    result: &serde_json::Value,
    from_block: u64,
) -> bool {
    result
        .get("number")
        .and_then(|n| n.as_str())
        .and_then(|h| u64::from_str_radix(h.trim_start_matches("0x"), 16).ok())
        .is_some_and(|n| n >= from_block)
}

fn rpc_error(id: serde_json::Value, msg: &str) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32000, "message": msg },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The mock must actually corrupt what it proxies. Without this check,
    /// a green fault-injection scenario would only prove that nothing
    /// broke. That is the exact failure mode where a drill silently stops
    /// drilling.
    #[tokio::test]
    async fn faults_actually_mutate_the_proxied_reply() {
        let block = serde_json::json!({
            "number": "0x10",
            "hash": format!("0x{}", "11".repeat(32)),
            "parentHash": format!("0x{}", "22".repeat(32)),
        });

        let mut r = block.clone();
        apply_fault("eth_getBlockByNumber", &[], &mut r, Fault::None);
        assert_eq!(r, block, "None must pass through untouched");

        let mut r = block.clone();
        apply_fault(
            "eth_getBlockByNumber",
            &[],
            &mut r,
            Fault::WrongBlockHash { from_block: 0x10 },
        );
        assert_ne!(
            r["hash"], block["hash"],
            "hash must be corrupted at the threshold"
        );

        // Below the threshold, nothing changes. This lets a test arm a
        // fault without invalidating epochs already verified.
        let mut r = block.clone();
        apply_fault(
            "eth_getBlockByNumber",
            &[],
            &mut r,
            Fault::WrongBlockHash { from_block: 0x11 },
        );
        assert_eq!(r, block, "below the threshold must pass through");

        let mut r = block.clone();
        apply_fault(
            "eth_getBlockByNumber",
            &[],
            &mut r,
            Fault::BrokenParentChain { from_block: 0x10 },
        );
        assert_eq!(
            r["hash"], block["hash"],
            "the block's OWN hash stays correct"
        );
        assert_ne!(
            r["parentHash"], block["parentHash"],
            "only ancestry is a lie"
        );

        let mut logs = serde_json::json!([{ "address": "0xabc" }]);
        apply_fault("eth_getLogs", &[], &mut logs, Fault::SwallowLogs);
        assert_eq!(logs, serde_json::json!([]), "logs must be swallowed");
    }
}
