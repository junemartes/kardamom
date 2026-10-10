//! The proxy server: one listener for the JSON-RPC pipe and the control
//! endpoint, one task per connection.

use std::net::{IpAddr, SocketAddr};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use crate::fault::{Caller, Fault, Faults, Refusal};
use crate::http::{Connection, Request, Response};

/// A running proxy. Dropping it stops the server.
pub struct FaultProxy {
    addr: SocketAddr,
    /// The active faults: one writer per control request, one reader per
    /// in-flight connection. A `watch` channel, not a mutex: a reader
    /// borrows the latest value with no lock to poison, and holds no
    /// guard across the `.await` that follows.
    faults: watch::Sender<Faults>,
    /// The JSON-RPC requests forwarded. A test proves a follower went
    /// through the proxy, not around it, with this count.
    served: Arc<AtomicU64>,
    task: tokio::task::JoinHandle<()>,
}

impl FaultProxy {
    /// Bind on `bind` and forward to `upstream`, an L1 JSON-RPC URL.
    ///
    /// # Errors
    /// Returns an error when the port fails to bind or the upstream
    /// client cannot be built.
    pub async fn spawn(upstream: &str, bind: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(bind)
            .await
            .with_context(|| format!("bind the fault proxy on {bind}"))?;
        let addr = listener.local_addr()?;
        let upstream = Upstream::new(upstream)?;
        let (faults, _) = watch::channel(Faults::default());
        let served = Arc::new(AtomicU64::new(0));
        let conn = Conn {
            upstream,
            faults: faults.clone(),
            served: served.clone(),
        };
        let task = tokio::spawn(accept_loop(listener, conn));
        Ok(Self {
            addr,
            faults,
            served,
            task,
        })
    }

    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Make `fault` the one active fault. `Fault::None` stops lying.
    /// Takes effect on the next request.
    pub fn set_fault(&self, fault: Fault) {
        self.set_faults(Faults::one(fault));
    }

    /// Replace the active list. Takes effect on the next request.
    pub fn set_faults(&self, faults: Faults) {
        self.faults.send_replace(faults);
    }

    #[must_use]
    pub fn faults(&self) -> Faults {
        self.faults.borrow().clone()
    }

    #[must_use]
    pub fn served(&self) -> u64 {
        self.served.load(Ordering::Relaxed)
    }

    /// Serve until the listener fails. The binary's main waits here.
    pub async fn serve_forever(mut self) {
        let _ = (&mut self.task).await;
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Accept connections until the listener errors, one [`Conn::serve`]
/// task per connection.
async fn accept_loop(listener: TcpListener, conn: Conn) {
    while accept_one(&listener, &conn).await.is_continue() {}
}

/// Accept one connection and spawn its task. `Break` when the listener
/// itself failed: the loop's exit condition.
async fn accept_one(listener: &TcpListener, conn: &Conn) -> ControlFlow<()> {
    let Ok((sock, _)) = listener.accept().await else {
        return ControlFlow::Break(());
    };
    tokio::spawn(conn.clone().serve(sock));
    ControlFlow::Continue(())
}

/// The upstream L1 endpoint, reached with one client for every
/// connection the proxy serves.
#[derive(Clone)]
struct Upstream {
    client: reqwest::Client,
    url: String,
}

impl Upstream {
    fn new(url: &str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .build()
            .context("build the upstream client")?;
        Ok(Self {
            client,
            url: url.to_string(),
        })
    }

    /// Forward one raw JSON-RPC body and parse the reply.
    async fn forward(&self, body: &[u8]) -> Result<Value> {
        self.client
            .post(&self.url)
            .header("content-type", "application/json")
            .body(body.to_vec())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .context("parse the upstream reply")
    }
}

/// One connection's serving state, cloned from the shared state once per
/// connection.
#[derive(Clone)]
struct Conn {
    upstream: Upstream,
    faults: watch::Sender<Faults>,
    served: Arc<AtomicU64>,
}

impl Conn {
    /// Serve requests on `sock` until the peer closes, a reply closes, or
    /// a request is malformed.
    async fn serve(self, sock: TcpStream) {
        let client = sock.peer_addr().ok().map(|addr| addr.ip());
        let mut conn = Connection::new(sock);
        while self.step(&mut conn, client).await.is_continue() {}
    }

    /// One request: read it, answer it, and decide whether the
    /// connection goes on.
    async fn step(&self, conn: &mut Connection, client: Option<IpAddr>) -> ControlFlow<()> {
        let Ok(Some(request)) = conn.read_request().await else {
            return ControlFlow::Break(());
        };
        let mut response = self.respond(&request, client).await;
        response.close |= request.close;
        if conn.write(&response).await.is_err() || response.close {
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    async fn respond(&self, request: &Request, client: Option<IpAddr>) -> Response {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => Response::json(200, &serde_json::json!({ "ok": true })),
            ("GET", "/fault") => self.active(),
            ("POST", "/fault") => self.set(&request.body),
            ("POST", path) => {
                let caller = Caller {
                    client,
                    second: path == "/second",
                };
                self.proxy(&request.body, caller).await
            }
            _ => Response::json(404, &serde_json::json!({ "error": "no such path" })),
        }
    }

    /// The control endpoint's answer: the active list.
    fn active(&self) -> Response {
        Response::json(200, &serde_json::json!({ "active": *self.faults.borrow() }))
    }

    /// `POST /fault`: one fault object or a list of them replaces the
    /// active list.
    fn set(&self, body: &[u8]) -> Response {
        let faults = match parse_faults(body) {
            Ok(faults) => faults,
            Err(e) => {
                return Response::json(400, &serde_json::json!({ "error": e.to_string() }));
            }
        };
        tracing::info!(?faults, "faults set");
        self.faults.send_replace(faults);
        self.active()
    }

    /// Forward one JSON-RPC body, or refuse it, and apply the active
    /// faults to the reply.
    async fn proxy(&self, body: &[u8], caller: Caller) -> Response {
        self.served.fetch_add(1, Ordering::Relaxed);
        let calls: Value = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return rpc_error_response(400, &Value::Null, &format!("bad request: {e}")),
        };
        let faults = self.faults.borrow().clone();
        if let Some(refusal) = faults.refusal_for(caller) {
            return refuse(refusal, &calls);
        }
        let mut reply = match self.upstream.forward(body).await {
            Ok(v) => v,
            Err(e) => return rpc_error_response(502, &calls["id"], &format!("upstream: {e:#}")),
        };
        mutate(&faults, caller, &calls, &mut reply);
        Response::json(200, &reply)
    }
}

/// A fault object or a list of them, without the no-op entries.
fn parse_faults(body: &[u8]) -> Result<Faults> {
    let value: Value = serde_json::from_slice(body).context("the body is not JSON")?;
    let faults = match value {
        Value::Array(_) => serde_json::from_value::<Faults>(value),
        _ => serde_json::from_value::<Fault>(value).map(Faults::one),
    }
    .context("the body is not a fault or a list of faults")?;
    Ok(faults.without_none())
}

/// The reply of a `Down` or a `RateLimit` fault.
fn refuse(refusal: Refusal, calls: &Value) -> Response {
    match refusal {
        Refusal::RateLimited => rpc_error_response(429, &calls["id"], "rate limited"),
        Refusal::Down => Response {
            close: true,
            ..rpc_error_response(503, &calls["id"], "the endpoint is down")
        },
    }
}

/// Apply `faults` to a reply: to each element of a batch, matched to its
/// call by id, or to the one reply of the one call.
fn mutate(faults: &Faults, caller: Caller, calls: &Value, reply: &mut Value) {
    match (calls, reply) {
        (Value::Array(calls), Value::Array(replies)) => replies
            .iter_mut()
            .for_each(|r| mutate_one(faults, caller, method_of(calls, &r["id"]), r)),
        (call, reply) => mutate_one(faults, caller, call["method"].as_str(), reply),
    }
}

fn mutate_one(faults: &Faults, caller: Caller, method: Option<&str>, reply: &mut Value) {
    if let (Some(method), Some(result)) = (method, reply.get_mut("result")) {
        faults.apply_from(caller, method, result);
    }
}

/// The method of the call in `calls` with `id`.
fn method_of<'a>(calls: &'a [Value], id: &Value) -> Option<&'a str> {
    calls
        .iter()
        .find(|c| c["id"] == *id)
        .and_then(|c| c["method"].as_str())
}

fn rpc_error_response(status: u16, id: &Value, msg: &str) -> Response {
    Response::json(
        status,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": -32000, "message": msg },
        }),
    )
}

#[cfg(test)]
mod tests;
