//! The HTTP side of the egress: the range read of a reader that resumes,
//! and the health check.
//!
//! `GET /v1/frames?from_index=<i>&from_block=<b>&limit=<bytes>` answers:
//! - `200` with the frames after the cursor, each as `[len:u32][frame]`,
//!   little endian, the cursor after the last of them in the headers
//!   `x-egress-after-index` and `x-egress-after-block`, and the head of the
//!   store in `x-egress-next-index` and `x-egress-next-block`. A reader
//!   whose cursor after is below the head asks again from it;
//! - `410` when the store holds nothing that old, with its floor in
//!   `x-egress-floor-index` and `x-egress-floor-block`;
//! - `400` for a bad query, `503` when the owner thread is gone.
//!
//! `POST /` with the JSON-RPC call `eth_getTransactionReceipt` answers as
//! the state query of an executor does: the receipt as the `0x` hex of its
//! rkyv bytes, or `null`, with the block of the newest receipt in the
//! header `x-state-block`. So a client of the executor query reads the
//! egress with no change.
//!
//! `GET /health` answers `200 ok`.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::B256;
use bytes::Bytes;
use crossbeam_channel::Sender;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::merge::Head;
use crate::owner::{Input, ReadRequest};
use crate::store::Range;

/// The largest read that one request takes, in bytes.
const MAX_LIMIT: usize = 16 * 1024 * 1024;
/// The pause after a failed accept.
const ACCEPT_RETRY: Duration = Duration::from_millis(100);

/// The listener of the egress.
pub struct Http {
    listener: TcpListener,
    inputs: Sender<Input>,
}

impl Http {
    /// Bind `addr`. Reads go to the owner thread on `inputs`.
    ///
    /// # Errors
    ///
    /// Returns an error when the bind fails.
    pub async fn bind(addr: SocketAddr, inputs: Sender<Input>) -> std::io::Result<Self> {
        Ok(Self {
            listener: TcpListener::bind(addr).await?,
            inputs,
        })
    }

    /// The bound address.
    ///
    /// # Errors
    ///
    /// Returns an error when the socket has no address.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serve until the task is dropped.
    pub async fn serve(self) {
        loop {
            self.accept_one().await;
        }
    }

    async fn accept_one(&self) {
        match self.listener.accept().await {
            Ok((stream, _)) => {
                let inputs = self.inputs.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let inputs = inputs.clone();
                        async move { Ok::<_, Infallible>(respond(req, &inputs).await) }
                    });
                    if let Err(e) = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        tracing::debug!(error = %e, "egress: connection ended with an error");
                    }
                });
            }
            Err(e) => {
                tracing::warn!(error = %e, "egress: accept failed");
                tokio::time::sleep(ACCEPT_RETRY).await;
            }
        }
    }
}

async fn respond(req: Request<Incoming>, inputs: &Sender<Input>) -> Response<Full<Bytes>> {
    if req.method() == hyper::Method::POST && req.uri().path() == "/" {
        return receipt_call(req, inputs).await;
    }
    match req.uri().path() {
        "/health" => reply(StatusCode::OK, Bytes::from_static(b"ok\n")),
        "/v1/frames" => match Cursor::parse(req.uri().query().unwrap_or_default()) {
            Some(cursor) => cursor.read(inputs).await,
            None => reply(
                StatusCode::BAD_REQUEST,
                Bytes::from_static(b"from_index and from_block are required\n"),
            ),
        },
        _ => reply(StatusCode::NOT_FOUND, Bytes::from_static(b"not found\n")),
    }
}

/// The cursor and the limit of one range read.
struct Cursor {
    from_index: u64,
    from_block: u64,
    limit: usize,
}

impl Cursor {
    /// Parse `from_index`, `from_block` and the optional `limit`.
    fn parse(query: &str) -> Option<Self> {
        let pairs: HashMap<&str, &str> = query
            .split('&')
            .filter_map(|pair| pair.split_once('='))
            .collect();
        let limit = pairs
            .get("limit")
            .map_or(Some(MAX_LIMIT), |l| l.parse().ok())?;
        Some(Self {
            from_index: pairs.get("from_index")?.parse().ok()?,
            from_block: pairs.get("from_block")?.parse().ok()?,
            limit: limit.min(MAX_LIMIT),
        })
    }

    async fn read(self, inputs: &Sender<Input>) -> Response<Full<Bytes>> {
        let (reply_tx, answer) = oneshot::channel();
        let sent = inputs.try_send(Input::Read(ReadRequest {
            from_index: self.from_index,
            from_block: self.from_block,
            limit: self.limit,
            reply: reply_tx,
        }));
        let range = match (sent, answer.await) {
            (Ok(()), Ok(Ok(range))) => range,
            (Ok(()), Ok(Err(e))) => {
                tracing::error!(error = %e, "egress: a range read failed");
                return reply(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Bytes::from(format!("{e}\n")),
                );
            }
            _ => {
                return reply(
                    StatusCode::SERVICE_UNAVAILABLE,
                    Bytes::from_static(b"the store is busy or gone\n"),
                );
            }
        };
        match range {
            Range::Frames {
                frames,
                after,
                head,
            } => frames_reply(&frames, after, head),
            Range::Unavailable { floor } => with_head(
                reply(StatusCode::GONE, Bytes::from_static(b"below the floor\n")),
                "floor",
                floor,
            ),
        }
    }
}

/// The largest JSON-RPC body taken.
const MAX_CALL: usize = 64 * 1024;

/// One JSON-RPC call: `eth_getTransactionReceipt` with one hash.
#[derive(serde::Deserialize)]
struct Call {
    #[serde(default)]
    id: serde_json::Value,
    method: String,
    #[serde(default)]
    params: Vec<B256>,
}

async fn receipt_call(req: Request<Incoming>, inputs: &Sender<Input>) -> Response<Full<Bytes>> {
    let Ok(body) = http_body_util::Limited::new(req.into_body(), MAX_CALL)
        .collect()
        .await
        .map(http_body_util::Collected::to_bytes)
    else {
        return reply(StatusCode::BAD_REQUEST, Bytes::from_static(b"body\n"));
    };
    let call: Call = match serde_json::from_slice(&body) {
        Ok(call) => call,
        Err(e) => return json_error(&serde_json::Value::Null, -32700, &e.to_string()),
    };
    let (Some(tx_hash), "eth_getTransactionReceipt") = (call.params.first(), call.method.as_str())
    else {
        return json_error(&call.id, -32601, "only eth_getTransactionReceipt");
    };
    let (reply_tx, answer) = oneshot::channel();
    let asked = inputs.try_send(Input::Receipt {
        tx_hash: *tx_hash,
        reply: reply_tx,
    });
    let (Ok(()), Ok((receipt, block))) = (asked, answer.await) else {
        return reply(
            StatusCode::SERVICE_UNAVAILABLE,
            Bytes::from_static(b"the store is busy or gone\n"),
        );
    };
    let result = receipt
        .and_then(|r| rkyv::to_bytes::<rkyv::rancor::Error>(&r).ok())
        .map(|bytes| format!("0x{}", alloy_primitives::hex::encode(bytes.as_slice())));
    let body = serde_json::json!({"jsonrpc": "2.0", "id": call.id, "result": result});
    let mut response = reply(StatusCode::OK, Bytes::from(body.to_string()));
    response
        .headers_mut()
        .insert("x-state-block", hyper::header::HeaderValue::from(block));
    response
}

fn json_error(id: &serde_json::Value, code: i32, message: &str) -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message},
    });
    reply(StatusCode::OK, Bytes::from(body.to_string()))
}

fn frames_reply(frames: &[Vec<u8>], after: Head, head: Head) -> Response<Full<Bytes>> {
    let body = frames.iter().try_fold(Vec::new(), |mut body, frame| {
        body.extend_from_slice(&u32::try_from(frame.len()).ok()?.to_le_bytes());
        body.extend_from_slice(frame);
        Some(body)
    });
    match body {
        Some(body) => with_head(
            with_head(reply(StatusCode::OK, Bytes::from(body)), "next", head),
            "after",
            after,
        ),
        None => reply(
            StatusCode::INTERNAL_SERVER_ERROR,
            Bytes::from_static(b"a frame above 4 GiB\n"),
        ),
    }
}

/// Put `head` in the headers `x-egress-<name>-index` and `-block`.
fn with_head(mut response: Response<Full<Bytes>>, name: &str, head: Head) -> Response<Full<Bytes>> {
    [
        (format!("x-egress-{name}-index"), head.next_index),
        (format!("x-egress-{name}-block"), head.next_block),
    ]
    .into_iter()
    .filter_map(|(key, value)| {
        Some((
            hyper::header::HeaderName::try_from(key).ok()?,
            hyper::header::HeaderValue::from(value),
        ))
    })
    .for_each(|(key, value)| {
        response.headers_mut().insert(key, value);
    });
    response
}

fn reply(status: StatusCode, body: Bytes) -> Response<Full<Bytes>> {
    let mut response = Response::new(Full::new(body));
    *response.status_mut() = status;
    response
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
