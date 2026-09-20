//! The executor query client: the third read layer, on a cache miss.
//!
//! One JSON-RPC POST over HTTP/1.0 to an executor's account query
//! (`crates/state/src/nonce_query.rs`). The endpoints rotate per query,
//! and a query walks the list until one endpoint answers within the
//! timeout. The answer carries the snapshot's block and end position, so
//! a caller writes it back through the monotone rule tagged with that
//! position, never with the head.

use std::num::{NonZeroU64, NonZeroUsize};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use alloy_primitives::{Address, B256, U256};
use kardamom_types::Receipt;
use serde::{Deserialize, Serialize};

/// The `[executor_query]` section. Off when the endpoint list is empty.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct ExecutorQueryConfig {
    /// The executor query endpoints, as `http://host:port`. Empty means
    /// no query: a miss stays a miss.
    pub endpoints: Vec<String>,
    /// The bound of one query, in ms. Never zero.
    pub timeout_ms: NonZeroU64,
    /// The bound of concurrent queries. A burst of cold addresses becomes
    /// at most this many snapshots on the executors. Never zero.
    pub max_in_flight: NonZeroUsize,
}

impl Default for ExecutorQueryConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            timeout_ms: NonZeroU64::new(2_000).unwrap(),
            max_in_flight: NonZeroUsize::new(64).unwrap(),
        }
    }
}

impl ExecutorQueryConfig {
    #[must_use]
    pub fn enabled(&self) -> bool {
        !self.endpoints.is_empty()
    }

    #[must_use]
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.get())
    }
}

/// Which committed value to ask for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryMethod {
    Nonce,
    Balance,
}

impl QueryMethod {
    fn name(self) -> &'static str {
        match self {
            Self::Nonce => "eth_getTransactionCount",
            Self::Balance => "eth_getBalance",
        }
    }
}

/// One answer: the value, and where the snapshot that gave it stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryAnswer {
    pub value: U256,
    pub block: u64,
    /// The snapshot's end position, as an index. Zero when the executor
    /// did not send the header.
    pub tx_idx: u64,
}

/// Why a query gave no answer. Every endpoint failed; the last reason.
#[derive(Debug, thiserror::Error)]
#[error("executor query failed: {reason}")]
pub struct QueryError {
    reason: String,
    timed_out: bool,
}

impl QueryError {
    /// A failure with its reason, and whether it was the timeout.
    #[must_use]
    pub fn new(reason: String, timed_out: bool) -> Self {
        Self { reason, timed_out }
    }

    /// Whether the last endpoint failed on the timeout. A metric label.
    #[must_use]
    pub fn is_timeout(&self) -> bool {
        self.timed_out
    }
}

/// The client. Cheap to clone; every clone shares the connection pool,
/// the rotation counter, and the in-flight bound.
#[derive(Clone)]
pub struct ExecutorQuery {
    client: reqwest::Client,
    endpoints: Arc<[String]>,
    next: Arc<AtomicUsize>,
    in_flight: Arc<tokio::sync::Semaphore>,
}

impl ExecutorQuery {
    /// Build the client. `None` when the config names no endpoint.
    ///
    /// # Panics
    ///
    /// Panics when the HTTP client cannot be built, the same contract as
    /// `reqwest::Client::new`: only a broken TLS backend does that, and
    /// it happens at startup.
    #[must_use]
    pub fn new(cfg: &ExecutorQueryConfig) -> Option<Self> {
        if !cfg.enabled() {
            return None;
        }
        let client = reqwest::Client::builder()
            .timeout(cfg.timeout())
            .build()
            .expect("reqwest client: the TLS backend failed to start");
        Some(Self {
            client,
            endpoints: cfg.endpoints.clone().into(),
            next: Arc::new(AtomicUsize::new(0)),
            in_flight: Arc::new(tokio::sync::Semaphore::new(cfg.max_in_flight.get())),
        })
    }

    /// The committed nonce of `address`: the next nonce it must use.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] when every endpoint failed, or when the
    /// in-flight bound is reached.
    pub async fn nonce(&self, address: Address) -> Result<QueryAnswer, QueryError> {
        self.query(QueryMethod::Nonce, address).await
    }

    /// The committed balance of `address`.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] when every endpoint failed, or when the
    /// in-flight bound is reached.
    pub async fn balance(&self, address: Address) -> Result<QueryAnswer, QueryError> {
        self.query(QueryMethod::Balance, address).await
    }

    /// The committed receipt of `tx_hash`, from an executor's state DB.
    /// `None` when the executor that answered holds no such transaction.
    /// The ingress asks on a miss of its own receipt cache: that cache
    /// lives in one process, so a restarted ingress holds no receipt from
    /// before its start.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError`] when every endpoint failed, or when the
    /// in-flight bound is reached.
    pub async fn receipt(&self, tx_hash: B256) -> Result<Option<Receipt>, QueryError> {
        let body = receipt_request_body(tx_hash);
        let raw = self.rotate(&body).await?;
        parse_receipt_answer(&raw.text).map_err(|reason| QueryError::new(reason, false))
    }

    /// An account query: the rotation, then the hex quantity.
    async fn query(
        &self,
        method: QueryMethod,
        address: Address,
    ) -> Result<QueryAnswer, QueryError> {
        let raw = self.rotate(&request_body(method, address)).await?;
        let value = parse_answer(&raw.text).map_err(|reason| QueryError::new(reason, false))?;
        Ok(QueryAnswer {
            value,
            block: raw.block,
            tx_idx: raw.tx_idx,
        })
    }

    /// Send `body` to the endpoints in rotation. The first HTTP answer
    /// wins. A query over the in-flight bound is shed at once, so a flood
    /// of cold addresses, or of polls for receipts that do not exist yet,
    /// never queues on the executors.
    async fn rotate(&self, body: &str) -> Result<RawAnswer, QueryError> {
        let Ok(_permit) = self.in_flight.try_acquire() else {
            return Err(QueryError::new("in-flight bound reached".into(), false));
        };
        let n = self.endpoints.len();
        let first = self.next.fetch_add(1, Ordering::Relaxed) % n;
        let mut last_err = QueryError::new("no executor endpoints".into(), false);
        for endpoint in (0..n).map(|i| &self.endpoints[first.saturating_add(i) % n]) {
            if let ControlFlow::Break(answer) =
                self.try_endpoint(endpoint, body, &mut last_err).await
            {
                return Ok(answer);
            }
        }
        Err(last_err)
    }

    /// One endpoint of the rotation: `Break` carries the answer; on
    /// failure the reason lands in `last_err`.
    async fn try_endpoint(
        &self,
        endpoint: &str,
        body: &str,
        last_err: &mut QueryError,
    ) -> ControlFlow<RawAnswer> {
        match self.query_one(endpoint, body).await {
            Ok(answer) => ControlFlow::Break(answer),
            Err(e) => {
                *last_err = QueryError::new(format!("{endpoint}: {}", e.reason), e.timed_out);
                ControlFlow::Continue(())
            }
        }
    }

    async fn query_one(&self, endpoint: &str, body: &str) -> Result<RawAnswer, QueryError> {
        let resp = self
            .client
            .post(endpoint)
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| QueryError::new(e.to_string(), e.is_timeout()))?;
        let header = |name: &str| {
            resp.headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        };
        let block = header("x-state-block");
        let tx_idx = header("x-state-tx-idx");
        let text = resp
            .text()
            .await
            .map_err(|e| QueryError::new(e.to_string(), e.is_timeout()))?;
        Ok(RawAnswer {
            text,
            block,
            tx_idx,
        })
    }
}

/// One HTTP answer before its result is parsed: the JSON-RPC body, and
/// where the snapshot that gave it stands.
struct RawAnswer {
    text: String,
    block: u64,
    tx_idx: u64,
}

/// The JSON-RPC request body for a receipt lookup.
#[must_use]
pub fn receipt_request_body(tx_hash: B256) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["{tx_hash}"]}}"#
    )
}

/// Parse the JSON-RPC answer of a receipt lookup: `null`, or the rkyv
/// bytes of the stored receipt as `0x` hex.
///
/// # Errors
///
/// Returns the reason as text when the body is not JSON, carries an
/// `error` member, or the result does not decode as a receipt.
pub fn parse_receipt_answer(body: &str) -> Result<Option<Receipt>, String> {
    #[derive(Deserialize)]
    struct Reply {
        result: Option<String>,
        error: Option<serde_json::Value>,
    }
    let reply: Reply = serde_json::from_str(body).map_err(|e| format!("bad json: {e}"))?;
    if let Some(e) = reply.error {
        return Err(format!("rpc error: {e}"));
    }
    reply
        .result
        .map(|hex| {
            let bytes =
                alloy_primitives::hex::decode(&hex).map_err(|e| format!("bad receipt hex: {e}"))?;
            rkyv::from_bytes::<Receipt, rkyv::rancor::Error>(&bytes)
                .map_err(|e| format!("bad receipt bytes: {e}"))
        })
        .transpose()
}

/// The JSON-RPC request body for one query.
#[must_use]
pub fn request_body(method: QueryMethod, address: Address) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"{}","params":["{address}","latest"]}}"#,
        method.name()
    )
}

/// Parse the JSON-RPC answer into the hex quantity it carries.
///
/// # Errors
///
/// Returns the reason as text when the body is not JSON, carries an
/// `error` member, has no `result`, or the result is not a hex quantity.
pub fn parse_answer(body: &str) -> Result<U256, String> {
    #[derive(Deserialize)]
    struct Reply {
        result: Option<String>,
        error: Option<serde_json::Value>,
    }
    let reply: Reply = serde_json::from_str(body).map_err(|e| format!("bad json: {e}"))?;
    if let Some(e) = reply.error {
        return Err(format!("rpc error: {e}"));
    }
    let hex = reply.result.ok_or_else(|| "no result".to_string())?;
    let digits = hex
        .strip_prefix("0x")
        .ok_or_else(|| format!("result is not a hex quantity: {hex}"))?;
    U256::from_str_radix(digits, 16).map_err(|e| format!("bad hex quantity {hex}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_and_answer_round_trip() {
        let a = Address::repeat_byte(0x11);
        let body = request_body(QueryMethod::Balance, a);
        assert!(body.contains("eth_getBalance"), "{body}");
        assert!(body.contains(&format!("{a}")), "{body}");
        assert_eq!(
            parse_answer(r#"{"jsonrpc":"2.0","id":1,"result":"0x1f4"}"#).unwrap(),
            U256::from(0x1f4u64)
        );
        assert!(parse_answer(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602}}"#).is_err());
        assert!(parse_answer(r#"{"jsonrpc":"2.0","id":1,"result":"12"}"#).is_err());
    }

    /// The executor sends the stored bytes of the receipt, the format of
    /// its `receipts` table, so the ingress decodes the same type it holds
    /// in its own cache.
    #[test]
    fn a_receipt_answer_round_trips_and_null_is_none() {
        let hash = B256::repeat_byte(0xBE);
        let body = receipt_request_body(hash);
        assert!(body.contains("eth_getTransactionReceipt"), "{body}");
        assert!(body.contains(&format!("{hash}")), "{body}");

        let receipt = Receipt {
            tx_hash: hash,
            status: true,
            gas_used: 21_000,
            nonce: 9,
            from: Address::repeat_byte(0x11),
            block_number: 7,
            ..Receipt::default()
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&receipt).unwrap();
        let answer = format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":"0x{}"}}"#,
            alloy_primitives::hex::encode(&bytes)
        );
        assert_eq!(parse_receipt_answer(&answer).unwrap(), Some(receipt));
        assert_eq!(
            parse_receipt_answer(r#"{"jsonrpc":"2.0","id":1,"result":null}"#).unwrap(),
            None
        );
        assert!(parse_receipt_answer(r#"{"jsonrpc":"2.0","id":1,"result":"0x00"}"#).is_err());
        assert!(
            parse_receipt_answer(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603}}"#).is_err()
        );
    }

    #[test]
    fn off_when_no_endpoint() {
        assert!(ExecutorQuery::new(&ExecutorQueryConfig::default()).is_none());
    }
}
