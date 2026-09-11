//! The nonce lookup from an executor.
//!
//! A cold sender seeds at nonce 0. A restarted replica therefore parks
//! every transaction of an established sender until a receipt raises the
//! floor. When the twin is stopped too, no receipt comes, and the sender
//! stays parked until expiry. This is the open issue F02.1.
//!
//! The lookup closes it. When a transaction parks for a sender with no
//! known floor, the core asks for a lookup through [`LookupRequester`]. The
//! binary's lookup task queries an executor for the committed nonce and
//! delivers the answer as a `FloorUpdate`. The floor rises through
//! `advance_floor`, with a max merge. It never seeds the state machine.
//!
//! The answer is a lower bound. An executor at any height gives a valid
//! answer: a floor that lags the truth only parks a transaction a little
//! longer. See `docs/specs/dynamic-sequencer-sizing.md`, section 3.4.
//!
//! The core does not own the in-flight set or the timeout. The task does.
//! The core sends one request per park, and the task drops the duplicates.
//! A timed-out lookup therefore retries on the sender's next park.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::Address;
use serde::{Deserialize, Serialize};

/// The lookup request seam: the channel half the core holds. The core
/// calls [`Self::request`] off the state machine, once per park of a
/// sender with no known floor. The lookup task in the binary owns the
/// receiver.
pub struct LookupRequester {
    tx: tokio::sync::mpsc::UnboundedSender<Address>,
}

impl LookupRequester {
    /// A requester and the receiver its task drains.
    #[must_use]
    pub fn channel() -> (Self, tokio::sync::mpsc::UnboundedReceiver<Address>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// Ask the task for `sender`'s committed nonce.
    pub fn request(&mut self, sender: Address) {
        // A closed receiver means the task is gone. The park then waits
        // for a receipt or expires, as before the lookup existed.
        let _ = self.tx.send(sender);
    }
}

/// The `[lookup]` config section.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct LookupConfig {
    /// The executor query endpoints, as `http://host:port`. Empty means
    /// no lookup. The task tries them in rotation, one after the other,
    /// until one answers.
    pub executor_endpoints: Vec<String>,
    /// The bound of one query, in ms. It also bounds the retry rate: the
    /// task starts no second lookup for a sender within this time. Never
    /// zero: serde rejects a `0` at parse time.
    pub timeout_ms: NonZeroU64,
    /// The bound of concurrent lookups. A burst of cold senders above
    /// nonce 0 becomes at most this many queries in flight. Never zero:
    /// a zero bound would shed every lookup, which `executor_endpoints`
    /// left empty already expresses.
    pub max_in_flight: NonZeroUsize,
}

impl Default for LookupConfig {
    fn default() -> Self {
        Self {
            executor_endpoints: Vec::new(),
            timeout_ms: NonZeroU64::new(2_000).unwrap(),
            max_in_flight: NonZeroUsize::new(64).unwrap(),
        }
    }
}

impl LookupConfig {
    #[must_use]
    pub fn enabled(&self) -> bool {
        !self.executor_endpoints.is_empty()
    }

    /// The bound of one query, [`Self::timeout_ms`] as a `Duration`.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.get())
    }
}

/// The JSON-RPC request body for one lookup.
#[must_use]
pub fn request_body(sender: Address) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["{sender}","latest"]}}"#
    )
}

/// Parse the JSON-RPC answer into the account nonce.
///
/// # Errors
///
/// Returns the reason as text when the body is not JSON, carries an
/// `error` member, has no `result`, or the result is not a hex quantity.
pub fn parse_answer(body: &str) -> Result<u64, String> {
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
    u64::from_str_radix(digits, 16).map_err(|e| format!("bad hex quantity {hex}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_body_is_eth_get_transaction_count() {
        let a = Address::repeat_byte(0x22);
        let body = request_body(a);
        assert!(body.contains("eth_getTransactionCount"));
        assert!(body.contains(&format!("{a}")));
        assert!(body.contains("latest"));
    }

    #[test]
    fn parses_a_hex_quantity() {
        assert_eq!(
            parse_answer(r#"{"jsonrpc":"2.0","id":1,"result":"0x2a"}"#),
            Ok(42)
        );
        assert_eq!(
            parse_answer(r#"{"jsonrpc":"2.0","id":1,"result":"0x0"}"#),
            Ok(0)
        );
    }

    #[test]
    fn rejects_errors_and_junk() {
        assert!(parse_answer(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32601}}"#).is_err());
        assert!(parse_answer(r#"{"jsonrpc":"2.0","id":1,"result":"42"}"#).is_err());
        assert!(parse_answer("not json").is_err());
    }

    #[test]
    fn requester_delivers_to_the_task_side() {
        let (mut req, mut rx) = LookupRequester::channel();
        let a = Address::repeat_byte(0x33);
        req.request(a);
        assert_eq!(rx.try_recv().ok(), Some(a));
    }

    #[test]
    fn default_config_is_disabled() {
        assert!(!LookupConfig::default().enabled());
    }
}
