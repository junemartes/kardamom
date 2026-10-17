//! The nonce lookup for a parked sender.
//!
//! A cold sender seeds at nonce 0. Without the lookup, a restarted replica
//! parks every transaction of an established sender until a receipt raises
//! the floor. When the twin is stopped too, no receipt comes, and the
//! sender stays parked until expiry.
//!
//! When a transaction parks for a sender with no known floor, the core asks
//! for a lookup through [`LookupRequester`]. The binary's lookup task asks
//! three layers for the committed nonce, in this order, and stops at the
//! first answer:
//!
//! 1. The local account layer, which the `tx_receipts` rows fill.
//! 2. Redis, when `[cache]` names an address.
//! 3. An executor.
//!
//! The task delivers the answer as a `FloorUpdate`. The floor rises through
//! `advance_floor`, with a max merge. It never seeds the state machine.
//!
//! A request carries the rollback generation of the controller. The answer
//! carries it back, so an answer to a request from before the latest
//! rollback counts for nothing, and every later rollback voids the floor
//! of an answer. A source can still answer from a voided state after a
//! rollback: Redis before the state mirror clears it, and an executor
//! before its own rollback. Such a floor stays until the next rollback
//! event. Each executor that rolls back sends one, so the floor of a stale
//! executor answer goes when that executor rolls back. A stale Redis
//! answer stays at most until the next rollback event, or until a receipt
//! of the sender gives the chain floor again.
//!
//! Every answer is a lower bound. A layer at any height gives a valid
//! answer: a floor that lags the truth only parks a transaction a little
//! longer. So a Redis miss or outage only sends the lookup to an executor.
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
    tx: tokio::sync::mpsc::UnboundedSender<LookupRequest>,
}

/// One lookup request: the sender, and the rollback generation of the
/// controller when the core asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupRequest {
    pub sender: Address,
    pub generation: u64,
}

impl LookupRequester {
    /// A requester and the receiver its task drains.
    #[must_use]
    pub fn channel() -> (Self, tokio::sync::mpsc::UnboundedReceiver<LookupRequest>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (Self { tx }, rx)
    }

    /// Ask the task for the committed nonce of the sender of `request`.
    pub fn request(&mut self, request: LookupRequest) {
        // A closed receiver means the task is gone. The park then waits
        // for a receipt or expires, as before the lookup existed.
        let _ = self.tx.send(request);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requester_delivers_to_the_task_side() {
        let (mut req, mut rx) = LookupRequester::channel();
        let request = LookupRequest {
            sender: Address::repeat_byte(0x33),
            generation: 2,
        };
        req.request(request);
        assert_eq!(rx.try_recv().ok(), Some(request));
    }

    #[test]
    fn default_config_is_disabled() {
        assert!(!LookupConfig::default().enabled());
    }
}
