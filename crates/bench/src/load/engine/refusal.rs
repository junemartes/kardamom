//! A refused submit: the ingress's answer, its log line, and the retry
//! state of one submit.

use std::time::{Duration, Instant};

use jsonrpsee::core::ClientError;
use kardamom_ingress::error::CHAIN_HALTED_CODE;

use crate::load::plan::PlannedTx;

/// Every refusal up to this count gets a log line. After it, only a
/// refusal whose count is a power of two does, so a long halt cannot
/// flood the log.
const LOG_FIRST: u64 = 16;

/// The pause between two submits of one transaction while the ingress
/// stays paused.
pub(super) const PAUSE_RETRY_DELAY: Duration = Duration::from_millis(500);

/// One refusal of a submit: the JSON-RPC error code, when the ingress
/// answered, and the message.
pub(super) struct Refusal {
    code: Option<i32>,
    message: String,
}

impl Refusal {
    /// Read the refusal from the error of the submit call.
    pub(super) fn of(err: &ClientError) -> Self {
        match err {
            ClientError::Call(obj) => Self {
                code: Some(obj.code()),
                message: obj.message().to_string(),
            },
            other => Self {
                code: None,
                message: other.to_string(),
            },
        }
    }

    /// Whether the ingress refused the submit as chain halted. A paused
    /// ingress refuses before it forwards. The ingress also answers this
    /// code after a forward, when the sealer refuses the record on its
    /// `da_lag` or `record_lag` guard. A refused record is not ordered,
    /// so its nonce stays open in both cases. The retry sends the same
    /// signed transaction: it has the same hash, so a copy that did land
    /// is a duplicate that the dedup drops, not a second transaction.
    pub(super) fn is_paused(&self) -> bool {
        self.code == Some(CHAIN_HALTED_CODE)
    }

    /// Log the refusal of `tx`, when `count` is in the logged set.
    pub(super) fn log(&self, tx: &PlannedTx, count: u64) {
        if count > LOG_FIRST && !count.is_power_of_two() {
            return;
        }
        tracing::warn!(
            sender = tx.sender,
            nonce = tx.nonce,
            hash = %tx.hash,
            code = ?self.code,
            error = %self.message,
            refusals = count,
            "load: the ingress refused a submit"
        );
    }
}

/// The retry state of one submit: the attempts that counted, and when
/// the first refusal of a paused ingress came.
#[derive(Default)]
pub(super) struct RetryState {
    pub(super) attempt: u32,
    paused_since: Option<Instant>,
}

impl RetryState {
    /// Whether a paused refusal at `now` is still inside `wait`, counted
    /// from the first paused refusal. Such a refusal does not count as
    /// an attempt.
    pub(super) fn waits_out_pause(&mut self, wait: Duration, now: Instant) -> bool {
        let since = *self.paused_since.get_or_insert(now);
        now.saturating_duration_since(since) < wait
    }
}
