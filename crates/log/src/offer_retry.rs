//! Time-budgeted retry for Aeron `offer`, shared by the publisher adapters
//! ([`crate::publisher`] and [`crate::aeron_live`]).
//!
//! Aeron's `offer` returns a non-negative stream position on success. On
//! failure it returns a negative status code: `NOT_CONNECTED (-1)` when no
//! subscriber image has formed yet, `BACK_PRESSURED (-2)` when the term
//! window is full, plus `ADMIN_ACTION (-3)`, `PUBLICATION_CLOSED (-4)`, and
//! `MAX_POSITION (-5)`.
//!
//! An earlier loop spun a fixed 1024 times (microseconds) and treated every
//! negative code the same way. A publisher whose subscriber had not yet
//! joined gave up almost at once. Aeron does not replay history from before
//! a subscription starts, so that first frame was lost with no warning. Over
//! IPC the connection forms within the spin budget, so the single-host e2e
//! test worked (it still wraps its pipeline bring-up in a fixed `sleep`).
//! Over UDP multicast, where image establishment can take hundreds of
//! milliseconds, the publisher dropped the message and nothing moved
//! downstream.
//!
//! [`offer_with_deadline`] fixes this. It spins briefly for transient
//! back-pressure (the receiver drains within microseconds), then backs off
//! to 1 ms sleeps, and retries until the offer succeeds or a deadline
//! passes. The first publish now waits for the subscriber to connect, and
//! is delivered.

use std::time::{Duration, Instant};

use tracing::warn;

use crate::error::LogError;

/// How long to keep retrying a failing offer before giving up. This covers
/// UDP-multicast image establishment (the rusteron docs bound a connect wait
/// at 5 s) plus margin. Once the subscriber is connected, an offer succeeds
/// on the first attempt, so this budget is rarely approached.
pub(crate) const OFFER_TIMEOUT: Duration = Duration::from_secs(5);

/// Spin (busy-wait, no sleep) for this many initial attempts. This is the
/// low-latency path for transient back-pressure, where the receiver frees
/// the term window within microseconds. After this, the code sleeps: a
/// negative code that outlasts a spin burst is almost always
/// `NOT_CONNECTED` (no subscriber image yet), and more spinning would only
/// burn a core.
const SPIN_ATTEMPTS: u64 = 1024;

/// Human-readable label for an Aeron offer status code (negative = failure).
pub(crate) fn offer_code_str(code: i64) -> &'static str {
    match code {
        -1 => "NOT_CONNECTED",
        -2 => "BACK_PRESSURED",
        -3 => "ADMIN_ACTION",
        -4 => "PUBLICATION_CLOSED",
        -5 => "MAX_POSITION_EXCEEDED",
        _ => "UNKNOWN",
    }
}

/// Retry `try_offer` until it returns a non-negative position or `timeout`
/// elapses. Returns the raw success code; the caller decodes the position.
///
/// `try_offer` is the bare Aeron `offer` call (returns `>= 0` position or a
/// negative status). See the module docs for why this waits, instead of
/// failing fast after a fixed spin count.
pub(crate) fn offer_with_deadline(
    mut try_offer: impl FnMut() -> i64,
    timeout: Duration,
) -> Result<i64, LogError> {
    let start = Instant::now();
    let mut attempt: u64 = 0;
    loop {
        let code = try_offer();
        if code >= 0 {
            return Ok(code);
        }
        if start.elapsed() >= timeout {
            return Err(LogError::Aeron(format!(
                "aeron offer failed after {:?}: {} ({code})",
                start.elapsed(),
                offer_code_str(code),
            )));
        }
        attempt += 1;
        if attempt <= SPIN_ATTEMPTS {
            std::hint::spin_loop();
        } else {
            // Log throttling makes a long wait (e.g. a slow subscriber) visible
            // without flooding the log.
            if attempt.is_multiple_of(1024) {
                warn!(
                    code = offer_code_str(code),
                    elapsed_ms = start.elapsed().as_millis() as u64,
                    "aeron offer retrying (awaiting subscriber or back-pressure relief)"
                );
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn succeeds_immediately_when_offer_ok() {
        // A connected publication returns a position on the first try.
        // There is no spin, no sleep, and no wait.
        let calls = Cell::new(0u64);
        let r = offer_with_deadline(
            || {
                calls.set(calls.get() + 1);
                42
            },
            Duration::from_secs(1),
        )
        .expect("ok");
        assert_eq!(r, 42);
        assert_eq!(calls.get(), 1, "must not retry on success");
    }

    #[test]
    fn waits_out_not_connected_then_delivers() {
        // Regression test: a subscriber connects only after the publisher has
        // been offering for a while. NOT_CONNECTED (-1) for the first 1100
        // attempts (past the spin budget, into the sleep phase), then a real
        // position. The old fixed 1024-spin loop returned Err here. The fix
        // must keep retrying and deliver.
        let n = Cell::new(0u64);
        let r = offer_with_deadline(
            || {
                let i = n.get();
                n.set(i + 1);
                if i < 1100 { -1 } else { 7 }
            },
            Duration::from_secs(5),
        )
        .expect("should deliver once the subscriber connects");
        assert_eq!(r, 7);
        assert!(n.get() >= 1100, "must have retried past the spin budget");
    }

    #[test]
    fn retries_transient_back_pressure() {
        // BACK_PRESSURED (-2) a few times, then success. The fast spin path
        // handles this without sleeping.
        let n = Cell::new(0u64);
        let r = offer_with_deadline(
            || {
                let i = n.get();
                n.set(i + 1);
                if i < 10 { -2 } else { 99 }
            },
            Duration::from_secs(1),
        )
        .expect("ok after back-pressure clears");
        assert_eq!(r, 99);
    }

    #[test]
    fn times_out_when_never_connected() {
        // A publication whose subscriber never joins must error after the
        // deadline. It must not spin forever or fail in microseconds.
        let start = Instant::now();
        let err = offer_with_deadline(|| -1, Duration::from_millis(60)).expect_err("must time out");
        assert!(
            start.elapsed() >= Duration::from_millis(60),
            "must honour the full deadline, waited {:?}",
            start.elapsed()
        );
        match err {
            LogError::Aeron(m) => {
                assert!(
                    m.contains("NOT_CONNECTED"),
                    "error should name the code: {m}"
                )
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn error_message_labels_the_offer_code() {
        let err =
            offer_with_deadline(|| -4, Duration::from_millis(20)).expect_err("err on closed pub");
        match err {
            LogError::Aeron(m) => assert!(
                m.contains("PUBLICATION_CLOSED"),
                "error should label code -4: {m}"
            ),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
