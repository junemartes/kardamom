//! Shared constants and helpers for time-budgeted Aeron `offer` retry, used
//! by [`crate::aeron_live::pending`] and [`crate::aeron_live::thread`].
//!
//! Aeron's `offer` returns a non-negative stream position on success. On
//! failure it returns a negative status code: `NOT_CONNECTED (-1)` when no
//! subscriber image has formed yet, `BACK_PRESSURED (-2)` when the term
//! window is full, plus `ADMIN_ACTION (-3)`, `PUBLICATION_CLOSED (-4)`, and
//! `MAX_POSITION (-5)`.
//!
//! Aeron does not replay history from before a subscription starts, so a
//! publish before the subscriber connects is lost with no warning. The
//! Aeron thread's pending-publish queue (`crate::aeron_live::pending`)
//! retries a parked offer, one attempt per loop iteration, until it
//! succeeds or [`OFFER_TIMEOUT`] passes.

use std::time::Duration;

/// How long to keep retrying a failing offer before giving up. This covers
/// UDP-multicast image establishment (the rusteron docs bound a connect wait
/// at 5 s) plus margin. Once the subscriber is connected, an offer succeeds
/// on the first attempt, so this budget is rarely approached.
pub(crate) const OFFER_TIMEOUT: Duration = Duration::from_secs(5);

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
