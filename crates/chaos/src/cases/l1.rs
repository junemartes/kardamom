//! The L1 outage cases: a lying L1 endpoint in front of the followers,
//! the batcher's resume through it, and an outage of the batcher past
//! the sealer's egress retention. Each case reads the truth from anvil
//! directly, serves the followers the lie through the fault proxy, and
//! judges the DA record on L1 at the end.
//!
//! The followers of this cluster read one L1 source. An assertion that
//! needs two sources (a disagreement counter, a halt on a swallowed
//! log, a resume after a wrong hash reached the follower's anchor) is
//! logged as deferred and does not fail the case: the case names the
//! exact behavior the two-source followers add, so it turns into a hard
//! assertion with them.

mod batcher;
mod followers;
mod halt;
mod liar;
mod null_receipts;
mod outage;
mod two_day;

pub(crate) use liar::liar;
pub(crate) use null_receipts::null_receipts;
pub(crate) use outage::batcher_outage_past_retention;
pub(crate) use two_day::two_day_outage;

/// Log an assertion the single-source followers cannot satisfy.
pub(super) fn deferred(ctx: &str, what: &str) {
    crate::log(format!(
        "{ctx}: DEFERRED to the two-source followers: {what}"
    ));
}
