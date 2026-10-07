//! The L1 outage cases: a lying L1 endpoint in front of the L1 follower,
//! the batcher's resume through it, an outage of the batcher past the
//! sealer's egress retention, and the loss of follower instances. Each
//! case reads the truth from anvil directly, serves the follower the lie
//! through the fault proxy, and judges the DA record on L1 at the end.
//! The da-watcher reads the follower's `l1_blocks` stream, not L1.
//!
//! The follower of this cluster reads one L1 source. An assertion that
//! needs two sources (a disagreement counter, a halt on a swallowed
//! log, a resume after a wrong hash reached the follower's anchor) is
//! logged as deferred and does not fail the case: the case names the
//! exact behavior the two-source followers add, so it turns into a hard
//! assertion with them.

mod batcher;
mod follower_loss;
mod followers;
mod halt;
mod liar;
mod null_receipts;
mod outage;
mod two_day;

pub(crate) use follower_loss::{
    instance_loss as follower_instance_loss, total_loss as follower_total_loss,
};
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
