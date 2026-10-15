//! The L1 outage cases: a lying L1 endpoint in front of the L1 follower,
//! the batcher's resume through it, an outage of the batcher past the
//! sealer's egress retention, and the loss of follower instances. Each
//! case reads the truth from anvil directly, serves the follower the lie
//! through the fault proxy, and judges the DA record on L1 at the end.
//! The da-watcher reads the follower's `l1_blocks` stream, not L1.
//!
//! The follower of this cluster reads two sources: the proxy, which lies
//! on command, and the proxy's second source (`/second`), which serves
//! L1 except for a fault scoped to the caller. A lie of the first source
//! is a disagreement of the two: the follower halts and publishes none of
//! it.

mod batcher;
mod disagreement;
mod follower_loss;
mod followers;
mod halt;
mod liar;
mod null_receipts;
mod outage;
mod two_day;

pub(crate) use disagreement::follower_disagreement;
pub(crate) use follower_loss::{
    instance_loss as follower_instance_loss, total_loss as follower_total_loss,
};
pub(crate) use liar::liar;
pub(crate) use null_receipts::null_receipts;
pub(crate) use outage::batcher_outage_past_retention;
pub(crate) use two_day::two_day_outage;
