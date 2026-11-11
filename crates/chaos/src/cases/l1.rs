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

use std::num::NonZeroU32;
use std::time::Duration;

mod batcher;
mod disagreement;
mod follower_loss;
mod followers;
mod halt;
mod liar;
mod null_receipts;
mod outage;
mod transport;
mod two_day;

pub(crate) use disagreement::follower_disagreement;
pub(crate) use follower_loss::{
    instance_loss as follower_instance_loss, total_loss as follower_total_loss,
};
pub(crate) use followers::{Followers, await_resume};
pub(crate) use halt::await_followers_halted;
pub(crate) use liar::liar;
pub(crate) use null_receipts::null_receipts;
pub(crate) use outage::batcher_outage_past_retention;
pub(crate) use transport::{provider_unresponsive, transport_refusals};
pub(crate) use two_day::two_day_outage;

/// How long the stale-post alert must hold to count as fired.
const ALERT_HOLD: Duration = Duration::from_secs(30);

/// The L1 blocks that finalize the head of anvil. Anvil mines a block
/// only for a transaction, and behind the fault proxy it finalizes two
/// blocks behind its head. With these blocks mined right after a lie is
/// armed or cleared, the followers read it within one poll, not after the
/// next posts of the batcher.
pub(crate) const FINALIZE_HEAD: u64 = 3;

/// The case rate of the cases that hold until the sealer floor passes a
/// cursor: twice the frames of the L1 case rate. A batcher restart of
/// 30 s at this rate leaves 3000 frames unposted, below the twice the
/// retention (4096 frames) at which the floor passes its cursor.
pub(crate) const FLOOR_PASS_TPS: NonZeroU32 = NonZeroU32::new(100).expect("a nonzero rate");
