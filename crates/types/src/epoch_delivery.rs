//! The delivery contract of an L1 epoch, from the da-watcher's publish to
//! the sealer's commit.
//!
//! The da-watcher publishes each epoch on `tx_deposits`. The sequencers
//! relay it to the sealer. A boundary carries the sealer's L1 origin, so it
//! confirms every epoch up to that origin. An epoch can be lost on the way:
//! cluster ingress is at-most-once across a leader kill, and a sequencer
//! that restarts loses the epochs it held. So two parties keep the
//! epochs that no boundary confirmed:
//!
//! 1. The sequencer offers its own copies again when the sealer answers
//!    with an origin gap. This heals a lost offer within one L1 block.
//! 2. The da-watcher publishes its unconfirmed epochs again when no
//!    boundary confirms one for [`REPUBLISH_AFTER`]. This heals a
//!    sequencer that lost its queue.
//!
//! A sequencer raises the `origin_gap` halt only when the missing epoch
//! stays missing for [`ORIGIN_GAP_GRACE`], three re-publish periods.

use core::num::NonZeroUsize;
use core::time::Duration;

/// How long the da-watcher waits for a boundary that confirms a published
/// epoch. Then it publishes every unconfirmed epoch again, in order.
///
/// The bound comes from the deploy's timings. The sealer emits a boundary
/// every tick (2 s), and an epoch forces one at once. A new leader takes
/// up to the leader heartbeat timeout (10 s). The sequencer's own resend
/// starts with the next epoch's origin-gap reject, one L1 block (12 s)
/// later. 10 s + 12 s + two ticks is 26 s. So in a leader kill the
/// sequencer heals first, and the da-watcher publishes again only when no
/// sequencer holds the epoch.
pub const REPUBLISH_AFTER: Duration = Duration::from_secs(30);

/// How long a sequencer waits for a missing epoch before it raises the
/// `origin_gap` halt. Three re-publish periods: the da-watcher publishes
/// the epoch again within one period, and two more periods cover a
/// re-publish that met back-pressure.
pub const ORIGIN_GAP_GRACE: Duration = Duration::from_secs(90);

/// The most epochs the da-watcher publishes past the sealer's confirmed
/// origin. At the bound it stops publishing new epochs and waits for a
/// boundary; it never drops an epoch. 2048 L1 blocks are 6.8 hours of L1.
/// The bound is below the sequencer's queue bound, so the epochs of one
/// window never fill a sequencer's queue.
pub const PUBLISH_WINDOW: NonZeroUsize = NonZeroUsize::new(2048).expect("2048 is not zero");

#[cfg(test)]
mod tests {
    use super::{ORIGIN_GAP_GRACE, REPUBLISH_AFTER};

    /// The halt must come clearly after the re-publish that heals the gap.
    #[test]
    fn the_sequencer_waits_three_republish_periods_before_it_halts() {
        assert!(ORIGIN_GAP_GRACE >= REPUBLISH_AFTER.saturating_mul(3));
    }
}
