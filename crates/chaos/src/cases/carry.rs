//! The carry window on the container cluster: the activation in the order
//! of the staging rollout, then the faults around its switch.
//!
//! The shard deploys the member feed, the executors on the sealer nodes,
//! the frames of the executors with the egress, and the v2 loop of the
//! sequencers. The v1 loop offers each transaction without its bytes, and
//! the sealer refuses such an offer while the window is on. Each step is
//! one case, under the case load, in this order:
//!
//! 1. `carry-versions-on`: a lockbox on L1, a deploy that gives its
//!    address to the L1 follower, then features 3 and 4 by upgrade
//!    transaction. Every member runs decision version 5, and every
//!    sequencer keeps it. Three native deposits cross a provider outage,
//!    preserve source-hash receipt order, and credit every executor exactly.
//! 2. `carry-held-offer-after-downgrade`: the v2 loop parks nonce `n + 1`
//!    under version 5. Feature 4 turns off, and every member and lane
//!    reaches version 4 before nonce `n` releases the held offer. Both
//!    receipts must succeed and the sender nonce must advance by two.
//!    Feature 4 turns on again.
//! 3. `carry-readers-on-egress`: the batcher and the validator move to
//!    the egress once they are past R, the highest stored reference, and
//!    the sequencers get the version floor 5. R does not move.
//! 4. `carry-window-on`: a follower stops, feature 5 turns the window on,
//!    and the follower starts again. It switches at the same entry as its
//!    peers, and every member keeps the exact window.
//! 5. `carry-executor-first`: executor 0 moves to carry mode. The state
//!    audit after it compares the carry executor with the executors in
//!    reference mode.
//! 6. `carry-executors-rest`: executors 1 and 2 move, one at a time. Then
//!    the smoke gate runs.
//! 7. `carry-window-off-on`: feature 5 turns off and on again.
//! 8. `carry-leader-kill-at-switch`: feature 5 turns off, then on, and the
//!    leader dies within 2 s after it switches.
//! 9. `carry-egress-restart`: one egress instance restarts. Its R does not
//!    change, and the readers of the egress keep reading.
//!
//! Every upgrade transaction passes `just sealer-upgrade-check` first.

mod activation;
mod deposits;
mod egress;
mod members;
mod switch;
#[cfg(test)]
mod tests;
mod upgrade;

use std::time::Duration;

use crate::harness::Harness;
use crate::knobs::Knobs;

/// One step of the carry window, as one case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    VersionsOn,
    HeldOfferAfterDowngrade,
    ReadersOnEgress,
    WindowOn,
    ExecutorFirst,
    ExecutorsRest,
    WindowOffOn,
    LeaderKillAtSwitch,
    EgressRestart,
}

/// Every step, in run order.
pub(crate) const STEPS: &[Step] = &[
    Step::VersionsOn,
    Step::HeldOfferAfterDowngrade,
    Step::ReadersOnEgress,
    Step::WindowOn,
    Step::ExecutorFirst,
    Step::ExecutorsRest,
    Step::WindowOffOn,
    Step::LeaderKillAtSwitch,
    Step::EgressRestart,
];

/// The steps of one case, with the harness.
struct Run<'a> {
    h: &'a mut Harness,
    ctx: &'static str,
}

impl Step {
    /// The CI name.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::VersionsOn => "carry-versions-on",
            Self::HeldOfferAfterDowngrade => "carry-held-offer-after-downgrade",
            Self::ReadersOnEgress => "carry-readers-on-egress",
            Self::WindowOn => "carry-window-on",
            Self::ExecutorFirst => "carry-executor-first",
            Self::ExecutorsRest => "carry-executors-rest",
            Self::WindowOffOn => "carry-window-off-on",
            Self::LeaderKillAtSwitch => "carry-leader-kill-at-switch",
            Self::EgressRestart => "carry-egress-restart",
        }
    }

    /// The least load window of the step: the deploys, the upgrade
    /// transactions with their finality, and the restarts.
    pub(crate) fn floor(self, k: &Knobs) -> Duration {
        let inject = k.inject_delay;
        match self {
            // The first step has a deploy, two checked upgrades, and the
            // three-credit proof across a provider fault. Their load
            // windows add; each runtime proof keeps its own deadline.
            Self::VersionsOn => inject
                .saturating_add(Duration::from_mins(40))
                .saturating_add(Duration::from_mins(8))
                .saturating_add(Duration::from_mins(20))
                .saturating_add(k.l1_fault),
            Self::ReadersOnEgress | Self::ExecutorFirst => inject + Duration::from_mins(40),
            // Two upgrade transactions and the held transfer.
            Self::HeldOfferAfterDowngrade | Self::WindowOffOn => inject + Duration::from_mins(8),
            Self::WindowOn => inject + k.restart_slo + Duration::from_mins(8),
            // One staged executor roll for each move.
            Self::ExecutorsRest => inject + Duration::from_mins(80),
            Self::LeaderKillAtSwitch => {
                inject + k.leader_slo + k.restart_slo + Duration::from_mins(8)
            }
            Self::EgressRestart => inject + k.restart_slo + Duration::from_mins(2),
        }
    }

    /// The case body.
    ///
    /// # Errors
    ///
    /// Returns the failure of the step.
    pub(crate) async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        let mut run = Run {
            h,
            ctx: self.name(),
        };
        match self {
            Self::VersionsOn => run.versions_on().await,
            Self::HeldOfferAfterDowngrade => run.held_offer_after_downgrade().await,
            Self::ReadersOnEgress => run.readers_on_egress().await,
            Self::WindowOn => run.window_on().await,
            Self::ExecutorFirst => run.executor_first().await,
            Self::ExecutorsRest => run.executors_rest().await,
            Self::WindowOffOn => run.window_off_on().await,
            Self::LeaderKillAtSwitch => run.leader_kill_at_switch().await,
            Self::EgressRestart => run.egress_restart().await,
        }
    }
}
