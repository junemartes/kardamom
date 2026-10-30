//! The carry window on the container cluster: the activation in the order
//! of the staging rollout, then the faults around its switch.
//!
//! The shard deploys the member feed, the executors on the sealer nodes,
//! the frames of the executors with the egress, and the v2 loop of the
//! sequencers. The v1 loop offers each transaction without its bytes, and
//! the sealer refuses such an offer while the window is on. Each step is
//! one case, under the case load, in this order:
//!
//! 1. `carry-versions-on`: a lockbox on L1, and the L1 follower and the
//!    validator read its address. Then features 3 and 4 by upgrade
//!    transaction. Every member runs decision version 5, and every
//!    sequencer keeps it.
//! 2. `carry-deposits-provider-fault`: three native deposits cross a
//!    provider outage, preserve source-hash receipt order, and credit
//!    every executor exactly.
//! 3. `carry-held-offer-after-downgrade`: the v2 loop parks nonce `n + 1`
//!    under version 5. Feature 4 turns off, and every member and lane
//!    reaches version 4 before nonce `n` releases the held offer. Both
//!    receipts must succeed and the sender nonce must advance by two.
//!    Feature 4 turns on again.
//! 4. `carry-readers-on-egress`: the batcher and the validator move to
//!    the egress once they are past R, the highest stored reference, and
//!    the sequencers get the version floor 5. R does not move.
//! 5. `carry-window-on`: a follower stops, feature 5 turns the window on,
//!    and the follower starts again. It switches at the same entry as its
//!    peers, and every member keeps the exact window.
//! 6. `carry-executor-first`: executor 0 moves to carry mode. The state
//!    audit after it compares the carry executor with the executors in
//!    reference mode.
//! 7. `carry-executors-rest`: executors 1 and 2 move to carry mode, one at
//!    a time, and the smoke gate runs.
//! 8. `carry-deposits-all-carry`: three credits cross a provider outage
//!    with every executor in carry mode.
//! 9. `carry-window-off-on`: feature 5 turns off and on again.
//! 10. `carry-leader-kill-at-switch`: feature 5 turns off, then on, and
//!     the leader dies within 2 s after it switches.
//! 11. `carry-egress-restart`: one egress instance restarts. Its R does
//!     not change, and the readers of the egress keep reading.
//!
//! Every upgrade transaction passes the rules of `just
//! sealer-upgrade-check` first, read straight from the cluster (see
//! `check`). A switch of the deploy is a targeted update of the jobs that it changes
//! (see `patch`), the change that the job file renders for it.

mod activation;
mod check;
mod deposits;
mod egress;
mod members;
mod patch;
mod switch;
#[cfg(test)]
mod tests;
mod upgrade;

use std::num::NonZeroU32;

use crate::harness::Harness;
use crate::knobs::Knobs;

/// The case load of the carry steps. The steps prove the switches under
/// traffic, not a rate.
pub(crate) const CARRY_TPS: NonZeroU32 = NonZeroU32::new(25).expect("a nonzero rate");

/// The submit attempts of the load of a step with a provider outage. The
/// chain stops taking transactions for the fault, the halt detection and
/// the resume, about two minutes. Each attempt parks 30 s at the ingress,
/// so ten attempts keep every sender of the load live through it.
const FAULT_RETRY: u32 = 10;

/// One step of the carry window, as one case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    VersionsOn,
    DepositsProviderFault,
    HeldOfferAfterDowngrade,
    ReadersOnEgress,
    WindowOn,
    ExecutorFirst,
    ExecutorsRest,
    DepositsAllCarry,
    WindowOffOn,
    LeaderKillAtSwitch,
    EgressRestart,
}

/// Every step, in run order.
pub(crate) const STEPS: &[Step] = &[
    Step::VersionsOn,
    Step::DepositsProviderFault,
    Step::HeldOfferAfterDowngrade,
    Step::ReadersOnEgress,
    Step::WindowOn,
    Step::ExecutorFirst,
    Step::ExecutorsRest,
    Step::DepositsAllCarry,
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
            Self::DepositsProviderFault => "carry-deposits-provider-fault",
            Self::HeldOfferAfterDowngrade => "carry-held-offer-after-downgrade",
            Self::ReadersOnEgress => "carry-readers-on-egress",
            Self::WindowOn => "carry-window-on",
            Self::ExecutorFirst => "carry-executor-first",
            Self::ExecutorsRest => "carry-executors-rest",
            Self::DepositsAllCarry => "carry-deposits-all-carry",
            Self::WindowOffOn => "carry-window-off-on",
            Self::LeaderKillAtSwitch => "carry-leader-kill-at-switch",
            Self::EgressRestart => "carry-egress-restart",
        }
    }

    /// The submit attempts of the case load.
    pub(crate) fn load_retry(self, k: &Knobs) -> u32 {
        match self {
            Self::DepositsProviderFault | Self::DepositsAllCarry => FAULT_RETRY,
            _ => k.load_retry,
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
            Self::DepositsProviderFault | Self::DepositsAllCarry => run.deposits().await,
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
