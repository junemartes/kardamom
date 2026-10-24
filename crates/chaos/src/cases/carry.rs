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
//!    sequencer keeps it.
//! 2. `carry-full-ref-below-v5`: a full reference that the sealer orders
//!    below version 5, on purpose. The v2 loop takes nonce `n + 1` with
//!    its bytes under version 5 and holds it for nonce `n`. Feature 4 turns
//!    off. Then nonce `n` releases the held nonce, which goes out with its
//!    bytes under version 4. Every member counts it. Feature 4 turns on
//!    again.
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
    FullRefBelowV5,
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
    Step::FullRefBelowV5,
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
            Self::FullRefBelowV5 => "carry-full-ref-below-v5",
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
            // The lockbox deploy, then two upgrade transactions.
            Self::VersionsOn => inject + Duration::from_mins(10),
            // Two upgrade transactions and the held transfer.
            Self::FullRefBelowV5 | Self::WindowOffOn => inject + Duration::from_mins(4),
            // A deploy that rolls the sequencers, the batcher and the
            // validator.
            Self::ReadersOnEgress => inject + Duration::from_mins(8),
            Self::WindowOn => inject + k.restart_slo + Duration::from_mins(4),
            // One staged executor roll for each move.
            Self::ExecutorFirst => inject + Duration::from_mins(5),
            Self::ExecutorsRest => inject + Duration::from_mins(9),
            Self::LeaderKillAtSwitch => {
                inject + k.leader_slo + k.restart_slo + Duration::from_mins(3)
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
            Self::FullRefBelowV5 => run.full_ref_below_v5().await,
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
