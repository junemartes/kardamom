//! The wait between the persisted-state audit and the next case. The
//! audit stops and restores the cluster, executor and validator jobs. The
//! restore returns when the allocations run, not when the chain runs. The
//! batcher and the ingresses do not restart: they connect again. The
//! first boundary tick of a restored leader can come minutes later. An
//! ingress refuses every submit with `sealer_no_quorum` until it sees a
//! status frame, and a refused nonce leaves a gap that fails the case
//! load. So the next case starts only after the chain runs again.

use std::cell::{Cell, RefCell};
use std::time::{Duration, Instant};

use crate::cases::chain_status::ChainView;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget};
use crate::probes::{CLUSTER_TASK, Probed};

/// The time the chain gets to run again after the audit, for all the
/// steps together.
const RECOVERY_BUDGET: Duration = Duration::from_mins(5);
/// How often a step reads its signal.
const POLL: Duration = Duration::from_secs(5);
/// The sealer's heartbeat line. Each member prints it once for each 30
/// boundary ticks, with its role.
const TICK_LINE: &str = "cluster boundary-clock TICK";
/// The role field of a heartbeat line from the leader. A member that
/// replays its log after a restart prints a heartbeat as a follower.
const LEADER_ROLE: &str = "role=LEADER";

/// A signal that rises while the chain runs.
#[derive(Debug, Clone, Copy)]
enum Counter {
    /// The leader's heartbeat lines in the cluster logs.
    LeaderTicks,
    /// The batcher's confirmed posts.
    BatcherPosts,
    /// The highest committed block of the executors.
    ExecutorHead,
}

impl Counter {
    fn name(self) -> &'static str {
        match self {
            Self::LeaderTicks => "leader heartbeat lines",
            Self::BatcherPosts => "batcher posts",
            Self::ExecutorHead => "executor head",
        }
    }

    /// One reading. `None` when the source does not answer.
    async fn read(self, h: &Harness) -> anyhow::Result<Option<i64>> {
        Ok(match self {
            Self::LeaderTicks => {
                let logs = h.nomad.job_logs(CLUSTER_TASK, Streams::StdoutOnly).await?;
                i64::try_from(Self::leader_ticks(&logs)).ok()
            }
            Self::BatcherPosts => h.probes.batcher_posts().await,
            Self::ExecutorHead => h.probes.executor_progress().await,
        })
    }

    /// How many heartbeat lines of `logs` come from a leader.
    fn leader_ticks(logs: &str) -> usize {
        logs.lines()
            .filter(|line| line.contains(TICK_LINE) && line.contains(LEADER_ROLE))
            .count()
    }
}

/// One step of the recovery, in the order the chain comes back.
#[derive(Debug, Clone, Copy)]
enum Step {
    /// The counter rises past its first reading.
    Rises(Counter),
    /// Every ingress takes submits: no sealer halt, and no pause.
    IngressesOpen,
}

impl Step {
    const ORDER: [Self; 4] = [
        Self::Rises(Counter::LeaderTicks),
        Self::IngressesOpen,
        Self::Rises(Counter::BatcherPosts),
        Self::Rises(Counter::ExecutorHead),
    ];

    /// What did not happen when the step fails.
    fn failure(self) -> &'static str {
        match self {
            Self::Rises(Counter::LeaderTicks) => "no sealer leader ticked the boundary clock",
            Self::IngressesOpen => "an ingress still refuses submits",
            Self::Rises(Counter::BatcherPosts) => "the batcher confirmed no new post",
            Self::Rises(Counter::ExecutorHead) => "the executors did not advance",
        }
    }

    /// What happened when the step holds.
    fn success(self) -> &'static str {
        match self {
            Self::Rises(Counter::LeaderTicks) => "a sealer leader ticks the boundary clock",
            Self::IngressesOpen => "every ingress takes submits",
            Self::Rises(Counter::BatcherPosts) => "the batcher posts",
            Self::Rises(Counter::ExecutorHead) => "the executors advance",
        }
    }
}

/// A counter's first reading, and the test that a later one is above it.
#[derive(Debug, Default)]
struct Rise {
    base: Cell<Option<i64>>,
}

impl Rise {
    /// Take `now`. The first reading that answers becomes the base; true
    /// once a later reading is above it.
    fn observe(&self, now: Option<i64>) -> bool {
        let base = self.base.get().or(now);
        self.base.set(base);
        base.zip(now).is_some_and(|(base, now)| now > base)
    }

    fn describe(&self, counter: Counter, now: Option<i64>) -> String {
        let show = |v: Option<i64>| v.map_or("no answer".to_string(), |v| v.to_string());
        format!(
            "{} {} -> {}",
            counter.name(),
            show(self.base.get()),
            show(now)
        )
    }
}

/// The wait of one step: its base, and its last reading for the failure
/// message.
struct StepWait<'a> {
    harness: &'a Harness,
    step: Step,
    rise: Rise,
    last: RefCell<String>,
}

impl<'a> StepWait<'a> {
    fn new(harness: &'a Harness, step: Step) -> Self {
        Self {
            harness,
            step,
            rise: Rise::default(),
            last: RefCell::new("no reading".to_string()),
        }
    }

    /// One observation: `Some` once the step holds.
    async fn observe(&self) -> anyhow::Result<Option<()>> {
        Ok(match self.step {
            Step::Rises(counter) => self.rose(counter).await?,
            Step::IngressesOpen => self.ingresses_open().await,
        })
    }

    async fn rose(&self, counter: Counter) -> anyhow::Result<Option<()>> {
        let now = counter.read(self.harness).await?;
        let rose = self.rise.observe(now);
        *self.last.borrow_mut() = self.rise.describe(counter, now);
        Ok(rose.then_some(()))
    }

    async fn ingresses_open(&self) -> Option<()> {
        let mut refusals = Vec::new();
        for node in &self.harness.probes.ingresses {
            refusals.extend(self.refusal_of(node).await);
        }
        *self.last.borrow_mut() = refusals.join("; ");
        refusals.is_empty().then_some(())
    }

    /// Why the ingress on `node` refuses a submit.
    async fn refusal_of(&self, node: &Probed) -> Option<String> {
        let why = ChainView::refusal_at(&node.rpc_url(), self.harness.knobs.chain_id).await;
        why.map(|why| format!("{}: {why}", node.container))
    }
}

impl Harness {
    /// Wait until the chain runs again after the persisted-state audit:
    /// a sealer leader ticks the boundary clock, every ingress takes
    /// submits, the batcher confirms a new post, and the executors
    /// advance. The steps run in this order and share one budget.
    ///
    /// # Errors
    ///
    /// Returns an error that names the first step that did not hold
    /// within the budget, with its last reading.
    pub async fn await_chain_recovered(&self) -> anyhow::Result<()> {
        let started = Instant::now();
        for step in Step::ORDER {
            self.await_recovery_step(step, started).await?;
        }
        crate::log(format!(
            "the chain recovered after the persisted-state audit in {}s",
            started.elapsed().as_secs()
        ));
        Ok(())
    }

    async fn await_recovery_step(&self, step: Step, started: Instant) -> anyhow::Result<()> {
        let wait = StepWait::new(self, step);
        let wait = &wait;
        let budget = Budget::new(RECOVERY_BUDGET.saturating_sub(started.elapsed()), POLL);
        poll::until(budget, |_| async move { wait.observe().await })
            .await?
            .or_fail(|_| {
                crate::chaos_fail!(
                    "the chain did not recover after the persisted-state audit: {} after {} s ({})",
                    step.failure(),
                    started.elapsed().as_secs(),
                    wait.last.borrow()
                )
            })?;
        crate::log(format!(
            "persisted-state recovery: {} ({}s, {})",
            step.success(),
            started.elapsed().as_secs(),
            wait.last.borrow()
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Counter, Rise};

    #[test]
    fn only_the_heartbeat_lines_of_a_leader_count() {
        let logs = "2026-10-06T13:52:35Z cluster role=LEADER memberId=1\n\
            2026-10-06T13:52:40Z cluster boundary-clock TICK memberId=2 block=1800 role=FOLLOWER\n\
            2026-10-06T13:52:41Z cluster boundary-clock TICK memberId=1 block=1809 role=LEADER\n\
            2026-10-06T13:52:49Z cluster boundary-clock TICK memberId=1 block=1839 role=LEADER\n";
        assert_eq!(Counter::leader_ticks(logs), 2);
        assert_eq!(Counter::leader_ticks("cluster role=LEADER memberId=1"), 0);
    }

    #[test]
    fn a_rise_starts_at_the_first_answer_and_needs_a_higher_reading() {
        let rise = Rise::default();
        assert!(!rise.observe(None), "no answer is no base");
        assert!(!rise.observe(Some(7)), "the first answer is the base");
        assert!(!rise.observe(None), "no answer is no rise");
        assert!(!rise.observe(Some(7)));
        assert!(rise.observe(Some(8)));
        assert_eq!(
            rise.describe(Counter::BatcherPosts, Some(8)),
            "batcher posts 7 -> 8"
        );
    }
}
