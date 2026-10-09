//! The combined outages: two or three classes of services are down at
//! the same time. The fleet cases prove that one class returns on its
//! own. These cases prove the order of the return: a class that comes
//! back before the class it needs must wait for it, and a class that
//! comes back after it must catch up. Each case is one row of a table:
//! the classes it takes down, what the pipeline must do while they are
//! down, the order and the pace of the return, and the checks after it.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use crate::cases::Case;
use crate::cases::chain_status::ChainView;
use crate::cases::fleet::{FULL_RESTART_ELECTION, names, sealers};
use crate::harness::Harness;
use crate::nomad::{Alloc, SavedJob};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

mod checks;
#[cfg(test)]
mod tests;

pub(crate) use checks::Check;
use checks::Refills;

/// How long every class stays down before the first one returns. The
/// expectation while down is checked inside it.
const HOLD: Duration = Duration::from_secs(60);
/// The window between the two executor readings that judge the
/// expectation.
const WINDOW: Duration = Duration::from_secs(15);
/// How long a live ingress gets to refuse a submit on the lost quorum:
/// the sealer silence an ingress tolerates, then its next status poll.
const REFUSAL_BUDGET: Duration = Duration::from_secs(60);
/// The cause an ingress names while no sealer answers.
const NO_QUORUM: &str = "sealer_no_quorum";
/// The task of each sequencer lane on a sequencer node.
const LANES: [&str; 2] = ["sequencer-0", "sequencer-1"];
/// A class of services a case takes down, in dependency order: a class
/// needs every class before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Class {
    /// The three Raft members, on their own nodes.
    Sealer,
    /// The four sequencer replicas: two lanes on each of two nodes.
    Sequencer,
    /// The two ingress replicas.
    Ingress,
}

/// How a class goes down.
enum Fault {
    /// Hard-kill the task containers `(node, task)`, then stop the job,
    /// so that Nomad does not start them again.
    KillTasks(Vec<(String, &'static str)>),
    /// `docker kill` the whole nodes.
    KillNodes(Vec<String>),
}

/// What holds a class down, and brings it back.
enum Held {
    /// The stopped job, as Nomad held it before the case.
    Job(SavedJob),
    /// The killed nodes.
    Nodes(Vec<String>),
}

impl Class {
    fn job(self) -> &'static str {
        match self {
            Self::Sealer => CLUSTER_TASK,
            Self::Sequencer => "sequencer",
            Self::Ingress => "ingress",
        }
    }

    /// The allocation count of the class when every replica runs.
    fn count(self) -> usize {
        match self {
            Self::Sealer => 3,
            Self::Sequencer => 4,
            Self::Ingress => 2,
        }
    }

    fn fault(self, h: &Harness) -> anyhow::Result<Fault> {
        Ok(match self {
            Self::Sealer => Fault::KillNodes(sealers(h)?),
            Self::Sequencer => Fault::KillTasks(
                h.probes
                    .sequencers
                    .iter()
                    .flat_map(|n| LANES.iter().map(move |lane| (n.container.clone(), *lane)))
                    .collect(),
            ),
            Self::Ingress => Fault::KillTasks(
                h.probes
                    .ingresses
                    .iter()
                    .map(|n| (n.container.clone(), "ingress"))
                    .collect(),
            ),
        })
    }

    async fn take_down(self, h: &mut Harness, ctx: &str) -> anyhow::Result<Held> {
        Ok(match self.fault(h)? {
            Fault::KillTasks(tasks) => Held::Job(self.kill_tasks_and_stop(h, ctx, &tasks).await?),
            Fault::KillNodes(nodes) => Held::Nodes(self.kill_nodes(h, ctx, nodes).await?),
        })
    }

    /// Hard-kill every task of the job, then stop the job. Nomad restarts
    /// a killed task in seconds, and a task that runs again before its
    /// peers are down is a single-replica case, not a combined one.
    async fn kill_tasks_and_stop(
        self,
        h: &mut Harness,
        ctx: &str,
        tasks: &[(String, &'static str)],
    ) -> anyhow::Result<SavedJob> {
        let job = SavedJob::capture(&h.nomad, self.job()).await?;
        let listed: Vec<String> = tasks
            .iter()
            .map(|(node, task)| format!("{task}@{node}"))
            .collect();
        crate::log(format!(
            "{ctx}: hard-kill every {} task ({}) and stop the job",
            self.job(),
            listed.join(" ")
        ));
        for (node, task) in tasks {
            h.inject_hard(&[node], task).await?;
        }
        job.stop().await?;
        Ok(job)
    }

    async fn kill_nodes(
        self,
        h: &Harness,
        ctx: &str,
        nodes: Vec<String>,
    ) -> anyhow::Result<Vec<String>> {
        crate::log(format!(
            "{ctx}: docker kill every {} node ({})",
            self.job(),
            nodes.join(" ")
        ));
        h.kill_nodes(&names(&nodes)).await?;
        Ok(nodes)
    }

    /// No allocation of the job restarted since its return: the class
    /// waited for what it needs. A crash loop shows as a restart count.
    async fn assert_no_restart(self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let allocs = h.nomad.running(self.job()).await?;
        let restarted: Vec<String> = allocs
            .iter()
            .map(|a| (a.short_id(), task_restarts(a)))
            .filter(|(_, n)| *n > 0)
            .map(|(id, n)| format!("{id} restarted {n} times"))
            .collect();
        anyhow::ensure!(
            allocs.len() == self.count() && restarted.is_empty(),
            "{}: {ctx}: the {} job did not wait for the classes it needs: {} of {} running, {}",
            crate::FAIL_PREFIX,
            self.job(),
            allocs.len(),
            self.count(),
            restarted.join(", ")
        );
        crate::log(format!(
            "{ctx}: every {} allocation runs with no restart while it waits",
            self.job()
        ));
        Ok(())
    }
}

/// The restarts of every task of an allocation, as Nomad counts them.
fn task_restarts(alloc: &Alloc) -> u64 {
    alloc
        .task_states
        .values()
        .filter_map(|state| state["Restarts"].as_u64())
        .sum()
}

impl Held {
    async fn bring_back(&self, h: &Harness, ctx: &str, class: Class) -> anyhow::Result<()> {
        crate::log(format!("{ctx}: bring the {} back", class.job()));
        match self {
            Self::Job(job) => job.restore().await,
            Self::Nodes(nodes) => h.start_nodes(nodes).await,
        }
    }

    /// After a pause, a class that a job brought back must run with no
    /// restart. A class on killed nodes is judged by its count later.
    async fn assert_waited(&self, h: &Harness, ctx: &str, class: Class) -> anyhow::Result<()> {
        match self {
            Self::Job(_) => class.assert_no_restart(h, ctx).await,
            Self::Nodes(_) => Ok(()),
        }
    }
}

/// The order and the pace of the return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recovery {
    /// Every class returns in one go.
    AllAtOnce,
    /// The classes return in dependency order, the stagger apart: the
    /// sealers, then the sequencers, then the ingresses.
    Ordered(Duration),
    /// The classes return against the dependency order, the stagger
    /// apart: a class returns before the class it needs, and must wait.
    Reverse(Duration),
}

/// One return: the class, and the pause before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Step {
    class: Class,
    after: Duration,
}

impl Recovery {
    /// The return steps for the classes in `down`.
    fn plan(self, down: &[Class]) -> Vec<Step> {
        let mut order = down.to_vec();
        let stagger = match self {
            Self::AllAtOnce => Duration::ZERO,
            Self::Ordered(stagger) => {
                order.sort_unstable();
                stagger
            }
            Self::Reverse(stagger) => {
                order.sort_unstable_by(|a, b| b.cmp(a));
                stagger
            }
        };
        order
            .into_iter()
            .enumerate()
            .map(|(i, class)| Step {
                class,
                after: if i == 0 { Duration::ZERO } else { stagger },
            })
            .collect()
    }
}

/// The highest block and the highest count of applied transactions any
/// executor reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Progress {
    block: i64,
    applied: i64,
}

impl Progress {
    /// One reading, retried for twelve seconds. The executors run
    /// through every combined case, so a dark gauge is a harness failure.
    async fn read(h: &Harness, ctx: &str) -> anyhow::Result<Self> {
        let outcome = poll::until(Budget::secs(12, 3), |_| async move {
            Ok(h.probes
                .executor_progress()
                .await
                .zip(h.probes.executor_tx_applied().await)
                .map(|(block, applied)| Self { block, applied }))
        })
        .await?;
        outcome
            .or_fail(|_| {
                crate::chaos_fail!(
                    "{ctx}: no executor gauge answers, so the outage cannot be observed"
                )
            })
            .map(|(progress, _)| progress)
    }
}

/// What the pipeline must do while the classes are down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expect {
    /// No block is committed: the executor gauge stays flat, and every
    /// live ingress refuses a submit with `sealer_no_quorum`.
    Stall,
    /// Blocks are still sealed, but they hold no user transaction: the
    /// executor gauge advances and the applied-transaction counter stays
    /// flat.
    SealOnly,
}

impl Expect {
    /// Whether two readings, one window apart, show the expectation.
    fn judge(self, before: Progress, after: Progress) -> Result<(), String> {
        match self {
            Self::Stall if after.block != before.block => Err(format!(
                "the pipeline UNEXPECTEDLY progressed while the sealers were down (executor block {} -> {})",
                before.block, after.block
            )),
            Self::SealOnly if after.block == before.block => Err(format!(
                "no block was sealed while only the client edge was down (executor block {} -> {})",
                before.block, after.block
            )),
            Self::SealOnly if after.applied != before.applied => Err(format!(
                "a user transaction was applied while no ingress and no sequencer ran (applied {} -> {})",
                before.applied, after.applied
            )),
            Self::Stall | Self::SealOnly => Ok(()),
        }
    }

    fn observed(self, before: Progress, after: Progress) -> String {
        match self {
            Self::Stall => format!(
                "the pipeline correctly STALLED (executor block {} -> {} over {}s)",
                before.block,
                after.block,
                WINDOW.as_secs()
            ),
            Self::SealOnly => format!(
                "the sealers tick with no user transaction (executor block {} -> {}, applied {} -> {} over {}s)",
                before.block,
                after.block,
                before.applied,
                after.applied,
                WINDOW.as_secs()
            ),
        }
    }

    /// Judge two readings one window apart. A stall with the ingresses
    /// up also needs every ingress to refuse on the lost quorum.
    async fn assert(self, h: &Harness, ctx: &str, down: &[Class]) -> anyhow::Result<()> {
        let before = Progress::read(h, ctx).await?;
        tokio::time::sleep(WINDOW).await;
        let after = Progress::read(h, ctx).await?;
        self.judge(before, after)
            .map_err(|why| crate::chaos_fail!("{ctx}: {why}"))?;
        crate::log(format!("{ctx}: {}", self.observed(before, after)));
        if self == Self::Stall && !down.contains(&Class::Ingress) {
            assert_ingresses_refuse(h, ctx).await?;
        }
        Ok(())
    }
}

/// Every ingress refuses a submit on the lost quorum within the budget.
async fn assert_ingresses_refuse(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let budget = Budget::new(REFUSAL_BUDGET, Duration::from_secs(2));
    let outcome = poll::until(budget, |_| async move {
        let answers = refusals(h).await;
        let all = answers.iter().all(|a| a.contains(NO_QUORUM));
        Ok::<_, anyhow::Error>(all.then_some(answers))
    })
    .await?;
    let (answers, elapsed) = outcome.or_fail(|t| {
        crate::chaos_fail!(
            "{ctx}: an ingress does not refuse on {NO_QUORUM} after {}s without a sealer",
            t.as_secs()
        )
    })?;
    crate::log(format!(
        "{ctx}: every ingress refuses on {NO_QUORUM} after {}s ({})",
        elapsed.as_secs(),
        answers.join("; ")
    ));
    Ok(())
}

/// One line per ingress: its refusal, or that it takes submits.
async fn refusals(h: &Harness) -> Vec<String> {
    let mut lines = Vec::with_capacity(h.probes.ingresses.len());
    for node in &h.probes.ingresses {
        let why = ChainView::refusal_at(&node.rpc_url(), h.knobs.chain_id).await;
        lines.push(format!(
            "{}: {}",
            node.container,
            why.unwrap_or_else(|| "takes submits".to_string())
        ));
    }
    lines
}

/// One combined case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Combined {
    pub(crate) case: Case,
    /// The classes the case takes down, in that order.
    pub(crate) down: &'static [Class],
    pub(crate) expect: Expect,
    pub(crate) recovery: Recovery,
    pub(crate) checks: &'static [Check],
}

/// The ingresses and the sequencers die. The sealers tick on with empty
/// blocks. The sequencers return first, then the ingresses.
pub(crate) const INGRESS_SEQUENCER: Combined = Combined {
    case: Case::IngressSequencerLossRecover,
    down: &[Class::Ingress, Class::Sequencer],
    expect: Expect::SealOnly,
    recovery: Recovery::Ordered(Duration::from_secs(30)),
    checks: &[Check::NoOriginGap, Check::NoRefBelowFloor],
};

/// The ingresses and the sealers die. The ingresses return a minute
/// before the sealers, and must wait for them.
pub(crate) const INGRESS_SEALER: Combined = Combined {
    case: Case::IngressSealerLossRecover,
    down: &[Class::Ingress, Class::Sealer],
    expect: Expect::Stall,
    recovery: Recovery::Reverse(Duration::from_secs(60)),
    checks: &[Check::OneLeaderPerTerm],
};

/// The sequencers and the sealers die, and return together. The
/// sequencers lose their epoch queues, so the da-watcher must fill them.
pub(crate) const SEQUENCER_SEALER: Combined = Combined {
    case: Case::SequencerSealerLossRecover,
    down: &[Class::Sequencer, Class::Sealer],
    expect: Expect::Stall,
    recovery: Recovery::AllAtOnce,
    checks: &[
        Check::OneLeaderPerTerm,
        Check::NoOriginGap,
        Check::EpochRefill,
    ],
};

/// All three classes die. They return in dependency order, thirty
/// seconds apart.
pub(crate) const ALL_THREE: Combined = Combined {
    case: Case::IngressSequencerSealerLossRecover,
    down: &[Class::Ingress, Class::Sequencer, Class::Sealer],
    expect: Expect::Stall,
    recovery: Recovery::Ordered(Duration::from_secs(30)),
    checks: &[Check::OneLeaderPerTerm, Check::NoOriginGap],
};

/// All three classes die. They return against the dependency order,
/// forty-five seconds apart: each one waits for the next.
pub(crate) const ALL_THREE_REVERSE: Combined = Combined {
    case: Case::IngressSequencerSealerReverse,
    down: &[Class::Ingress, Class::Sequencer, Class::Sealer],
    expect: Expect::Stall,
    recovery: Recovery::Reverse(Duration::from_secs(45)),
    checks: &[Check::OneLeaderPerTerm, Check::NoOriginGap],
};

impl Combined {
    /// Take the classes down, hold them, judge the pipeline while they
    /// are down, bring them back as the recovery says, and check that
    /// everything is back.
    pub(crate) async fn run(&self, h: &mut Harness) -> anyhow::Result<()> {
        let ctx = self.case.name();
        let refills = Refills::read(h).await;
        let mut taken = BTreeMap::new();
        for class in self.down {
            taken.insert(*class, class.take_down(h, ctx).await?);
        }
        let held = Instant::now();
        self.expect.assert(h, ctx, self.down).await?;
        tokio::time::sleep(HOLD.saturating_sub(held.elapsed())).await;
        self.bring_back(h, ctx, &taken).await?;
        self.assert_back(h, ctx).await?;
        for check in self.checks {
            check.assert(h, ctx, refills).await?;
        }
        Ok(())
    }

    /// Bring every class back as the plan says. After each pause, the
    /// class that returned before it must have waited.
    async fn bring_back(
        &self,
        h: &Harness,
        ctx: &str,
        taken: &BTreeMap<Class, Held>,
    ) -> anyhow::Result<()> {
        let plan = self.recovery.plan(self.down);
        for (i, step) in plan.iter().enumerate() {
            Self::return_one(h, ctx, &taken[&step.class], *step, plan.get(i + 1)).await?;
        }
        Ok(())
    }

    /// Bring one class back, then pause until the next step. The pause
    /// ends with the proof that the class waited.
    async fn return_one(
        h: &Harness,
        ctx: &str,
        held: &Held,
        step: Step,
        next: Option<&Step>,
    ) -> anyhow::Result<()> {
        held.bring_back(h, ctx, step.class).await?;
        let Some(next) = next else {
            return Ok(());
        };
        crate::log(format!(
            "{ctx}: pause {}s before the {} return",
            next.after.as_secs(),
            next.class.job()
        ));
        tokio::time::sleep(next.after).await;
        held.assert_waited(h, ctx, step.class).await
    }

    /// Every class is back at its count, the members elected a leader
    /// when they were down, both ingresses are live, and the executors
    /// advance.
    async fn assert_back(&self, h: &mut Harness, ctx: &str) -> anyhow::Result<()> {
        let mut classes = self.down.to_vec();
        classes.sort_unstable();
        for class in classes {
            h.assert_count(class.job(), class.count(), h.knobs.reschedule_slo)
                .await?;
        }
        if self.down.contains(&Class::Sealer) {
            let leader = h.evidence.cluster_leader(FULL_RESTART_ELECTION).await?;
            crate::log(format!("{ctx}: members elected memberId={leader}"));
        }
        h.assert_ingress_pair_live(ctx).await?;
        h.assert_executor_progress(Duration::from_mins(3)).await
    }
}
