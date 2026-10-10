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
use crate::cases::cache::wait_readers_degraded;
use crate::cases::chain_status::ChainView;
use crate::cases::component::executor_containers;
use crate::cases::fleet::{FULL_RESTART_ELECTION, await_exporter_back, sealers};
use crate::cases::validator::COMMITTED;
use crate::harness::Harness;
use crate::nomad::{Alloc, Job, SavedJob};
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

mod checks;
#[cfg(test)]
mod tests;

use checks::Baseline;
pub(crate) use checks::Check;

/// How long every class stays down before the first one returns. The
/// expectation while down is checked inside it.
const HOLD: Duration = Duration::from_secs(60);
/// The window between the two head readings that judge the expectation.
const WINDOW: Duration = Duration::from_secs(15);
/// How long a live ingress gets to refuse a submit on the lost quorum:
/// the sealer silence an ingress tolerates, then its next status poll.
const REFUSAL_BUDGET: Duration = Duration::from_secs(60);
/// The cause an ingress names while no sealer answers.
const NO_QUORUM: &str = "sealer_no_quorum";
/// The task of each sequencer lane on a sequencer node.
const LANES: [&str; 2] = ["sequencer-0", "sequencer-1"];

/// A class of services a case takes down, in the order the data flows: a
/// class takes what it needs from the classes before it. The sealers
/// order; the executors execute and publish receipts; the validator and
/// the state mirrors follow the executors; Redis holds what the mirrors
/// write; the sequencers learn their floors from the executors and from
/// Redis; the ingresses need a sealer for their session and the
/// executors for their receipts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Class {
    /// The three Raft members, on their own nodes.
    Sealer,
    /// The three executor tasks, with their job stopped. The nodes stay
    /// up, so the mirrors on them can return before the executors.
    Executor,
    /// The three executor nodes, with their mirrors and drivers.
    ExecutorNodes,
    /// The one validator task, on the aux node.
    Validator,
    /// The three state mirrors, one on each executor node.
    StateMirror,
    /// The redis job: the primary, the replica and three sentinels.
    Redis,
    /// The four sequencer replicas: two lanes on each of two nodes.
    Sequencer,
    /// The two ingress replicas.
    Ingress,
}

/// How a class goes down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FaultKind {
    /// Hard-kill the task containers, then stop the job, so that Nomad
    /// does not start them again. The job's restore brings them back.
    KillTasks,
    /// `docker kill` the whole nodes. `docker start` brings them back.
    KillNodes,
    /// Stop the job. Redis keeps nothing on disk, so a stop loses the
    /// cache as a kill does. The job's restore brings it back.
    StopJob,
}

/// The fault of a class, with what it hits.
enum Fault {
    /// The task containers `(node, task)` of the job.
    KillTasks(Vec<(String, &'static str)>),
    /// The node containers.
    KillNodes(Vec<String>),
    /// The job alone.
    StopJob,
}

/// What holds a class down, and brings it back.
enum Held {
    /// The stopped job, as Nomad held it before the case.
    Job(SavedJob),
    /// The killed nodes.
    Nodes(Vec<String>),
}

/// A class whose return has started, and what proves that it runs.
enum Pending<'a> {
    /// The allocations of the posted job version run.
    Job { job: &'a SavedJob, desired: Job },
    /// The job on the started nodes reaches its count.
    Count { job: &'static str, count: usize },
}

impl Class {
    fn job(self) -> &'static str {
        match self {
            Self::Sealer => CLUSTER_TASK,
            Self::Executor | Self::ExecutorNodes => "executor",
            Self::Validator => "validator",
            Self::StateMirror => "state-mirror",
            Self::Redis => "redis",
            Self::Sequencer => "sequencer",
            Self::Ingress => "ingress",
        }
    }

    /// The allocation count of the class when every replica runs.
    fn count(self) -> usize {
        match self {
            Self::Sealer | Self::Executor | Self::ExecutorNodes | Self::StateMirror => 3,
            Self::Validator => 1,
            Self::Redis => 5,
            Self::Sequencer => 4,
            Self::Ingress => 2,
        }
    }

    /// How the class goes down. The sealers and the executor nodes lose
    /// their nodes; Redis loses its job; the other classes lose their
    /// tasks and their job stops.
    fn fault_kind(self) -> FaultKind {
        match self {
            Self::Sealer | Self::ExecutorNodes => FaultKind::KillNodes,
            Self::Redis => FaultKind::StopJob,
            Self::Executor
            | Self::Validator
            | Self::StateMirror
            | Self::Sequencer
            | Self::Ingress => FaultKind::KillTasks,
        }
    }

    fn fault(self, h: &Harness) -> anyhow::Result<Fault> {
        let one_task = |nodes: &[String], task| {
            Fault::KillTasks(nodes.iter().map(|n| (n.clone(), task)).collect())
        };
        Ok(match self {
            Self::Sealer => Fault::KillNodes(sealers(h)?),
            Self::Executor => one_task(&executor_containers(h), "executor"),
            Self::ExecutorNodes => Fault::KillNodes(executor_containers(h)),
            Self::Validator => one_task(
                std::slice::from_ref(&h.probes.validator.container),
                "validator",
            ),
            Self::StateMirror => one_task(&executor_containers(h), "state-mirror"),
            Self::Redis => Fault::StopJob,
            Self::Sequencer => Fault::KillTasks(
                h.probes
                    .sequencers
                    .iter()
                    .flat_map(|n| LANES.iter().map(move |lane| (n.container.clone(), *lane)))
                    .collect(),
            ),
            Self::Ingress => one_task(&h.probes.ingress_containers(), "ingress"),
        })
    }

    async fn take_down(self, h: &mut Harness, ctx: &str) -> anyhow::Result<Held> {
        Ok(match self.fault(h)? {
            Fault::KillTasks(tasks) => {
                let listed: Vec<String> = tasks
                    .iter()
                    .map(|(node, task)| format!("{task}@{node}"))
                    .collect();
                crate::log(format!(
                    "{ctx}: hard-kill every {} task ({}) and stop the job",
                    self.job(),
                    listed.join(" ")
                ));
                Held::Job(h.kill_tasks_and_stop(self.job(), &tasks).await?)
            }
            Fault::KillNodes(nodes) => {
                h.kill_all_nodes(ctx, self.job(), &nodes).await?;
                Held::Nodes(nodes)
            }
            Fault::StopJob => {
                let job = SavedJob::capture(&h.nomad, self.job()).await?;
                crate::log(format!("{ctx}: stop the {} job", self.job()));
                job.stop().await?;
                Held::Job(job)
            }
        })
    }

    /// No allocation of the job restarted since its return: the class
    /// waited for what it needs. A crash loop shows as a restart count,
    /// also when the class got what it needs since, so the proof holds
    /// after the pause and again at the end.
    async fn assert_no_restart(self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let allocs = h.nomad.running(self.job()).await?;
        let restarted: Vec<String> = allocs.iter().filter_map(Alloc::restart_note).collect();
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
            "{ctx}: every {} allocation runs with no restart",
            self.job()
        ));
        Ok(())
    }
}

impl Held {
    /// Start the return and come back at once: post the job again, or
    /// `docker start` the nodes.
    async fn start(&self, h: &Harness, ctx: &str, class: Class) -> anyhow::Result<Pending<'_>> {
        crate::log(format!("{ctx}: bring the {} back", class.job()));
        Ok(match self {
            Self::Job(job) => Pending::Job {
                job,
                desired: job.post_restore().await?,
            },
            Self::Nodes(nodes) => {
                h.start_nodes(nodes).await?;
                Pending::for_nodes(class)
            }
        })
    }

    /// After a pause, a class that a job brought back must run with no
    /// restart. A class on killed nodes is judged by its count.
    async fn assert_waited(&self, h: &Harness, ctx: &str, class: Class) -> anyhow::Result<()> {
        match self {
            Self::Job(_) => class.assert_no_restart(h, ctx).await,
            Self::Nodes(_) => Ok(()),
        }
    }
}

impl Pending<'_> {
    /// The proof for a class on started nodes: its job at its count.
    fn for_nodes(class: Class) -> Self {
        Self::Count {
            job: class.job(),
            count: class.count(),
        }
    }

    /// Wait until the class runs.
    async fn await_running(self, h: &mut Harness) -> anyhow::Result<()> {
        match self {
            Self::Job { job, desired } => job.await_running(&desired).await,
            Self::Count { job, count } => h.assert_count(job, count, h.knobs.reschedule_slo).await,
        }
    }
}

/// The order and the pace of the return.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recovery {
    /// Every class returns in one go: every job is posted and every
    /// node started before the first wait.
    AllAtOnce,
    /// The classes return in the order of [`Class`], the stagger apart:
    /// each class finds the classes it takes from already up. The
    /// stagger starts when the class runs.
    Ordered(Duration),
    /// The classes return against the order of [`Class`], the stagger
    /// apart: a class returns before the class it takes from, and must
    /// wait.
    Reverse(Duration),
}

/// One wave of the return: the classes that start together, and the
/// pause before them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Wave {
    classes: Vec<Class>,
    after: Duration,
}

impl Recovery {
    /// The waves of the return for the classes in `down`.
    fn plan(self, down: &[Class]) -> Vec<Wave> {
        let mut order = down.to_vec();
        let stagger = match self {
            Self::AllAtOnce => {
                return vec![Wave {
                    classes: order,
                    after: Duration::ZERO,
                }];
            }
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
            .map(|(i, class)| Wave {
                classes: vec![class],
                after: if i == 0 { Duration::ZERO } else { stagger },
            })
            .collect()
    }
}

impl Wave {
    /// Pause, prove that the classes of the wave before waited, then
    /// start every class of this wave and wait for each one to run.
    async fn run(
        &self,
        h: &mut Harness,
        ctx: &str,
        taken: &BTreeMap<Class, Held>,
        before: Option<&Self>,
    ) -> anyhow::Result<()> {
        tokio::time::sleep(self.after).await;
        for class in before.map(|w| w.classes.as_slice()).unwrap_or_default() {
            taken[class].assert_waited(h, ctx, *class).await?;
        }
        let mut pending = Vec::with_capacity(self.classes.len());
        for class in &self.classes {
            pending.push(taken[class].start(h, ctx, *class).await?);
        }
        for started in pending {
            started.await_running(h).await?;
        }
        Ok(())
    }

    fn describe(&self) -> String {
        let jobs: Vec<&str> = self.classes.iter().map(|c| c.job()).collect();
        format!(
            "pause {}s, then {}",
            self.after.as_secs(),
            jobs.join(" and ")
        )
    }
}

/// The highest head any live client reports, and the highest count of
/// applied transactions any executor reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Progress {
    /// The highest of the executor block, the sealed head an ingress
    /// reads from the sealer's status frames, and the validator's
    /// committed block. Each one follows the sealer, so the highest one
    /// is the head of the chain as the live clients see it.
    block: i64,
    /// `None` while no executor exporter answers: no executor runs, so
    /// no transaction is applied.
    applied: Option<i64>,
}

impl Progress {
    /// One reading, retried for twelve seconds. Some client of the
    /// sealer is up through every combined case, so no head at all is a
    /// harness failure.
    async fn read(h: &Harness, ctx: &str) -> anyhow::Result<Self> {
        let outcome = poll::until(Budget::secs(12, 3), |_| async move {
            let block = h
                .probes
                .executor_progress()
                .await
                .max(h.probes.ingress_sealed_head().await)
                .max(h.probes.val_metric(COMMITTED).await);
            let applied = h.probes.executor_tx_applied().await;
            Ok(block.map(|block| Self { block, applied }))
        })
        .await?;
        outcome
            .or_fail(|_| {
                crate::chaos_fail!(
                    "{ctx}: no client reports a head, so the outage cannot be observed"
                )
            })
            .map(|(progress, _)| progress)
    }

    fn describe(self) -> String {
        let applied = self.applied.map_or("?".to_string(), |a| a.to_string());
        format!("head {}, applied {applied}", self.block)
    }
}

/// What the pipeline must do while the classes are down.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Expect {
    /// No block is committed: the head stays flat, and every live
    /// ingress refuses a submit with `sealer_no_quorum`.
    Stall,
    /// Blocks are still sealed, but no user transaction is applied: the
    /// head rises and the applied-transaction counter of a live executor
    /// stays flat. A head that reads lower is not a rise: a source of the
    /// head stopped answering.
    SealOnly,
}

impl Expect {
    /// Whether two readings, one window apart, show the expectation.
    fn judge(self, before: Progress, after: Progress) -> Result<(), String> {
        let applied = before.applied.zip(after.applied);
        match self {
            Self::Stall if after.block != before.block => Err(format!(
                "the pipeline UNEXPECTEDLY progressed while the sealers were down (head {} -> {})",
                before.block, after.block
            )),
            Self::SealOnly if after.block <= before.block => Err(format!(
                "no block was sealed while the sealers were up (head {} -> {})",
                before.block, after.block
            )),
            Self::SealOnly if applied.is_some_and(|(b, a)| a != b) => Err(format!(
                "a user transaction was applied while the classes were down ({} -> {})",
                before.describe(),
                after.describe()
            )),
            Self::Stall | Self::SealOnly => Ok(()),
        }
    }

    fn observed(self, before: Progress, after: Progress) -> String {
        match self {
            Self::Stall => format!(
                "the pipeline correctly STALLED (head {} -> {} over {}s)",
                before.block,
                after.block,
                WINDOW.as_secs()
            ),
            Self::SealOnly => format!(
                "the sealers tick with no user transaction ({} -> {} over {}s)",
                before.describe(),
                after.describe(),
                WINDOW.as_secs()
            ),
        }
    }

    /// Judge two readings one window apart. A stall with the ingresses
    /// up also needs every ingress to refuse on the lost quorum. With
    /// Redis down, a cold read must count as degraded, not stall.
    async fn assert(self, h: &Harness, ctx: &str, down: &[Class]) -> anyhow::Result<()> {
        let before = Progress::read(h, ctx).await?;
        tokio::time::sleep(WINDOW).await;
        let after = Progress::read(h, ctx).await?;
        self.judge(before, after)
            .map_err(|why| crate::chaos_fail!("{ctx}: {why}"))?;
        crate::log(format!("{ctx}: {}", self.observed(before, after)));
        if self == Self::Stall && !down.contains(&Class::Ingress) {
            h.assert_ingresses_refuse(ctx).await?;
        }
        if down.contains(&Class::Redis) {
            wait_readers_degraded(h, ctx).await?;
        }
        Ok(())
    }
}

impl Harness {
    /// Every ingress refuses a submit on the lost quorum within the
    /// budget.
    async fn assert_ingresses_refuse(&self, ctx: &str) -> anyhow::Result<()> {
        let budget = Budget::new(REFUSAL_BUDGET, Duration::from_secs(2));
        let outcome = poll::until(budget, |_| async move {
            let answers = self.refusals().await;
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
    async fn refusals(&self) -> Vec<String> {
        let mut lines = Vec::with_capacity(self.probes.ingresses.len());
        for node in &self.probes.ingresses {
            let why = ChainView::refusal_at(&node.rpc_url(), self.knobs.chain_id).await;
            lines.push(format!(
                "{}: {}",
                node.container,
                why.unwrap_or_else(|| "takes submits".to_string())
            ));
        }
        lines
    }
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

/// The executor nodes and the sealer nodes die. The executors return a
/// minute before the sealers, and must wait for them.
pub(crate) const EXECUTOR_SEALER: Combined = Combined {
    case: Case::ExecutorSealerLossRecover,
    down: &[Class::ExecutorNodes, Class::Sealer],
    expect: Expect::Stall,
    recovery: Recovery::Reverse(Duration::from_secs(60)),
    checks: &[Check::OneLeaderPerTerm],
};

/// The executors, the sealers and the validator die. They return in
/// dependency order, thirty seconds apart: the sealers, the executors,
/// then the validator.
pub(crate) const EXECUTOR_SEALER_VALIDATOR: Combined = Combined {
    case: Case::ExecutorSealerValidatorRecover,
    down: &[Class::Executor, Class::Sealer, Class::Validator],
    expect: Expect::Stall,
    recovery: Recovery::Ordered(Duration::from_secs(30)),
    checks: &[Check::OneLeaderPerTerm],
};

/// The ingresses and the executor nodes die. The sealers order the
/// transactions in flight. The executors return first and publish the
/// receipts of their replay while no ingress listens; the ingresses
/// return a minute later and must serve those receipts from the state
/// of an executor.
pub(crate) const INGRESS_EXECUTOR: Combined = Combined {
    case: Case::IngressExecutorLossRecover,
    down: &[Class::ExecutorNodes, Class::Ingress],
    expect: Expect::SealOnly,
    recovery: Recovery::Ordered(Duration::from_secs(60)),
    checks: &[Check::ReceiptFromState],
};

/// The whole read path dies: the executors, Redis and the state mirrors.
/// Redis returns empty, the mirrors return and wait for a checkpoint,
/// and the executors return last. Every mirror must rebuild, and the
/// readers must use Redis again.
pub(crate) const READ_PATH: Combined = Combined {
    case: Case::ReadPathLossRecover,
    down: &[Class::Executor, Class::Redis, Class::StateMirror],
    expect: Expect::SealOnly,
    recovery: Recovery::Reverse(Duration::from_secs(30)),
    checks: &[Check::EveryMirrorRebuilt, Check::ReadersRecovered],
};

/// The sequencers, the executors and Redis die: no source of a sender
/// floor is left. The sequencers return first and must park every
/// established sender, then Redis returns empty, then the executors.
/// The floors must come from a lookup, and no ref may sit below one.
pub(crate) const SEQUENCER_EXECUTOR_REDIS: Combined = Combined {
    case: Case::SequencerExecutorRedisLoss,
    down: &[Class::Sequencer, Class::Executor, Class::Redis],
    expect: Expect::SealOnly,
    recovery: Recovery::Reverse(Duration::from_secs(30)),
    checks: &[Check::FloorsLookedUp, Check::NoRefBelowFloor],
};

impl Combined {
    /// Take the classes down, hold them, judge the pipeline while they
    /// are down, bring them back as the recovery says, and check that
    /// everything is back.
    pub(crate) async fn run(&self, h: &mut Harness) -> anyhow::Result<()> {
        let ctx = self.case.name();
        let baseline = Baseline::read(h, ctx, self.checks).await?;
        let mut taken = BTreeMap::new();
        for class in self.down {
            taken.insert(*class, class.take_down(h, ctx).await?);
        }
        // The return is proven by the posted job versions and the counts
        // below, not by the replacement of the last killed container.
        h.killed = None;
        let held = Instant::now();
        self.expect.assert(h, ctx, self.down).await?;
        tokio::time::sleep(HOLD.saturating_sub(held.elapsed())).await;
        self.bring_back(h, ctx, &taken).await?;
        self.assert_back(h, ctx).await?;
        for check in self.checks {
            check.assert(h, ctx, &baseline).await?;
        }
        Ok(())
    }

    /// Bring every class back, wave by wave.
    async fn bring_back(
        &self,
        h: &mut Harness,
        ctx: &str,
        taken: &BTreeMap<Class, Held>,
    ) -> anyhow::Result<()> {
        let plan = self.recovery.plan(self.down);
        crate::log(format!(
            "{ctx}: return plan: {}",
            plan.iter()
                .map(Wave::describe)
                .collect::<Vec<_>>()
                .join("; ")
        ));
        for (i, wave) in plan.iter().enumerate() {
            wave.run(h, ctx, taken, i.checked_sub(1).map(|j| &plan[j]))
                .await?;
        }
        Ok(())
    }

    /// Whether the case takes an executor class down.
    fn executors_down(&self) -> bool {
        self.down
            .iter()
            .any(|c| matches!(c, Class::Executor | Class::ExecutorNodes))
    }

    /// The classes whose job must show no restart at the end: the ones
    /// a job restore brought back.
    fn restart_proofs(&self) -> Vec<Class> {
        self.down
            .iter()
            .copied()
            .filter(|c| c.fault_kind() != FaultKind::KillNodes)
            .collect()
    }

    /// No returned job restarted, the members elected a leader when
    /// they were down, both ingresses are live, and the executors
    /// advance. A returned executor runs before its exporter binds, so
    /// the progress reading waits for an exporter first.
    async fn assert_back(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        for class in self.restart_proofs() {
            class.assert_no_restart(h, ctx).await?;
        }
        if self.down.contains(&Class::Sealer) {
            let leader = h.evidence.cluster_leader(FULL_RESTART_ELECTION).await?;
            crate::log(format!("{ctx}: members elected memberId={leader}"));
        }
        h.assert_ingress_pair_live(ctx).await?;
        if self.executors_down() {
            await_exporter_back(h, ctx).await?;
        }
        h.assert_executor_progress(Duration::from_mins(3)).await
    }
}
