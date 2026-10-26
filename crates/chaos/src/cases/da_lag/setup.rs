//! A small DA guard belongs only to its halt fault window.

use std::num::{NonZeroU64, NonZeroUsize};

use anyhow::Context;
use serde_json::Value;

use super::{await_halt, da_lag_halt, status};
use crate::harness::Harness;
use crate::metrics;
use crate::nomad::SavedJob;
use crate::poll::{self, Budget};
use crate::probes::CLUSTER_TASK;

const SMALL_BUDGET: NonZeroU64 = NonZeroU64::new(8).unwrap();
const PROPERTY: &str = "-Dkardamom.cluster.daLagBudgetBlocks=";
const BUDGET_METRIC: &str = "kardamom_ingress_cluster_da_lag_budget_blocks";

#[derive(Clone, Copy)]
pub(crate) enum GuardedCase {
    Halt,
    Canary,
}

impl GuardedCase {
    pub(crate) async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        if h.knobs.da_lag_budget_blocks.is_some() {
            return self.exercise(h).await;
        }
        SmallBudget::run(h, self).await
    }

    async fn exercise(self, h: &mut Harness) -> anyhow::Result<()> {
        match self {
            Self::Halt => da_lag_halt(h).await,
            Self::Canary => super::super::l1_canary::canary_da_lag(h).await,
        }
    }
}

struct SmallBudget;

impl SmallBudget {
    async fn run(h: &mut Harness, case: GuardedCase) -> anyhow::Result<()> {
        Self::await_caught_up(h).await?;
        let previous = Self::observed_budget(h)
            .await
            .context("the ingress exports no DA guard")?;
        let saved = SavedJob::capture(&h.nomad, CLUSTER_TASK).await?;
        let configured = BudgetJob::new(saved.definition(), SMALL_BUDGET)?;
        crate::log(format!(
            "da-lag-halt: set the case guard to {SMALL_BUDGET} blocks on {} task definitions",
            configured.tasks
        ));
        h.knobs.da_lag_budget_blocks = Some(SMALL_BUDGET);
        let verdict = Self::exercise(h, &saved, &configured, case).await;
        let restored = match saved.restore().await {
            Ok(()) => Self::await_budget(h, previous).await,
            Err(error) => Err(error),
        };
        h.knobs.da_lag_budget_blocks = None;
        if let Err(error) = &restored {
            crate::log(format!(
                "da-lag-halt: restoring the saved cluster job fails: {error:#}"
            ));
        }
        verdict?;
        restored?;
        h.assert_progress().await
    }

    async fn exercise(
        h: &mut Harness,
        saved: &SavedJob,
        configured: &BudgetJob,
        case: GuardedCase,
    ) -> anyhow::Result<()> {
        saved.register(&configured.definition).await?;
        h.assert_count(CLUSTER_TASK, 3, h.knobs.restart_slo).await?;
        Self::await_budget(h, 8).await?;
        await_halt(h, "da-lag-halt: guard setup", 8, false).await?;
        case.exercise(h).await
    }

    async fn await_caught_up(h: &Harness) -> anyhow::Result<()> {
        poll::until(Budget::secs(120, 2), |_| async move {
            Ok::<_, anyhow::Error>(status(h).await.filter(|sample| {
                !sample.halted && sample.sealed.saturating_sub(sample.posted) <= 4
            }))
        })
        .await?
        .or_fail(|_| {
            crate::chaos_fail!("da-lag-halt: posting is not caught up before guard setup")
        })?;
        Ok(())
    }

    async fn observed_budget(h: &Harness) -> Option<i64> {
        let body = h
            .probes
            .scrape()
            .fetch(&h.probes.ingress_target(&h.probes.ingresses[0]))
            .await?;
        metrics::first(&body, BUDGET_METRIC)
    }

    async fn await_budget(h: &Harness, expected: i64) -> anyhow::Result<()> {
        poll::until(Budget::secs(120, 2), |_| async move {
            Ok::<_, anyhow::Error>(
                Self::observed_budget(h)
                    .await
                    .filter(|budget| *budget == expected),
            )
        })
        .await?
        .or_fail(|_| {
            crate::chaos_fail!("da-lag-halt: active DA guard does not reach {expected}")
        })?;
        Ok(())
    }
}

struct BudgetJob {
    definition: Value,
    tasks: NonZeroUsize,
}

impl BudgetJob {
    fn new(saved: &Value, budget: NonZeroU64) -> anyhow::Result<Self> {
        let mut definition = saved.clone();
        let groups = definition["TaskGroups"]
            .as_array_mut()
            .context("cluster job has no task groups")?;
        let count = groups.iter_mut().try_fold(0usize, |count, group| {
            let tasks = group["Tasks"]
                .as_array_mut()
                .context("cluster group has no tasks")?;
            tasks.iter_mut().try_fold(count, |count, task| {
                count
                    .checked_add(usize::from(Self::set_task(task, budget)?))
                    .context("cluster task count overflows")
            })
        })?;
        Ok(Self {
            definition,
            tasks: NonZeroUsize::new(count).context("cluster job has no cluster task")?,
        })
    }

    fn set_task(task: &mut Value, budget: NonZeroU64) -> anyhow::Result<bool> {
        if task["Name"] != CLUSTER_TASK {
            return Ok(false);
        }
        let options = task["Env"]["JAVA_TOOL_OPTIONS"]
            .as_str()
            .context("cluster task has no JVM options")?;
        let positions: Vec<_> = options
            .match_indices(PROPERTY)
            .map(|(at, _)| at)
            .filter(|at| {
                options[..*at]
                    .chars()
                    .next_back()
                    .is_none_or(char::is_whitespace)
            })
            .collect();
        let [start] = positions.as_slice() else {
            anyhow::bail!("cluster task must set its DA guard once");
        };
        let tail = &options[*start..];
        let end = tail.find(char::is_whitespace).unwrap_or(tail.len());
        let changed = format!("{}{PROPERTY}{budget}{}", &options[..*start], &tail[end..]);
        task["Env"]["JAVA_TOOL_OPTIONS"] = Value::String(changed);
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
