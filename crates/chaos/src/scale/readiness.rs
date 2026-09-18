//! A resize barrier proves the desired allocations and their metrics together.

use std::collections::BTreeSet;

use anyhow::Context;

use super::{Resize, lane_target};
use crate::metrics;
use crate::nomad::{Alloc, Job, Nomad};

pub(super) struct LaneReadiness<'a> {
    resize: &'a Resize,
    nomad: Nomad,
    job: Job,
    groups: BTreeSet<String>,
    metrics: &'a [&'a str],
}

impl<'a> LaneReadiness<'a> {
    pub(super) async fn new(
        resize: &'a Resize,
        lanes: &[u32],
        metrics: &'a [&'a str],
    ) -> anyhow::Result<Self> {
        let nomad = Nomad::new(&resize.nomad_addr)?;
        let job = nomad.job("sequencer").await?;
        let groups: BTreeSet<String> = lanes.iter().map(|lane| format!("seq-{lane}")).collect();
        anyhow::ensure!(
            groups.iter().all(|g| job.has_group(g)),
            "requested resize lanes are missing or disabled in the current sequencer job"
        );
        Ok(Self {
            resize,
            nomad,
            job,
            groups,
            metrics,
        })
    }

    pub(super) async fn sample(&self) -> anyhow::Result<bool> {
        let allocs = self.nomad.allocations("sequencer").await?;
        let Some(running) = self.job.running(&allocs) else {
            return Ok(false);
        };
        let selected = running
            .into_iter()
            .filter(|a| self.groups.contains(&a.task_group));
        for alloc in selected {
            if !self.replica_idle(alloc).await? {
                return Ok(false);
            }
        }
        // A replacement during the scrape must supply its own evidence.
        let after = self.nomad.allocations("sequencer").await?;
        Ok(self.same_allocations(&allocs, &after)
            && self.nomad.job("sequencer").await?.version == self.job.version)
    }

    async fn replica_idle(&self, alloc: &Alloc) -> anyhow::Result<bool> {
        let node = self
            .resize
            .sequencers
            .get(&alloc.node_name)
            .with_context(|| {
                format!(
                    "allocation {} has unknown sequencer node {}",
                    alloc.id, alloc.node_name
                )
            })?;
        let lane: u32 = alloc
            .task_group
            .strip_prefix("seq-")
            .context("sequencer task group has no lane")?
            .parse()
            .context("invalid sequencer lane")?;
        let body = self.resize.scrape.fetch(&lane_target(node, lane)).await;
        let idle = Self::idle(body.as_deref(), self.metrics);
        if !idle {
            crate::log(format!(
                "resize waiting for allocation {} on {} lane {lane}: {} missing or nonzero",
                alloc.id,
                alloc.node_name,
                self.metrics.join(", ")
            ));
        }
        Ok(idle)
    }

    fn idle(body: Option<&str>, metrics: &[&str]) -> bool {
        body.is_some_and(|body| {
            metrics
                .iter()
                .all(|metric| metrics::sum(body, metric) == Some(0))
        })
    }

    fn same_allocations(&self, before: &[Alloc], after: &[Alloc]) -> bool {
        let ids = |allocs: &[Alloc]| {
            self.job.running(allocs).map(|running| {
                running
                    .into_iter()
                    .map(|a| a.id.clone())
                    .collect::<BTreeSet<_>>()
            })
        };
        matches!((ids(before), ids(after)), (Some(a), Some(b)) if a == b)
    }
}

#[cfg(test)]
mod tests;
