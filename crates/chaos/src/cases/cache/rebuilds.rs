//! A rebuild completes on each mirror, even when one task rebuilds repeatedly.

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::evidence::without_ansi;
use crate::harness::Harness;
use crate::nomad::{Alloc, Streams};
use crate::poll::{self, Budget};

#[derive(Debug)]
struct Mirror {
    allocation: String,
    completed: usize,
}

#[derive(Debug, Default)]
pub(crate) struct MirrorRebuilds {
    mirrors: BTreeMap<String, Mirror>,
}

impl MirrorRebuilds {
    pub(crate) async fn read(h: &Harness) -> anyhow::Result<Self> {
        let counts = Self::sample(h).await?;
        anyhow::ensure!(
            h.contract
                .of_role(h.knobs.executor_nodes.role())
                .iter()
                .all(|node| counts.mirrors.contains_key(&node.name)),
            "{}: cannot capture rebuild baseline: a mirror is missing: {counts:?}",
            crate::FAIL_PREFIX
        );
        Ok(counts)
    }

    async fn sample(h: &Harness) -> anyhow::Result<Self> {
        let allocations = h.nomad.allocations("state-mirror").await?;
        let mut counts = Self::default();
        for allocation in allocations.iter().filter(|a| a.is_desired_running()) {
            counts.sample_allocation(h, allocation).await?;
        }
        Ok(counts)
    }

    async fn sample_allocation(&mut self, h: &Harness, allocation: &Alloc) -> anyhow::Result<()> {
        let logs = h.nomad.alloc_logs(allocation, Streams::Both).await?;
        let mirror = Mirror {
            allocation: allocation.id.clone(),
            completed: logs
                .lines()
                .filter(|line| without_ansi(line).contains("rebuild: done"))
                .count(),
        };
        anyhow::ensure!(
            self.mirrors
                .insert(allocation.node_name.clone(), mirror)
                .is_none(),
            "{}: two live mirrors share {}",
            crate::FAIL_PREFIX,
            allocation.node_name
        );
        Ok(())
    }

    fn pending(&self, now: &Self) -> Vec<String> {
        self.mirrors
            .iter()
            .filter_map(|(node, before)| {
                let rebuilt = now.mirrors.get(node).is_some_and(|after| {
                    let baseline = if before.allocation == after.allocation {
                        before.completed
                    } else {
                        0
                    };
                    after.completed > baseline
                });
                (!rebuilt).then(|| node.clone())
            })
            .collect()
    }

    pub(crate) async fn await_all(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let pending = RefCell::new(self.mirrors.keys().cloned().collect::<Vec<_>>());
        let pending_ref = &pending;
        let outcome = poll::until(Budget::secs(300, 5), |_| async move {
            let now = Self::sample(h).await?;
            let missing = self.pending(&now);
            let complete = missing.is_empty();
            pending_ref.replace(missing);
            Ok::<_, anyhow::Error>(complete.then_some(now))
        })
        .await?;
        let (now, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: mirrors without a completed rebuild after {}s: {:?}",
                t.as_secs(),
                pending.borrow()
            )
        })?;
        crate::log(format!(
            "{ctx}: every mirror rebuilt after {}s ({self:?} -> {now:?})",
            elapsed.as_secs()
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests;
