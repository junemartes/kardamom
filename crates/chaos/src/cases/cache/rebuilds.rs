//! A rebuild completes on each mirror, even when one task rebuilds repeatedly.

use std::cell::RefCell;
use std::collections::BTreeMap;

use crate::evidence::without_ansi;
use crate::harness::Harness;
use crate::nomad::{Alloc, Streams};
use crate::poll::{self, Budget};

#[derive(Debug, Default, serde::Serialize)]
struct Counts {
    scanning: usize,
    write_retries: usize,
    completed: usize,
}

impl Counts {
    fn read(logs: &str) -> Self {
        Self {
            scanning: Self::occurrences(logs, "rebuild: scanning"),
            write_retries: Self::occurrences(logs, "rebuild: write failed; retrying"),
            completed: Self::occurrences(logs, "rebuild: done"),
        }
    }

    fn occurrences(logs: &str, needle: &str) -> usize {
        logs.lines()
            .filter(|line| without_ansi(line).contains(needle))
            .count()
    }
}

#[derive(Debug, serde::Serialize)]
struct Mirror {
    allocation: String,
    counts: Counts,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum RebuildPhase {
    /// Every mirror completes a rebuild.
    Complete,
    /// At least one mirror completes a rebuild. A mirror that starts
    /// after another one published the shared heads finds them warm and
    /// resumes with no rebuild.
    FirstComplete,
    /// Every mirror scans, and retries a write, with no completed
    /// rebuild.
    BlockedScan,
}

impl RebuildPhase {
    fn ready(self, before: &Mirror, after: &Mirror) -> bool {
        let counts = (before.allocation == after.allocation).then_some(&before.counts);
        let completed = counts.map_or(0, |counts| counts.completed);
        match self {
            Self::Complete | Self::FirstComplete => after.counts.completed > completed,
            Self::BlockedScan => {
                after.counts.completed == completed
                    && after.counts.scanning > counts.map_or(0, |counts| counts.scanning)
                    && after.counts.write_retries > counts.map_or(0, |counts| counts.write_retries)
            }
        }
    }

    /// Whether the phase holds with `pending` of `mirrors` mirrors not
    /// ready.
    fn reached(self, pending: usize, mirrors: usize) -> bool {
        match self {
            Self::FirstComplete => pending < mirrors,
            Self::Complete | Self::BlockedScan => pending == 0,
        }
    }

    fn budget(self) -> Budget {
        match self {
            Self::Complete | Self::FirstComplete => Budget::secs(300, 5),
            Self::BlockedScan => Budget::secs(180, 2),
        }
    }
}

#[derive(Debug, Default, serde::Serialize)]
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
            counts: Counts::read(&logs),
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

    fn pending(&self, now: &Self, phase: RebuildPhase) -> Vec<String> {
        self.mirrors
            .iter()
            .filter_map(|(node, before)| {
                let rebuilt = now
                    .mirrors
                    .get(node)
                    .is_some_and(|after| phase.ready(before, after));
                (!rebuilt).then(|| node.clone())
            })
            .collect()
    }

    pub(super) async fn await_phase(
        &self,
        h: &Harness,
        ctx: &str,
        phase: RebuildPhase,
    ) -> anyhow::Result<Self> {
        let pending = RefCell::new(self.mirrors.keys().cloned().collect::<Vec<_>>());
        let pending_ref = &pending;
        let outcome = poll::until(phase.budget(), |_| async move {
            let now = Self::sample(h).await?;
            let missing = self.pending(&now, phase);
            let complete = phase.reached(missing.len(), self.mirrors.len());
            pending_ref.replace(missing);
            Ok::<_, anyhow::Error>(complete.then_some(now))
        })
        .await?;
        let (now, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: mirrors without {phase:?} after {}s: {:?}",
                t.as_secs(),
                pending.borrow()
            )
        })?;
        crate::log(format!(
            "{ctx}: every mirror reaches {phase:?} after {}s ({self:?} -> {now:?})",
            elapsed.as_secs()
        ));
        Ok(now)
    }
}

#[cfg(test)]
mod tests;
