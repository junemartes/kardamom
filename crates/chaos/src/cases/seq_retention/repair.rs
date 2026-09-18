//! Recovery evidence belongs to the victim and must advance after injection.

use crate::harness::Harness;
use crate::nomad::Streams;

use super::Victim;

pub(super) struct Repair {
    node: String,
    job: &'static str,
    needles: [&'static str; 3],
    baseline: [usize; 3],
}

impl Repair {
    pub(super) async fn capture(
        h: &Harness,
        victim: Victim,
        container: &str,
    ) -> anyhow::Result<Self> {
        let node = h
            .contract
            .nodes
            .values()
            .find(|n| n.container == container)
            .ok_or_else(|| anyhow::anyhow!("unknown repair victim {container}"))?
            .name
            .clone();
        let mut repair = Self {
            node,
            job: victim.kind(),
            needles: [
                "cluster replay unavailable",
                "resync prepared: peer checkpoint staged",
                victim.restored_needle(),
            ],
            baseline: [0; 3],
        };
        repair.baseline = repair.counts(h).await?;
        Ok(repair)
    }

    /// Every allocation log of the victim's job on the victim's node,
    /// joined in allocation order.
    async fn logs(&self, h: &Harness) -> anyhow::Result<String> {
        let allocs = h.nomad.allocations_with_logs(self.job).await?;
        let mut logs = String::new();
        for alloc in allocs.iter().filter(|a| a.node_name == self.node) {
            logs.push_str(&h.nomad.alloc_logs(alloc, Streams::Both).await?);
            logs.push('\n');
        }
        Ok(logs)
    }

    async fn counts(&self, h: &Harness) -> anyhow::Result<[usize; 3]> {
        let logs = self.logs(h).await?;
        Ok(self
            .needles
            .map(|needle| logs.lines().filter(|line| line.contains(needle)).count()))
    }

    /// Whether the last repair in the victim log ran in-process. See
    /// [`Self::ran_in_process`].
    pub(super) async fn in_process(&self, h: &Harness) -> anyhow::Result<Result<(), String>> {
        Ok(self.ran_in_process(&self.logs(h).await?))
    }

    /// After the last `resync prepared` line, the consumer logs its
    /// revolution line and then the restore needle, with no
    /// `kardamom-<kind> starting` line in between. A start line there
    /// means the process exited and the orchestrator's restart did the
    /// restore.
    fn ran_in_process(&self, logs: &str) -> Result<(), String> {
        let [_, prepared, restored] = self.needles;
        let tail = &logs[logs.rfind(prepared).ok_or("no 'resync prepared' line")?..];
        let restored_at = tail
            .find(restored)
            .ok_or_else(|| format!("no '{restored}' line after the last 'resync prepared'"))?;
        let between = &tail[..restored_at];
        let start = format!("kardamom-{} starting", self.job);
        if between.contains(&start) {
            return Err(format!(
                "'{start}' logged between 'resync prepared' and '{restored}'"
            ));
        }
        if !between.contains("the pipeline starts again in-process") {
            return Err("no 'the pipeline starts again in-process' line before the restore".into());
        }
        Ok(())
    }

    pub(super) async fn seen(&self, h: &Harness) -> anyhow::Result<(bool, bool, bool)> {
        Ok(self.advanced(self.counts(h).await?))
    }

    fn advanced(&self, counts: [usize; 3]) -> (bool, bool, bool) {
        (
            counts[0] > self.baseline[0],
            counts[1] > self.baseline[1],
            counts[2] > self.baseline[2],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_or_partial_repairs_cannot_satisfy_a_new_injection() {
        let repair = Repair {
            node: "victim".into(),
            job: "executor",
            needles: ["a", "b", "c"],
            baseline: [1, 2, 3],
        };
        assert_eq!(repair.advanced([1, 2, 3]), (false, false, false));
        assert_eq!(repair.advanced([2, 2, 4]), (true, false, true));
        assert_eq!(repair.advanced([2, 3, 4]), (true, true, true));
    }

    #[test]
    fn a_start_line_inside_the_repair_means_the_orchestrator_restored() {
        let repair = Repair {
            node: "victim".into(),
            job: "executor",
            needles: ["replay unavailable", "resync prepared", "restored"],
            baseline: [0; 3],
        };
        let in_process = "resync prepared\nthe pipeline starts again in-process\nrestored\n";
        assert_eq!(repair.ran_in_process(in_process), Ok(()));
        let restarted = "resync prepared\nkardamom-executor starting\nrestored\n";
        assert!(repair.ran_in_process(restarted).unwrap_err().contains("starting"));
        let older = "resync prepared\nrestored\nresync prepared\n";
        assert!(repair.ran_in_process(older).unwrap_err().contains("no 'restored'"));
    }
}
