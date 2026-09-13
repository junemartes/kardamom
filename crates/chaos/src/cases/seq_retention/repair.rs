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

    async fn counts(&self, h: &Harness) -> anyhow::Result<[usize; 3]> {
        let allocs = h.nomad.allocations(self.job).await?;
        let mut logs = String::new();
        for alloc in allocs.iter().filter(|a| a.node_name == self.node) {
            logs.push_str(&h.nomad.alloc_logs(alloc, Streams::Both).await?);
            logs.push('\n');
        }
        Ok(self
            .needles
            .map(|needle| logs.lines().filter(|line| line.contains(needle)).count()))
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
}
