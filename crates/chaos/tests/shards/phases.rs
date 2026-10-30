//! Each replicated guard stays fixed for the lifetime of its chain.

use kardamom_chaos::{DeployVars, Harness, Knobs, Lifecycle, Shard};

use super::run_audited;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Guard {
    Standard,
    Short,
}

impl Guard {
    fn of(case: &str) -> Self {
        match case {
            "da-lag-halt" | "canary-da-lag" => Self::Short,
            _ => Self::Standard,
        }
    }

    fn vars(self, shard: Shard) -> DeployVars {
        let mut vars = shard.deploy_vars();
        if self == Self::Short {
            vars.da_lag_budget_blocks = Some(8);
        }
        vars
    }
}

pub(super) struct Phases(Vec<Phase>);

impl Phases {
    pub(super) fn read(shard: Shard, cases: &[String], reuse: bool) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !cases.is_empty(),
            "a chaos phase plan needs at least one case"
        );
        let phases = cases
            .chunk_by(|left, right| Guard::of(left) == Guard::of(right))
            .map(|group| Phase::read(shard, group, reuse))
            .collect::<anyhow::Result<Vec<_>>>()?;
        anyhow::ensure!(
            !reuse || phases.len() <= 1,
            "reuse cannot cross replicated DA guard settings; run each phase on a chain born with its fixed guard"
        );
        Ok(Self(phases))
    }

    pub(super) async fn run(self) -> anyhow::Result<()> {
        for phase in self.0 {
            phase.run().await?;
        }
        Ok(())
    }
}

struct Phase {
    shard: Shard,
    cases: Vec<String>,
    guard: Guard,
    vars: DeployVars,
    knobs: Knobs,
    reuse: bool,
}

impl Phase {
    fn read(shard: Shard, cases: &[String], reuse: bool) -> anyhow::Result<Self> {
        // A chunk is nonempty, so its first case selects its fixed guard.
        let guard = Guard::of(&cases[0]);
        let vars = guard.vars(shard);
        let knobs = Knobs::deployed(shard.env(), &vars)?;
        Ok(Self {
            shard,
            cases: cases.to_vec(),
            guard,
            vars,
            knobs,
            reuse,
        })
    }

    async fn run(&self) -> anyhow::Result<()> {
        let lifecycle = Lifecycle::in_workspace().with_vars(self.vars.clone());
        let contract = if self.reuse {
            lifecycle.contract()?
        } else {
            lifecycle.up().await?
        };
        let mut harness = Harness::new(contract, self.knobs.clone(), lifecycle.clone())?;
        harness.prepare_case_services().await?;
        self.await_startup(&harness).await?;
        harness.smoke_gate(harness.knobs.gate_account).await?;
        kardamom_chaos::log(format!(
            "chaos suite: shard={} fixed_guard={:?} da_budget={:?} cases=[{}] tps={} case_deadline_s={}",
            self.shard.name(),
            self.guard,
            self.knobs.da_lag_budget_blocks,
            self.cases.join(" "),
            harness.knobs.tps,
            harness.knobs.case_deadline.as_secs()
        ));
        for (index, case) in self.cases.iter().enumerate() {
            run_audited(&mut harness, case, self.audit(index, case)).await?;
        }
        self.finish(&mut harness).await?;
        if !self.reuse {
            lifecycle.down().await?;
        }
        Ok(())
    }

    fn audit(&self, index: usize, case: &str) -> bool {
        index
            .checked_add(1)
            .is_some_and(|next| next < self.cases.len())
            && self.shard.audits_after(case)
    }

    async fn await_startup(&self, harness: &Harness) -> anyhow::Result<()> {
        if self.guard == Guard::Short {
            kardamom_chaos::log("fixed DA guard: wait for the batcher to clear startup lag");
            harness.await_chain_recovered().await?;
        }
        Ok(())
    }

    async fn finish(&self, harness: &mut Harness) -> anyhow::Result<()> {
        kardamom_chaos::log(format!("chaos phase PASSED ({})", self.cases.join(" ")));
        harness.ingress_churn().await?;
        harness.validator_verdict().await?;
        harness.assert_persisted_state().await?;
        kardamom_chaos::log("cluster-e2e PASSED");
        Ok(())
    }
}

#[cfg(test)]
#[path = "phases_tests.rs"]
mod tests;
