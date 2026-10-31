//! The cases of one shard on one chain, with the guard of its deploy.

use kardamom_chaos::{DeployVars, Harness, Knobs, Lifecycle, Shard};

use super::run_audited;

/// The cases of one shard on one chain. A shard that deploys a DA guard
/// deploys it from genesis, for every case: the guard is replicated, so
/// it never changes on a live chain.
pub(super) struct Phase {
    shard: Shard,
    cases: Vec<String>,
    vars: DeployVars,
    knobs: Knobs,
    reuse: bool,
}

impl Phase {
    pub(super) fn read(shard: Shard, cases: &[String], reuse: bool) -> anyhow::Result<Self> {
        anyhow::ensure!(!cases.is_empty(), "a chaos phase needs at least one case");
        let vars = shard.deploy_vars();
        let knobs = Knobs::deployed(shard.env(), &vars)?;
        Ok(Self {
            shard,
            cases: cases.to_vec(),
            vars,
            knobs,
            reuse,
        })
    }

    pub(super) async fn run(&self) -> anyhow::Result<()> {
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
            "chaos suite: shard={} da_budget={:?} cases=[{}] tps={} case_deadline_s={}",
            self.shard.name(),
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
        if self.vars.da_lag_budget_blocks.is_some() {
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
