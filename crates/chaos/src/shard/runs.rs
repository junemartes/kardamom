//! Cases that need another replay budget start on a separate fresh chain.

use super::{DeployVars, Shard};

const DA_GUARD: u64 = 8;

/// One fresh deployment and its ordered cases within an existing CI shard.
#[derive(Debug)]
pub struct ShardRun {
    pub shard: Shard,
    pub vars: DeployVars,
    pub cases: Vec<String>,
}

impl ShardRun {
    fn new(shard: Shard, cases: &[String], fresh: bool) -> Self {
        let mut vars = shard.deploy_vars();
        if fresh && cases.first().is_some_and(|case| Self::needs_guard(case)) {
            vars.da_lag_budget_blocks = Some(DA_GUARD);
        }
        Self {
            shard,
            vars,
            cases: cases.to_vec(),
        }
    }

    fn needs_guard(case: &str) -> bool {
        matches!(case, "da-lag-halt" | "canary-da-lag")
    }
}

impl Shard {
    /// Keep case order, with a fresh chain between distinct DA guard settings.
    /// Reuse keeps the existing chain and requires its configured guard.
    #[must_use]
    pub fn runs(self, cases: &[String], reuse: bool) -> Vec<ShardRun> {
        if reuse || cases.is_empty() {
            return vec![ShardRun::new(self, cases, false)];
        }
        cases
            .chunk_by(|left, right| ShardRun::needs_guard(left) == ShardRun::needs_guard(right))
            .map(|cases| ShardRun::new(self, cases, true))
            .collect()
    }
}

#[cfg(test)]
mod tests;
