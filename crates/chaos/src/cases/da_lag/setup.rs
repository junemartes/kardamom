//! The DA guard stays fixed from genesis through every snapshot replay.

use anyhow::Context;

use super::{await_halt, da_lag_halt, status};
use crate::harness::Harness;
use crate::metrics;
use crate::poll::{self, Budget};

const BUDGET_METRIC: &str = "kardamom_ingress_cluster_da_lag_budget_blocks";

#[derive(Clone, Copy)]
pub(crate) enum GuardedCase {
    Halt,
    Canary,
}

impl GuardedCase {
    pub(crate) async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        let budget = h.knobs.da_lag_budget_blocks.context(
            "DA-halt cases require a fixed KARDAMOM_DA_LAG_BUDGET_BLOCKS from genesis; changing a live replay budget changes canonical decisions",
        )?;
        let expected = i64::try_from(budget.get())?;
        Self::await_ready(h, expected).await?;
        await_halt(h, "da-lag-halt: guard setup", expected, false).await?;
        match self {
            Self::Halt => da_lag_halt(h).await,
            Self::Canary => super::super::l1_canary::canary_da_lag(h).await,
        }
    }

    async fn await_ready(h: &Harness, expected: i64) -> anyhow::Result<()> {
        poll::until(Budget::secs(120, 2), |_| async move {
            let target = h.probes.ingress_target(&h.probes.ingresses[0]);
            let observed = h
                .probes
                .scrape()
                .fetch(&target)
                .await
                .and_then(|body| metrics::first(&body, BUDGET_METRIC));
            let caught_up = status(h).await.is_some_and(|sample| {
                !sample.halted && sample.sealed.saturating_sub(sample.posted) <= 4
            });
            Ok((observed == Some(expected) && caught_up).then_some(()))
        })
        .await?
        .or_fail(|_| {
            crate::chaos_fail!(
                "da-lag-halt: fixed DA guard {expected} is not active with posting caught up"
            )
        })?;
        Ok(())
    }
}
