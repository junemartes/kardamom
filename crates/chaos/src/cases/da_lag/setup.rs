//! A halt case observes the replicated guard without changing it.

use std::num::NonZeroU64;

use anyhow::Context;

use super::da_lag_halt;
use crate::harness::Harness;
use crate::metrics;
use crate::poll::{self, Budget};
use crate::probes::Probed;

const BUDGET_METRIC: &str = "kardamom_ingress_cluster_da_lag_budget_blocks";

#[derive(Clone, Copy)]
pub(crate) enum GuardedCase {
    Halt,
    Canary,
}

impl GuardedCase {
    pub(crate) async fn run(self, h: &mut Harness) -> anyhow::Result<()> {
        let budget = h.knobs.da_lag_budget_blocks.context(
            "DA halt cases require a chain born with a fixed DA guard; use the fresh guard phase",
        )?;
        GuardReadings::await_budget(h, budget).await?;
        match self {
            Self::Halt => da_lag_halt(h).await,
            Self::Canary => super::super::l1_canary::canary_da_lag(h).await,
        }
    }
}

struct GuardReadings(Vec<Option<NonZeroU64>>);

impl GuardReadings {
    fn parse(body: &str) -> Option<NonZeroU64> {
        metrics::first(body, BUDGET_METRIC)
            .and_then(|value| u64::try_from(value).ok())
            .and_then(NonZeroU64::new)
    }

    async fn read(h: &Harness, node: &Probed) -> Option<NonZeroU64> {
        let body = h
            .probes
            .scrape()
            .fetch(&h.probes.ingress_target(node))
            .await?;
        Self::parse(&body)
    }

    async fn sample(h: &Harness) -> Self {
        let mut readings = Vec::with_capacity(h.probes.ingresses.len());
        for node in &h.probes.ingresses {
            readings.push(Self::read(h, node).await);
        }
        Self(readings)
    }

    fn matches(&self, expected: NonZeroU64) -> bool {
        !self.0.is_empty() && self.0.iter().all(|value| *value == Some(expected))
    }

    async fn await_budget(h: &Harness, expected: NonZeroU64) -> anyhow::Result<()> {
        poll::until(Budget::secs(120, 2), |_| async move {
            Ok::<_, anyhow::Error>(Self::sample(h).await.matches(expected).then_some(()))
        })
        .await?
        .or_fail(|_| {
            crate::chaos_fail!(
                "DA halt: every ingress must observe the fixed genesis guard {expected}"
            )
        })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
