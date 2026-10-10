//! The wait for the executor tier after a whole-fleet return. The wait
//! passes when an executor reports the block gauge. The gauge appears
//! only after an executor finishes a block, so a live exporter alone
//! does not pass. A failure tells the two stalls apart: no exporter
//! answers, or the exporters answer and no executor finished a block.

use std::time::Duration;

use crate::harness::Harness;
use crate::metrics;
use crate::poll::{self, Budget, Outcome};
use crate::probes::{EXECUTOR_BLOCK_METRIC, Probes, SEALER_BOUNDARIES_METRIC};

/// The name fragment of the gauges that count the entries an executor
/// waits on for a void decision.
const VOID_FRAGMENT: &str = "void";

/// One read of one executor exporter.
#[derive(Debug)]
pub(super) struct Reading {
    pub(super) container: String,
    /// The `/metrics` body, or `None` when the exporter does not answer.
    pub(super) body: Option<String>,
}

impl Reading {
    /// The block gauge, or `None` before the first finished block.
    fn block(&self) -> Option<i64> {
        metrics::first(self.body.as_deref()?, EXECUTOR_BLOCK_METRIC)
    }

    /// One clause of the failure text for this executor.
    fn describe(&self) -> String {
        let Some(body) = &self.body else {
            return format!("{} does not answer", self.container);
        };
        let boundaries = metrics::first(body, SEALER_BOUNDARIES_METRIC)
            .map_or_else(|| "none".to_string(), |v| v.to_string());
        let void = metrics::samples_named_with(body, VOID_FRAGMENT);
        let waits = if void.is_empty() {
            String::new()
        } else {
            format!(
                "; it waits for a void decision on a lost entry ({})",
                void.join(" | ")
            )
        };
        format!(
            "{} answers with no block gauge (sealer boundaries {boundaries}){waits}",
            self.container
        )
    }
}

/// One read of every executor exporter.
#[derive(Debug)]
pub(super) struct ExecutorScan(pub(super) Vec<Reading>);

impl ExecutorScan {
    async fn read(probes: &Probes) -> Self {
        let mut readings = Vec::with_capacity(probes.executors.len());
        for (i, node) in probes.executors.iter().enumerate() {
            readings.push(Reading {
                container: node.container.clone(),
                body: probes.exec_metrics(i).await,
            });
        }
        Self(readings)
    }

    /// The highest block gauge of the fleet, or `None` while no
    /// executor finished a block.
    fn block(&self) -> Option<i64> {
        self.0.iter().filter_map(Reading::block).max()
    }

    /// The failure of a wait that ran out after `waited`.
    pub(super) fn stall(&self, ctx: &str, waited: Duration) -> anyhow::Error {
        let secs = waited.as_secs();
        if self.0.iter().all(|r| r.body.is_none()) {
            return crate::chaos_fail!(
                "{ctx}: no executor exporter answers {secs}s after the fleet returned"
            );
        }
        if self.block().is_some() {
            return crate::chaos_fail!(
                "{ctx}: the first executor block appeared only after the {secs}s budget"
            );
        }
        let executors: Vec<String> = self.0.iter().map(Reading::describe).collect();
        crate::chaos_fail!(
            "{ctx}: executor exporters answer {secs}s after the fleet returned, but no executor finished a block: {}",
            executors.join("; ")
        )
    }
}

/// Wait until an executor reports a finished block again. A returned
/// node's allocation runs before its exporter binds, and the executor
/// publishes the block gauge only after its first block. The progress
/// check needs a baseline from that gauge.
pub(crate) async fn await_exporter_back(h: &Harness, ctx: &str) -> anyhow::Result<()> {
    let budget = Budget::new(h.knobs.reschedule_slo, Duration::from_secs(3));
    let outcome = poll::until(budget, |_| async move {
        Ok(ExecutorScan::read(&h.probes).await.block())
    })
    .await?;
    let elapsed = match outcome {
        Outcome::Ready { elapsed, .. } => elapsed,
        Outcome::TimedOut { elapsed } => {
            return Err(ExecutorScan::read(&h.probes).await.stall(ctx, elapsed));
        }
    };
    crate::log(format!(
        "{ctx}: an executor exporter answers with a block gauge again after {}s",
        elapsed.as_secs()
    ));
    Ok(())
}

#[cfg(test)]
#[path = "exporter_back_tests.rs"]
mod tests;
