//! Nonce sources fail independently of the cold replica's cluster feed.

use std::cell::RefCell;

use anyhow::Context;
use serde::Serialize;

use crate::harness::Harness;
use crate::metrics;
use crate::poll::{self, Budget};

mod rules;
use rules::Rules;

const CTX: &str = "lookup-blackout";
/// The sequencer's count of nonce lookups by outcome.
pub(super) const LOOKUPS: &str = "kardamom_sequencer_nonce_lookups_total";
const BOUNDARY: &str = "kardamom_sequencer_last_boundary_unix_seconds";
/// The lookup outcomes with an answer: from an executor, from Redis, or
/// from committed account updates in the local live layer.
pub(super) const ANSWERED: [&str; 4] = [
    "outcome=\"ok\"",
    "outcome=\"redis\"",
    "outcome=\"local\"",
    "outcome=\"live\"",
];

#[derive(Clone, Copy, Debug, Serialize)]
enum Phase {
    Unavailable,
    Answered,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Counts {
    failed: i64,
    answered: i64,
    last_boundary: i64,
}

impl Counts {
    fn read(body: &str) -> Self {
        Self {
            failed: Self::outcomes(body, &["outcome=\"error\"", "outcome=\"timeout\""]),
            answered: Self::outcomes(body, &ANSWERED),
            last_boundary: metrics::first(body, BOUNDARY).unwrap_or(0),
        }
    }

    fn outcomes(body: &str, outcomes: &[&str]) -> i64 {
        outcomes
            .iter()
            .filter_map(|outcome| metrics::sum_where(body, LOOKUPS, outcome))
            .fold(0, i64::saturating_add)
    }

    fn proves(self, phase: Phase) -> bool {
        let lookup = match phase {
            Phase::Unavailable => self.failed > 0 && self.answered == 0,
            Phase::Answered => self.answered > 0,
        };
        lookup && self.last_boundary > 0
    }
}

pub(crate) struct LookupBlackout {
    node: String,
    rules: Rules,
    phases: Vec<serde_json::Value>,
}

impl LookupBlackout {
    pub(crate) async fn run(h: &mut Harness) -> anyhow::Result<()> {
        let mut fault = Self {
            node: h.probes.sequencers[0].container.clone(),
            rules: Rules::new(
                h.probes
                    .executors
                    .iter()
                    .map(|executor| executor.ip)
                    .collect(),
            ),
            phases: Vec::new(),
        };
        let outcome = fault.exercise(h).await;
        let cleared = fault.clear(h).await;
        let report = serde_json::json!({
            "case": CTX, "node": fault.node, "phases": fault.phases,
            "fault": "Receipt DATA stream1002, executor query TCP9024 and cache TCP6379/26379; cluster UDP remains reachable",
            "failure": outcome.as_ref().err().map(|e| format!("{e:#}")),
            "cleanup_failure": cleared.as_ref().err().map(|e| format!("{e:#}")),
        });
        std::fs::write(
            "/tmp/chaos-lookup-blackout-fault.json",
            serde_json::to_vec_pretty(&report)?,
        )?;
        outcome?;
        cleared?;
        h.assert_progress().await
    }

    async fn exercise(&mut self, h: &mut Harness) -> anyhow::Result<()> {
        h.nodes.exec(&self.node, &self.rules.arm()).await?;
        self.phase(h, Phase::Unavailable).await?;
        h.nodes.exec(&self.node, &Rules::allow_queries()).await?;
        self.phase(h, Phase::Answered).await
    }

    async fn phase(&mut self, h: &mut Harness, phase: Phase) -> anyhow::Result<()> {
        crate::log(format!(
            "{CTX}: {phase:?}; receipt data is blocked while cluster UDP stays reachable"
        ));
        let process = self.restart(h, phase).await?;
        let last = RefCell::new(None);
        let last_drops = RefCell::new(None);
        let result = poll::until(Budget::secs(120, 3), |_| async {
            let Some(body) = h
                .probes
                .scrape()
                .fetch(&h.probes.sequencer_lane0_target(0))
                .await
            else {
                return Ok(None);
            };
            let counts = Counts::read(&body);
            last.replace(Some(counts));
            if !counts.proves(phase) {
                return Ok(None);
            }
            let drops = h.nodes.exec(&self.node, &Rules::counters()).await?;
            let fault_active = Rules::require_drops(&drops).is_ok();
            last_drops.replace(Some(drops));
            Ok(fault_active.then_some(counts))
        })
        .await?
        .or_fail(|_| {
            anyhow::anyhow!(
                "{CTX}: {phase:?} lookup and a live canonical boundary not proved within120s: {:?}",
                last.borrow()
            )
        });

        self.phases.push(serde_json::json!({
            "phase": phase, "container": process.cid, "started": process.started,
            "last_counts": *last.borrow(), "drop_rules": *last_drops.borrow(),
            "failure": result.as_ref().err().map(|e| format!("{e:#}")),
        }));
        let (counts, elapsed) = result?;
        anyhow::ensure!(
            h.nodes
                .running_process(&self.node, "sequencer-0")
                .await
                .as_ref()
                == Some(&process),
            "{CTX}: the replacement process changed during the lookup proof"
        );
        crate::log(format!(
            "{CTX}: {phase:?} proved in {}s: {counts:?}",
            elapsed.as_secs()
        ));
        Ok(())
    }

    async fn restart(
        &self,
        h: &mut Harness,
        phase: Phase,
    ) -> anyhow::Result<crate::nodes::RunningProcess> {
        let old = h
            .nodes
            .running_process(&self.node, "sequencer-0")
            .await
            .context("lookup fault has no live lane-0 victim")?;
        h.inject_hard(&[&self.node], "sequencer-0").await?;
        if matches!(phase, Phase::Unavailable) {
            h.assert_progress().await?;
        }
        h.assert_count("sequencer", 4, h.knobs.restart_slo).await?;
        let process = h
            .nodes
            .running_process(&self.node, "sequencer-0")
            .await
            .context("lookup fault has no replacement lane-0 process")?;
        anyhow::ensure!(process != old, "{CTX}: the lane-0 victim did not restart");
        Ok(process)
    }

    async fn clear(&self, h: &Harness) -> anyhow::Result<()> {
        let input_script = Rules::clear_input();
        let output_script = Rules::clear_output();
        let (input, output) = tokio::join!(
            h.nodes.exec(&self.node, &input_script),
            h.nodes.exec(&self.node, &output_script),
        );
        input.and(output).map(|_| ())
    }
}

#[cfg(test)]
mod tests;
