//! The assertions every case shares. Each one carries the rule that
//! made an earlier version pass vacuously: recovery needs evidence of
//! replacement, a failed scrape is never a zero, and a stall check
//! needs two real samples.

use std::cell::RefCell;
use std::time::Duration;

use crate::harness::Harness;
use crate::inject::Killed;
use crate::metrics;
use crate::poll::{self, Budget};
use crate::probes::EXECUTOR_BLOCK_METRIC;

/// Where an executor stands against the fleet head.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Standing {
    Unreachable(String),
    Lagging(String, i64),
    /// Inside the lag bound, but at or below the block it showed in the
    /// first sample that put the whole fleet inside the bound.
    Stalled(String, i64),
    Converged,
}

/// One sample of every executor's own block gauge, in fleet order. A
/// failed scrape stays `None`: it is evidence, not zero.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FleetSample {
    blocks: Vec<Option<i64>>,
}

impl FleetSample {
    fn head(&self) -> Option<i64> {
        self.blocks.iter().flatten().copied().max()
    }

    /// Every replica answers and is within `lag` blocks of the head.
    fn within(&self, lag: i64) -> bool {
        let Some(head) = self.head() else {
            return false;
        };
        self.blocks
            .iter()
            .all(|b| b.is_some_and(|b| head.saturating_sub(b) <= lag))
    }

    /// Each replica's standing. With a `base`, a replica inside the lag
    /// bound is `Converged` only when its block is past its block in
    /// `base`: a replica that stopped inside the bound reads as
    /// `Stalled`, not as recovered.
    fn standings(&self, nodes: &[String], lag: i64, base: Option<&Self>) -> Vec<Standing> {
        let head = self.head();
        let floors = base.map(|b| b.blocks.as_slice()).unwrap_or_default();
        self.blocks
            .iter()
            .zip(nodes)
            .enumerate()
            .map(|(i, (block, node))| {
                let floor = floors.get(i).copied().flatten();
                Standing::of(node, *block, head, lag, floor)
            })
            .collect()
    }
}

impl Standing {
    fn of(node: &str, block: Option<i64>, head: Option<i64>, lag: i64, floor: Option<i64>) -> Self {
        let (Some(block), Some(head)) = (block, head) else {
            return Self::Unreachable(node.to_string());
        };
        if head.saturating_sub(block) > lag {
            return Self::Lagging(node.to_string(), head.saturating_sub(block));
        }
        match floor {
            Some(floor) if block <= floor => Self::Stalled(node.to_string(), block),
            _ => Self::Converged,
        }
    }
}

impl Harness {
    /// Wait until `job` has at least `min` running allocations, plus
    /// evidence that the last kill was recovered from: a stopped
    /// allocation is replaced by a different running allocation, or a
    /// hard-killed task runs again under a new container id.
    ///
    /// # Errors
    ///
    /// Returns an error after `slo`.
    pub async fn assert_count(
        &mut self,
        job: &str,
        min: usize,
        slo: Duration,
    ) -> anyhow::Result<()> {
        let killed = self.killed.take();
        let (this, killed) = (&*self, killed.as_ref());
        let outcome = poll::until(Budget::new(slo, Duration::from_secs(3)), |_| async move {
            this.count_recovered(job, min, killed).await
        })
        .await?;
        let (detail, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{job} did not reach >= {min} running (with the killed alloc/task replaced) within {}s",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "{job} has >= {min} running alloc(s) after {}s{detail}",
            elapsed.as_secs()
        ));
        Ok(())
    }

    /// One `assert_count` observation: the replacement evidence line when
    /// the job has recovered, else `None`.
    async fn count_recovered(
        &self,
        job: &str,
        min: usize,
        killed: Option<&Killed>,
    ) -> anyhow::Result<Option<String>> {
        let running = self.nomad.running(job).await?;
        if running.len() < min {
            return Ok(None);
        }
        Ok(match killed {
            None => Some(String::new()),
            Some(Killed::Alloc(old)) => (running.iter().filter(|a| a.id != *old).count() >= min)
                .then(|| format!(" (stopped alloc {} replaced)", &old[..8.min(old.len())])),
            Some(Killed::Inner { node, task, cid }) => self
                .nodes
                .inner_cid(node, task)
                .await
                .filter(|now| now != cid)
                .map(|now| format!(" ({task} restarted on {node}: {cid} -> {now})")),
        })
    }

    /// The pipeline-progress probe of the component cases: the sealer
    /// boundary counter must tick over a ten-second window. When no
    /// executor exporter answers, the executor block gauge stands in.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline does not progress.
    pub async fn assert_progress(&self) -> anyhow::Result<()> {
        let Some(b0) = self.probes.sealer_boundaries().await else {
            return self.assert_executor_progress(Duration::from_secs(60)).await;
        };
        tokio::time::sleep(Duration::from_secs(10)).await;
        let b1 = self.probes.sealer_boundaries().await.unwrap_or(0);
        anyhow::ensure!(
            b1 > b0,
            "{}: pipeline NOT progressing after recovery (sealer boundaries {b0} -> {b1})",
            crate::FAIL_PREFIX
        );
        crate::log(format!(
            "pipeline progressing (sealer boundaries {b0} -> {b1})"
        ));
        Ok(())
    }

    /// The cluster-mode progress probe: the executor block gauge must
    /// advance within `timeout`. Recovery is not instant after a leader
    /// kill, so this succeeds as soon as progress shows.
    ///
    /// # Errors
    ///
    /// Returns an error if the gauge never advances.
    pub async fn assert_executor_progress(&self, timeout: Duration) -> anyhow::Result<()> {
        let e0 = self
            .progress_sample()
            .await
            .ok_or_else(|| crate::chaos_fail!("no executor baseline before progress window"))?;
        let outcome = poll::after_sleep(
            Budget::new(timeout, Duration::from_secs(5)),
            |_| async move {
                let e1 = self.probes.executor_progress().await.unwrap_or(0);
                Ok((e1 > e0).then_some(e1))
            },
        )
        .await?;
        let (e1, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "pipeline NOT progressing (executor block {e0} -> ? over {}s)",
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "pipeline progressing (executor block {e0} -> {e1} after {}s)",
            elapsed.as_secs()
        ));
        Ok(())
    }

    /// The per-replica recovery verdict every case ends with: within the
    /// convergence SLO, every executor's own gauge must be scrapeable,
    /// within the lag bound of the fleet head, and past the block it
    /// showed when the fleet first came inside the bound. The
    /// fleet-maximum probes hide a replica that never recovered; this
    /// does not. One sample inside the bound is not enough: a replica
    /// that stalled less than the lag bound ago still reads as close to
    /// the head, and the sealer stamps a block on every tick with or
    /// without load, so a live replica always advances.
    ///
    /// # Errors
    ///
    /// Returns an error naming the unreachable, lagging or stalled
    /// replicas.
    pub async fn assert_executors_converged(&self, case: &str) -> anyhow::Result<()> {
        let budget = Budget::new(self.knobs.converge_slo, Duration::from_secs(5));
        let lag = i64::try_from(self.knobs.converge_lag).unwrap_or(i64::MAX);
        let nodes: Vec<String> = self
            .probes
            .executors
            .iter()
            .map(|n| n.container.clone())
            .collect();
        let nodes = &nodes;
        let last = RefCell::new(String::new());
        let last_ref = &last;
        let base: RefCell<Option<FleetSample>> = RefCell::new(None);
        let base_ref = &base;
        let outcome = poll::until(budget, |_| async move {
            let sample = self.fleet_sample().await;
            let standings = sample.standings(nodes, lag, base_ref.borrow().as_ref());
            *last_ref.borrow_mut() = describe(sample.head(), &standings);
            if base_ref.borrow().is_none() {
                // The first sample inside the bound is the floor the next
                // samples must pass; it is never the verdict itself.
                base_ref.replace(sample.within(lag).then_some(sample));
                return Ok(None);
            }
            Ok(standings
                .iter()
                .all(|s| *s == Standing::Converged)
                .then_some(sample.head()))
        })
        .await?;
        let (head, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{case}: executor fleet NOT fully recovered within {}s ({})",
                t.as_secs(),
                last.borrow()
            )
        })?;
        crate::log(format!(
            "{case}: all {} executors converged and advancing (head {}, per-replica lag <= {}) after {}s",
            self.probes.executors.len(),
            head.unwrap_or(0),
            self.knobs.converge_lag,
            elapsed.as_secs()
        ));
        Ok(())
    }

    async fn fleet_sample(&self) -> FleetSample {
        let mut blocks = Vec::new();
        for i in 0..self.probes.executors.len() {
            blocks.push(self.probes.exec_metric(i, EXECUTOR_BLOCK_METRIC).await);
        }
        FleetSample { blocks }
    }

    /// Both ingress exporters must answer: real liveness, not a Nomad
    /// row count.
    ///
    /// # Errors
    ///
    /// Returns an error if a replica's exporter stays dark for 120s.
    pub async fn assert_ingress_pair_live(&self, case: &str) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::secs(120, 5), |_| async move {
            Ok(self.dark_ingress().await.is_none().then_some(()))
        })
        .await?;
        outcome.or_fail(|t| {
            crate::chaos_fail!(
                "{case}: an ingress replica exporter is dark after {}s (pair not fully recovered)",
                t.as_secs()
            )
        })?;
        crate::log(format!("{case}: both ingress exporters live"));
        Ok(())
    }

    /// The first ingress replica whose exporter does not answer.
    async fn dark_ingress(&self) -> Option<String> {
        for node in &self.probes.ingresses {
            if !self
                .probes
                .scrape()
                .answers(&self.probes.ingress_target(node))
                .await
            {
                return Some(node.container.clone());
            }
        }
        None
    }

    /// The must-not-progress check: the executor block gauge stays flat
    /// over `window`. Both samples must be real scrapes; an unreachable
    /// gauge is a harness failure, because the executors survive a
    /// sealer quorum loss.
    ///
    /// # Errors
    ///
    /// Returns an error if a sample is missing or the gauge advanced.
    pub async fn assert_executor_stalled(&self, window: Duration) -> anyhow::Result<()> {
        let e0 = self.progress_sample().await.ok_or_else(|| {
            crate::chaos_fail!("stall assert: no executor gauge scrapeable BEFORE the window — cannot observe the stall")
        })?;
        tokio::time::sleep(window).await;
        let e1 = self.progress_sample().await.ok_or_else(|| {
            crate::chaos_fail!("stall assert: no executor gauge scrapeable AFTER the window — cannot observe the stall")
        })?;
        anyhow::ensure!(
            e1 == e0,
            "{}: pipeline UNEXPECTEDLY progressed while quorum lost (executor block {e0} -> {e1})",
            crate::FAIL_PREFIX
        );
        crate::log(format!(
            "pipeline correctly STALLED (executor block {e0} -> {e1} over {}s, no false progress)",
            window.as_secs()
        ));
        Ok(())
    }

    /// One executor-progress reading, retried five times.
    async fn progress_sample(&self) -> Option<i64> {
        let outcome = poll::until(Budget::secs(12, 3), |_| async move {
            Ok(self.probes.executor_progress().await)
        })
        .await
        .ok()?;
        outcome
            .or_fail(|_| anyhow::anyhow!("no sample"))
            .ok()
            .map(|(v, _)| v)
    }

    /// The restarted-replica health probe: the sequencer exporter at
    /// `target` answers with sequencer samples. Established-sender
    /// coverage stays on the twin, by design.
    ///
    /// # Errors
    ///
    /// Returns an error if the exporter is dark for the whole SLO.
    pub async fn assert_replica_healthy(
        &self,
        target: &metrics::Target,
        slo: Duration,
    ) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::new(slo, Duration::from_secs(5)), |_| async move {
            let body = self.probes.scrape().fetch(target).await;
            let samples = body.map(|b| metrics::count_with_prefix(&b, "kardamom_sequencer_"));
            Ok(samples.filter(|n| *n > 0))
        })
        .await?;
        let (samples, _) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "restarted replica on {} (:{}) never came up: metrics unscrapable within {}s of restart",
                target.node,
                target.port,
                t.as_secs()
            )
        })?;
        crate::log(format!(
            "restarted replica on {} (:{}) is up and exporting ({samples} sequencer metrics; established-sender coverage stays on the twin)",
            target.node, target.port
        ));
        Ok(())
    }
}

fn describe(head: Option<i64>, standings: &[Standing]) -> String {
    let bad: Vec<String> = standings
        .iter()
        .filter_map(|s| match s {
            Standing::Unreachable(n) => Some(format!("{n}=unreachable")),
            Standing::Lagging(n, lag) => Some(format!("{n}=lag:{lag}")),
            Standing::Stalled(n, block) => Some(format!("{n}=stalled-at:{block}")),
            Standing::Converged => None,
        })
        .collect();
    format!(
        "head={};{}",
        head.map_or("?".to_string(), |h| h.to_string()),
        bad.join(" ")
    )
}

#[cfg(test)]
mod tests;
