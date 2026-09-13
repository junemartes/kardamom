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
    Converged,
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
        let e0 = self.probes.executor_progress().await.unwrap_or(0);
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
    /// convergence SLO, every executor's own gauge must be scrapeable
    /// and within the lag bound of the fleet head. The fleet-maximum
    /// probes hide a replica that never recovered; this does not.
    ///
    /// # Errors
    ///
    /// Returns an error naming the unreachable or lagging replicas.
    pub async fn assert_executors_converged(&self, case: &str) -> anyhow::Result<()> {
        let budget = Budget::new(self.knobs.converge_slo, Duration::from_secs(5));
        let last = RefCell::new(String::new());
        let last_ref = &last;
        let outcome = poll::until(budget, |_| async move {
            let (head, standings) = self.fleet_standings().await;
            *last_ref.borrow_mut() = describe(head, &standings);
            Ok(standings
                .iter()
                .all(|s| *s == Standing::Converged)
                .then_some(head))
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
            "{case}: all {} executors converged (head {}, per-replica lag <= {}) after {}s",
            self.probes.executors.len(),
            head.unwrap_or(0),
            self.knobs.converge_lag,
            elapsed.as_secs()
        ));
        Ok(())
    }

    async fn fleet_standings(&self) -> (Option<i64>, Vec<Standing>) {
        let mut blocks = Vec::new();
        for i in 0..self.probes.executors.len() {
            blocks.push(self.probes.exec_metric(i, EXECUTOR_BLOCK_METRIC).await);
        }
        let head = blocks.iter().flatten().copied().max();
        let lag = i64::try_from(self.knobs.converge_lag).unwrap_or(i64::MAX);
        let standings = blocks
            .iter()
            .zip(&self.probes.executors)
            .map(|(block, node)| match (block, head) {
                (None, _) | (_, None) => Standing::Unreachable(node.container.clone()),
                (Some(b), Some(h)) if h.saturating_sub(*b) > lag => {
                    Standing::Lagging(node.container.clone(), h.saturating_sub(*b))
                }
                _ => Standing::Converged,
            })
            .collect();
        (head, standings)
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
            e1 <= e0,
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
            Standing::Converged => None,
        })
        .collect();
    format!(
        "head={};{}",
        head.map_or("?".to_string(), |h| h.to_string()),
        bad.join(" ")
    )
}
