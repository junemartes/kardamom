//! `sequencer-lapse` on the local stack: SIGSTOP freezes one replica of
//! the case lane past the client liveness of the media driver, while its
//! twin keeps the lane ordering. On SIGCONT the replica finds that the
//! driver has timed its client out, and exits through the fail-fast
//! handler. The cold process that takes its place must publish fresh work.

use std::time::{Duration, Instant};

use alloy_primitives::Address;
use anyhow::{Context, Result};

use super::load::RunningLoad;
use super::{CASE_SENDER, RESTART_BOUND, Run};
use crate::harness::{LocalStack, StackConfig, l2, metrics};

/// The freeze. The local media driver times a client out after 10 s
/// without a keepalive; the deployed freeze crosses the same liveness.
pub const FREEZE: Duration = Duration::from_secs(14);
/// The publish counter of a replica.
const PUBLISHED: &str = "kardamom_sequencer_tx_published_to_b_total";
/// The start time of a replica process. Two observations with the same
/// value come from the same process.
const STARTED: &str = "kardamom_sequencer_start_time_seconds";

/// One reading of a replica's exporter.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Publication {
    started: f64,
    published: f64,
}

impl Run<'_> {
    /// The lane of the case sender.
    fn lapse_lane(&self) -> Result<u32> {
        let sender = l2::dev_signers_through(CASE_SENDER)?
            .get(CASE_SENDER)
            .map(|s| s.address)
            .context("case sender")?;
        Ok(self.stack.sequencer_for(sender))
    }

    async fn publication(&self, i: usize) -> Option<Publication> {
        let addr = self.stack.sequencer_metrics(u32::try_from(i).ok()?);
        let scrape = metrics::scrape(addr).await.ok()?;
        Some(Publication {
            started: scrape.value(STARTED)?,
            published: scrape.value(PUBLISHED)?,
        })
    }

    /// The replica publishes fresh work between two observations of one
    /// process.
    async fn assert_fresh_work(&self, i: usize) -> Result<Publication> {
        let first = metrics::poll_until(
            "a first publication reading of the returned replica",
            RESTART_BOUND,
            Duration::from_millis(200),
            async || Ok(self.publication(i).await),
        )
        .await?;
        metrics::poll_until(
            "fresh work from the returned replica",
            RESTART_BOUND,
            Duration::from_millis(200),
            async || {
                Ok(self.publication(i).await.filter(|now| {
                    now.started.to_bits() == first.started.to_bits()
                        && now.published > first.published
                }))
            },
        )
        .await
    }

    /// Freeze replica `victim` past the client liveness. The case load
    /// must land through the twin during the freeze.
    async fn freeze(&mut self, victim: usize, load: &RunningLoad) -> Result<()> {
        self.stack.freeze_sequencer(victim)?;
        let frozen = Instant::now();
        eprintln!("{}: sequencer {victim} frozen (SIGSTOP)", self.at());
        let before = load.landed();
        load.await_landed_above(before, FREEZE)
            .await
            .context("the twin does not keep the lane ordering during the freeze")?;
        tokio::time::sleep(FREEZE.saturating_sub(frozen.elapsed())).await;
        self.stack.thaw_sequencer(victim)?;
        eprintln!(
            "{}: sequencer {victim} thawed after {:.1}s",
            self.at(),
            frozen.elapsed().as_secs_f64()
        );
        Ok(())
    }

    /// The case: freeze, client-timeout exit, cold return, fresh work,
    /// then the common checks of every recovery case.
    ///
    /// # Errors
    /// Returns the first failed check.
    pub async fn sequencer_lapse(mut self) -> Result<()> {
        let lane = self.lapse_lane()?;
        let victim = usize::try_from(lane)?;
        let twin = self.stack.add_sequencer_twin(lane)?;
        let plan = self
            .plan(CASE_SENDER, 0, Address::repeat_byte(0xc4))
            .await?;
        let load = RunningLoad::start(plan);
        load.await_landed_above(2, Duration::from_secs(30))
            .await
            .context("the case load does not flow before the freeze")?;
        let warm = self
            .publication(victim)
            .await
            .context("the victim exports no publication")?;
        self.freeze(victim, &load).await?;
        let code = self
            .stack
            .await_sequencer_fail_fast(victim, RESTART_BOUND)?;
        eprintln!(
            "{}: sequencer {victim} exited through fail-fast with {code:?}",
            self.at()
        );
        self.stack.start_sequencer(victim)?;
        self.stack.await_sequencer_session(victim, RESTART_BOUND)?;
        let fresh = self.assert_fresh_work(victim).await?;
        anyhow::ensure!(
            fresh.started.to_bits() != warm.started.to_bits(),
            "{}: the replica that publishes is the frozen process",
            self.name
        );
        eprintln!(
            "{}: the cold replica publishes fresh work ({} offers); the twin is {twin}",
            self.at(),
            fresh.published
        );
        let report = load.finish().await?;
        eprintln!("{}: case load: {}", self.at(), report.summary());
        report.assert_all_landed(self.name)?;
        report.assert_on_chain(self.name, &self.clients()?).await?;
        self.assert_executor_progress().await?;
        self.probe(report.end_nonce()).await?;
        self.validator_verdict().await
    }
}

/// Launch a stack whose sequencers run the v2 loop, with a validator, and
/// run the lapse on it. A failure dumps the process logs.
///
/// # Panics
/// Panics when the stack does not start or the case fails.
pub async fn run_lapse() {
    let name = "sequencer-lapse";
    let started = Instant::now();
    let mut stack = LocalStack::launch(StackConfig {
        validator: true,
        admission: true,
        ..StackConfig::default()
    })
    .await
    .expect("stack");
    let outcome = match Run::new(&mut stack, name) {
        Ok(run) => run.sequencer_lapse().await,
        Err(e) => Err(e),
    };
    if let Err(e) = outcome {
        stack.dump_tails();
        panic!("{name}: {e:#}");
    }
    eprintln!(
        "PIPELINE_RECOVERY {name} seconds={:.1}",
        started.elapsed().as_secs_f64()
    );
}
