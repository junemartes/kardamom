//! `sequencer-executor-redis-loss`: the sequencers, the executor and Redis
//! die, so no source of a sender floor is left. They return in reverse
//! order of need, the stagger apart: the sequencers, Redis (empty), then
//! the executor. The case lane runs two replicas, as deployed.
//!
//! - While the classes are down, the sealer keeps sealing empty blocks:
//!   the executor head after the return, less the most blocks the sealer
//!   can seal since the return, is past the head before the fault by at
//!   least one block for each second of the outage.
//! - Every returned replica of the case lane parks the case sender and
//!   looks its floor up. While no source can answer, no lookup answers and
//!   no transfer lands: no floor is guessed.
//! - After the executor returns, a lookup answers the floor on every
//!   replica of the case lane, every transfer lands once in nonce order,
//!   and no ref sits below a floor.
//! - No returned process exited: the sequencers, the executor and Redis
//!   all still run at the end.
//!
//! The replayed receipts of the executor would otherwise give the floor
//! first. So the stack runs the sequencers with the
//! `--first-floor-from-lookup` test seam: a receipt or a row sets no first
//! floor, and the first floor comes only from a lookup.

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::Result;

use super::cases::{Class, STAGGER};
use super::load::RunningLoad;
use super::{RESTART_BOUND, Run};
use crate::harness::faults::Stop;
use crate::harness::metrics;

/// The parks of a sequencer that asked for a floor.
const LOOKUP_REQUESTS: &str = "kardamom_sequencer_nonce_lookup_requests_total";
/// The lookups of a sequencer, by outcome.
const LOOKUPS: &str = "kardamom_sequencer_nonce_lookups_total";
/// The outcomes of a lookup that a source answered: an executor (`ok`),
/// Redis, or the local layer of either loop.
const ANSWERED: [&str; 4] = [
    "outcome=\"ok\"",
    "outcome=\"redis\"",
    "outcome=\"local\"",
    "outcome=\"live\"",
];

/// The floor lookups of one sequencer since its start.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Lookups {
    asked: f64,
    answered: f64,
}

impl Lookups {
    async fn of(addr: SocketAddr) -> Result<Self> {
        let scrape = metrics::scrape(addr).await?;
        Ok(Self {
            asked: scrape.value(LOOKUP_REQUESTS).unwrap_or(0.0),
            answered: ANSWERED
                .iter()
                .filter_map(|label| scrape.value_where(LOOKUPS, label))
                .fold(0.0, |total, count| total + count),
        })
    }
}

impl Run<'_> {
    /// Kill every sequencer, the executor and Redis. Bring them back in
    /// reverse order of need, and judge the floors at each step.
    ///
    /// # Errors
    /// Returns the first failed step or check.
    pub(super) async fn sequencer_executor_redis(&mut self, load: &RunningLoad) -> Result<()> {
        let head = self.reading().await?.block;
        let down = Instant::now();
        let stack = &mut *self.stack;
        (0..stack.sequencer_count()).try_for_each(|i| stack.stop_sequencer(i, Stop::Hard))?;
        stack.kill_executor();
        stack.kill_redis()?;
        eprintln!(
            "{}: the sequencers, the executor and Redis are down",
            self.at()
        );
        self.assert_load_stopped(load).await?;
        let landed = load.landed();

        self.return_sequencers()?;
        tokio::time::sleep(STAGGER).await;
        Class::Sequencer.assert_up(self)?;
        self.assert_parked_unanswered(load, landed).await?;

        self.stack.restart_redis()?;
        eprintln!("{}: Redis returned empty", self.at());
        tokio::time::sleep(STAGGER).await;
        Class::Sequencer.assert_up(self)?;
        anyhow::ensure!(
            load.landed() == landed,
            "{}: a transfer landed before the executor returned ({landed} -> {})",
            self.name,
            load.landed()
        );

        self.stack.restart_executor()?;
        let returned = Instant::now();
        let outage = returned.duration_since(down);
        eprintln!("{}: the executor returned", self.at());
        let took = match load.await_landed_above(landed, RESTART_BOUND).await {
            Ok(took) => took,
            Err(e) => {
                let lookups = self.case_lane_lookups().await;
                return Err(e.context(format!("the lookups of the case lane: {lookups:?}")));
            }
        };
        eprintln!(
            "{}: the case load lands again {:.1}s after the executor",
            self.at(),
            took.as_secs_f64()
        );
        self.assert_floors_looked_up().await?;
        self.assert_ingress_pair_live(RESTART_BOUND).await?;
        self.assert_executor_progress().await?;
        self.assert_sealed_while_down(head, outage, returned)
            .await?;
        self.assert_no_ref_below_floor().await?;
        self.assert_all_up()
    }

    /// Start every sequencer, and wait for its cluster session.
    fn return_sequencers(&mut self) -> Result<()> {
        Class::Sequencer.start(self)?;
        let stack = &mut *self.stack;
        (0..stack.sequencer_count())
            .try_for_each(|i| stack.await_sequencer_session(i, RESTART_BOUND))?;
        eprintln!("{}: the sequencers returned first", self.at());
        Ok(())
    }

    /// The lookups of every replica of the case lane.
    async fn case_lane_lookups(&self) -> Result<Vec<Lookups>> {
        let mut lanes = Vec::new();
        for i in self.lane_replicas(self.case_lane()?)? {
            lanes.push(Lookups::of(self.stack.sequencer_metrics(u32::try_from(i)?)).await?);
        }
        Ok(lanes)
    }

    /// Every replica of the case lane parked the case sender and asked for
    /// its floor. No source answered, and no transfer landed.
    async fn assert_parked_unanswered(&self, load: &RunningLoad, landed: u64) -> Result<()> {
        let lanes = self.case_lane_lookups().await?;
        let parked = lanes.iter().all(|l| l.asked > 0.0 && l.answered <= 0.0);
        anyhow::ensure!(
            parked && load.landed() == landed,
            "{}: expected parked senders with no answer and no landed transfer, read {lanes:?} and {landed} -> {} landed",
            self.name,
            load.landed()
        );
        eprintln!("{}: the case sender is parked: {lanes:?}", self.at());
        Ok(())
    }

    /// Every replica of the case lane got the floor of the case sender
    /// from a lookup: an executor, Redis or the local layer answered.
    async fn assert_floors_looked_up(&self) -> Result<()> {
        let lanes = self.case_lane_lookups().await?;
        anyhow::ensure!(
            lanes.iter().all(|l| l.answered > 0.0),
            "{}: a replica of the case lane has no answered floor lookup: {lanes:?}",
            self.name
        );
        eprintln!("{}: the floors came from a lookup: {lanes:?}", self.at());
        Ok(())
    }

    /// The sealer kept sealing while the classes were down. The executor
    /// head now, less the most blocks that the sealer can seal since the
    /// executor `returned`, is past the `head` before the fault by at
    /// least one block for each second of the `outage`. A sealer that
    /// stopped while the classes were down fails this.
    async fn assert_sealed_while_down(
        &self,
        head: u64,
        outage: Duration,
        returned: Instant,
    ) -> Result<()> {
        let now = self.reading().await?.block;
        let since = returned.elapsed().as_secs().saturating_add(1);
        // The sealer seals at most one block a tick; one more covers a
        // tick that ends early.
        let per_second = 1000u64
            .div_ceil(self.stack.cfg.cluster_tick_ms.get())
            .saturating_add(1);
        let after = since.saturating_mul(per_second);
        let least = head.saturating_add(outage.as_secs());
        anyhow::ensure!(
            now.saturating_sub(after) >= least,
            "{}: the executor head {now}, less {after} blocks sealed after the return, is not past {least}: the sealer did not seal through the {}s outage from head {head}",
            self.name,
            outage.as_secs()
        );
        eprintln!(
            "{}: sealed while down: head {head} -> {now} ({after} of them after the return) over a {}s outage",
            self.at(),
            outage.as_secs()
        );
        Ok(())
    }

    /// No returned process exited by the end: every sequencer, the
    /// executor and Redis still run.
    fn assert_all_up(&mut self) -> Result<()> {
        Class::Sequencer.assert_up(self)?;
        anyhow::ensure!(
            self.stack.executor_alive(),
            "{}: the returned executor exited",
            self.name
        );
        anyhow::ensure!(
            self.stack.redis_alive(),
            "{}: the returned Redis does not answer",
            self.name
        );
        Ok(())
    }
}
