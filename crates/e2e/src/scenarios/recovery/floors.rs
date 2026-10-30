//! `sequencer-executor-redis-loss`: the sequencers, the executor and Redis
//! die, so no source of a sender floor is left. They return in reverse
//! order of need, the stagger apart: the sequencers, Redis (empty), then
//! the executor. The returned sequencers park every established sender
//! and look its floor up. While no source can answer, no lookup answers
//! and no transfer lands: no floor is guessed. After the executor
//! returns, a lookup answers the floor of the case sender, every transfer
//! lands once in nonce order, and no ref sits below a floor.
//!
//! The replayed receipts of the executor would otherwise give the floor
//! first. So the stack runs the sequencers with the
//! `--first-floor-from-lookup` test seam: a receipt sets no first floor,
//! and the first floor comes only from a lookup.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;

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
/// The pause between two returns. A returned process must stay up
/// through it while the source it needs is still down.
const STAGGER: Duration = Duration::from_secs(6);

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
        self.assert_sequencers_up()?;
        self.assert_parked_unanswered(load, landed).await?;

        self.stack.restart_redis()?;
        eprintln!("{}: Redis returned empty", self.at());
        tokio::time::sleep(STAGGER).await;
        self.assert_sequencers_up()?;
        anyhow::ensure!(
            load.landed() == landed,
            "{}: a transfer landed before the executor returned ({landed} -> {})",
            self.name,
            load.landed()
        );

        self.stack.restart_executor()?;
        eprintln!("{}: the executor returned", self.at());
        let took = match load.await_landed_above(landed, RESTART_BOUND).await {
            Ok(took) => took,
            Err(e) => {
                let lookups = Lookups::of(self.case_sequencer()?).await;
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
        self.assert_no_ref_below_floor().await
    }

    fn return_sequencers(&mut self) -> Result<()> {
        let stack = &mut *self.stack;
        (0..stack.sequencer_count()).try_for_each(|i| stack.start_sequencer(i))?;
        (0..stack.sequencer_count())
            .try_for_each(|i| stack.await_sequencer_session(i, RESTART_BOUND))?;
        eprintln!("{}: the sequencers returned first", self.at());
        Ok(())
    }

    /// Every sequencer still runs: it waited for a floor source, with no
    /// exit.
    fn assert_sequencers_up(&mut self) -> Result<()> {
        let stack = &mut *self.stack;
        let down: Vec<usize> = (0..stack.sequencer_count())
            .filter(|&i| !stack.sequencer_alive(i).unwrap_or(false))
            .collect();
        anyhow::ensure!(
            down.is_empty(),
            "{}: sequencers {down:?} exited while no floor source answered",
            self.name
        );
        Ok(())
    }

    /// The sequencer of the case lane parked the case sender and asked for
    /// its floor. No source answered, and no transfer landed.
    async fn assert_parked_unanswered(&self, load: &RunningLoad, landed: u64) -> Result<()> {
        let lookups = Lookups::of(self.case_sequencer()?).await?;
        anyhow::ensure!(
            lookups.asked > 0.0 && lookups.answered <= 0.0 && load.landed() == landed,
            "{}: expected a parked sender with no answer and no landed transfer, read {lookups:?} and {landed} -> {} landed",
            self.name,
            load.landed()
        );
        eprintln!("{}: the case sender is parked: {lookups:?}", self.at());
        Ok(())
    }

    /// The sequencer of the case lane got the floor of the case sender
    /// from a lookup: an executor, Redis or the local layer answered.
    async fn assert_floors_looked_up(&self) -> Result<()> {
        let lookups = Lookups::of(self.case_sequencer()?).await?;
        anyhow::ensure!(
            lookups.answered > 0.0,
            "{}: the case load landed with no answered floor lookup: {lookups:?}",
            self.name
        );
        eprintln!("{}: the floor came from a lookup: {lookups:?}", self.at());
        Ok(())
    }

    /// The metrics address of the sequencer of the case lane.
    fn case_sequencer(&self) -> Result<SocketAddr> {
        Ok(self.stack.sequencer_metrics(self.case_lane()?))
    }
}
