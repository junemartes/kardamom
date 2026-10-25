//! The refill check of a combined case: after the return, the
//! da-watcher publishes a lost epoch again. The da-watcher keeps its
//! counters in memory, so the check follows a restart of the
//! da-watcher. The da-watcher is no class of a case, so the check also
//! reports each restart, and fails on a restart that no driver loss
//! explains.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget, Outcome};
use crate::probes::DA_WATCHER_PORT;

/// The da-watcher's count of epochs it published again. It rises while
/// the sealers are down: no boundary confirms an epoch, so the
/// da-watcher publishes it again after the re-publish timeout. After the
/// return, a lost epoch moves it when the da-watcher fills the gap. The
/// sequencer's origin-gap counter is not a baseline: it resets with the
/// sequencer.
const EPOCHS_REPUBLISHED: &str = "kardamom_da_watcher_epochs_republished_total";
/// The da-watcher's count of every epoch it published. It rises with
/// each L1 block, also for an offer that no queue takes.
const EPOCHS_PUBLISHED: &str = "kardamom_da_watcher_epochs_published_total";
/// The Nomad job of the da-watcher.
const WATCHER_JOB: &str = "da-watcher";
/// The line that the fail-fast handler of an Aeron client logs before it
/// ends the process.
const FAIL_FAST_EXIT: &str = "aeron client error; exiting";
/// The text of the Aeron client error on the loss of the media driver:
/// a driver shutdown or a driver keepalive timeout.
const DRIVER_LOSS: &str = "MediaDriver";
/// The causes of a start that finds no live media driver: the client
/// finds no `CnC` file, or no heartbeat in it. The process exits, and
/// Nomad starts it again until the new driver runs. These exits follow a
/// driver loss, so they are part of that loss.
const DRIVER_WAITS: [&str; 2] = ["CnC file not created", "no driver heartbeat detected"];
/// How long the counters get to show a refill, and the pause between
/// two readings, in seconds.
const REFILL_SECS: u64 = 60;
const REFILL_EVERY_SECS: u64 = 3;

/// The counters that show the refill of a lost epoch. `None` when the
/// exporter did not answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Refills {
    pub(super) republished: Option<i64>,
    pub(super) published: Option<i64>,
}

/// How the da-watcher filled a lost epoch again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refill {
    /// The da-watcher published an unconfirmed epoch again.
    Republished,
    /// The da-watcher restarted, and published an epoch after the
    /// reading that showed the restart. A restarted da-watcher resumes
    /// after the sealer's L1 origin.
    Restarted,
}

impl Refills {
    async fn read(h: &Harness) -> Self {
        let read = |name: &'static str| h.probes.aux_metric_where(DA_WATCHER_PORT, name, "");
        Self {
            republished: read(EPOCHS_REPUBLISHED).await,
            published: read(EPOCHS_PUBLISHED).await,
        }
    }

    /// Wait until the exporter answers both counters. A driver loss
    /// takes the da-watcher down until the driver restarts.
    ///
    /// # Errors
    ///
    /// Returns an error if the exporter does not answer within the
    /// restart SLO of a media driver.
    async fn await_answering(h: &Harness, ctx: &str) -> anyhow::Result<()> {
        let budget = Budget::new(
            h.knobs.driver_restart_slo(),
            Duration::from_secs(REFILL_EVERY_SECS),
        );
        poll::until(budget, |_| async move {
            let now = Self::read(h).await;
            Ok(now.republished.zip(now.published).map(|_| ()))
        })
        .await?
        .or_fail(|t| {
            crate::chaos_fail!(
                "{ctx}: the da-watcher exporter did not answer within {}s",
                t.as_secs()
            )
        })
        .map(|_| ())
    }

    pub(super) fn describe(self) -> String {
        let show = |v: Option<i64>| v.map_or("?".to_string(), |v| v.to_string());
        format!(
            "republished {}, published {}",
            show(self.republished),
            show(self.published)
        )
    }
}

/// The baseline of the refill counters, as the readings move it.
///
/// A counter below the baseline shows a restart of the da-watcher. The
/// watch then takes that reading as its new baseline, and needs a rise
/// from it. The publishes of the new process before that reading prove
/// nothing: they can fall while the sealers are down, into no queue.
///
/// The limits of the drop:
/// - A new process whose counters pass the baseline before a reading
///   shows no drop. Its publishes then count as a rise.
/// - A counter that the exporter did not answer at the baseline shows
///   no drop.
///
/// [`WatcherRuns`] reads the restarts from Nomad, so the check reports
/// these restarts too.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RefillWatch {
    base: Refills,
    restarted: bool,
}

impl RefillWatch {
    pub(super) fn new(base: Refills) -> Self {
        Self {
            base,
            restarted: false,
        }
    }

    /// The refill that `now` shows, or `None`. A drop moves the baseline
    /// to `now`.
    pub(super) fn step(&mut self, now: Refills) -> Option<Refill> {
        let rose = |a: Option<i64>, b: Option<i64>| a.zip(b).is_some_and(|(a, b)| b > a);
        let base = self.base;
        if rose(now.republished, base.republished) || rose(now.published, base.published) {
            *self = Self {
                base: now,
                restarted: true,
            };
            return None;
        }
        if rose(base.republished, now.republished) {
            return Some(Refill::Republished);
        }
        (self.restarted && rose(base.published, now.published)).then_some(Refill::Restarted)
    }

    /// Read the counters until they show a refill, within the budget.
    async fn watch(self, h: &Harness) -> anyhow::Result<Outcome<(Refills, Refill)>> {
        let watch = Cell::new(self);
        let watch = &watch;
        poll::until(
            Budget::secs(REFILL_SECS, REFILL_EVERY_SECS),
            |_| async move {
                let now = Refills::read(h).await;
                let mut state = watch.get();
                let refill = state.step(now);
                watch.set(state);
                Ok(refill.map(|refill| (now, refill)))
            },
        )
        .await
    }
}

/// The runs of the da-watcher, as Nomad and the da-watcher log show
/// them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WatcherRuns {
    /// The running allocations by id, with the restarts of their tasks.
    allocs: BTreeMap<String, u64>,
    /// The fail-fast exit lines in the logs of the da-watcher. Each line
    /// carries its timestamp, so a line is one exit.
    exits: BTreeSet<String>,
    /// The count of the starts that found no live media driver. Their
    /// cause lines carry no timestamp, so the runs count them.
    driver_waits: u64,
}

impl WatcherRuns {
    /// The runs from the allocations `(id, restarts)` and the logs.
    pub(super) fn new(allocs: impl IntoIterator<Item = (String, u64)>, logs: &str) -> Self {
        let driver_waits = logs
            .lines()
            .filter(|line| DRIVER_WAITS.iter().any(|cause| line.contains(cause)))
            .map(|_| 1_u64)
            .sum();
        Self {
            allocs: allocs.into_iter().collect(),
            exits: logs
                .lines()
                .filter(|line| line.contains(FAIL_FAST_EXIT))
                .map(|line| line.trim().to_string())
                .collect(),
            driver_waits,
        }
    }

    async fn read(h: &Harness) -> anyhow::Result<Self> {
        let allocs = h.nomad.running(WATCHER_JOB).await?;
        let logs = h.nomad.job_logs(WATCHER_JOB, Streams::Both).await?;
        Ok(Self::new(
            allocs.iter().map(|a| (a.id.clone(), a.task_restarts())),
            &logs,
        ))
    }

    /// The restarts since `base`. A new allocation counts as one
    /// restart, plus the restarts of its tasks.
    fn restarts_since(&self, base: &Self) -> u64 {
        self.allocs
            .iter()
            .map(|(id, restarts)| match base.allocs.get(id) {
                // Nomad never lowers the restart count of one allocation.
                Some(before) => restarts.saturating_sub(*before),
                // A restart count never comes near u64::MAX.
                None => restarts.saturating_add(1),
            })
            .sum()
    }

    /// The verdict on the restarts since `base`: `None` with no restart,
    /// a note when a driver loss explains each restart, else an error.
    /// A start that found no live driver after a loss is part of that
    /// loss.
    pub(super) fn judge(&self, base: &Self) -> Result<Option<String>, String> {
        let restarts = self.restarts_since(base);
        let exits: Vec<&str> = self
            .exits
            .difference(&base.exits)
            .map(String::as_str)
            .collect();
        let driver_losses: u64 = exits
            .iter()
            .filter(|line| line.contains(DRIVER_LOSS))
            .map(|_| 1)
            .sum();
        // Nomad never lowers the counts in one log.
        let waits = self.driver_waits.saturating_sub(base.driver_waits);
        let losses = match driver_losses {
            0 => 0,
            // A count of log lines never comes near u64::MAX.
            n => n.saturating_add(waits),
        };
        let allocs: Vec<String> = self
            .allocs
            .iter()
            .map(|(id, n)| format!("{} restarted {n} times", id.get(..8).unwrap_or(id)))
            .collect();
        let note = format!(
            "the da-watcher restarted {restarts} times during the case, and it is no class of the case \
             (allocations: {}; starts with no live driver: {waits}; fail-fast exits: {})",
            allocs.join(", "),
            if exits.is_empty() {
                "none".to_string()
            } else {
                exits.join(" | ")
            }
        );
        match restarts {
            0 => Ok(None),
            n if losses >= n => Ok(Some(note)),
            _ => Err(format!(
                "{note}; a driver loss explains {losses} of the {restarts} restarts"
            )),
        }
    }
}

/// What the refill check compares against: the counters and the runs of
/// the da-watcher before the fault.
pub(super) struct RefillBaseline {
    counters: Refills,
    runs: WatcherRuns,
}

impl RefillBaseline {
    pub(super) async fn read(h: &Harness) -> anyhow::Result<Self> {
        Ok(Self {
            counters: Refills::read(h).await,
            runs: WatcherRuns::read(h).await?,
        })
    }

    /// The counters show a refill within the budget, and a driver loss
    /// explains each restart of the da-watcher. A restart is reported as
    /// a warning. A driver loss on the aux node restarts the da-watcher
    /// once the new driver runs, so the watch starts when its exporter
    /// answers.
    pub(super) async fn assert(&self, h: &Harness, ctx: &str) -> anyhow::Result<()> {
        Refills::await_answering(h, ctx).await?;
        let outcome = RefillWatch::new(self.counters).watch(h).await?;
        let restarts = WatcherRuns::read(h)
            .await?
            .judge(&self.runs)
            .map_err(|why| crate::chaos_fail!("{ctx}: {why}"))?;
        if let Some(note) = &restarts {
            crate::log(format!("WARN: {ctx}: {note}"));
        }
        let (now, refill) = match outcome {
            Outcome::Ready { value, .. } => value,
            Outcome::TimedOut { elapsed } => {
                return Err(crate::chaos_fail!(
                    "{ctx}: no epoch was published again within {}s ({} -> {})",
                    elapsed.as_secs(),
                    self.counters.describe(),
                    Refills::read(h).await.describe()
                ));
            }
        };
        if refill == Refill::Restarted && restarts.is_none() {
            return Err(crate::chaos_fail!(
                "{ctx}: the da-watcher counters dropped, but Nomad shows no restart of the da-watcher"
            ));
        }
        crate::log(format!(
            "{ctx}: a lost epoch was filled again ({} -> {}, {refill:?})",
            self.counters.describe(),
            now.describe()
        ));
        Ok(())
    }
}
