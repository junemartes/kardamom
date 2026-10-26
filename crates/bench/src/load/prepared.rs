//! Prepared queues run with observable, bounded submission and drain phases.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use super::progress::Progress;
use super::report::ReportInput;
use super::{
    BackgroundTasks, LoadConfig, LoadRun, ReceiptFeed, RunHandles, RunSetup, SubmitOpts, Tracker,
    engine, pacer, print_report,
};

/// The signed queues and their configuration. See [`super::prepare`].
pub struct Prepared {
    cfg: LoadConfig,
    setup: RunSetup,
    progress: Progress,
}

impl Prepared {
    pub(super) fn new(cfg: LoadConfig, setup: RunSetup) -> anyhow::Result<Self> {
        let mut tracker = Tracker::new()?;
        tracker.expect(&setup.signers, setup.queues.planned());
        let progress = Progress::new(Arc::new(tracker));
        Ok(Self {
            cfg,
            setup,
            progress,
        })
    }

    /// Observe this run's own traffic and submission window.
    #[must_use]
    pub fn progress(&self) -> Progress {
        self.progress.clone()
    }

    /// Run the prepared harness. Returns whether the verdict passed.
    ///
    /// # Errors
    ///
    /// Returns an error on a submission-loop failure or a report write.
    pub async fn run(self) -> anyhow::Result<bool> {
        let Self {
            cfg,
            progress,
            setup:
                RunSetup {
                    client,
                    signers,
                    mut queues,
                },
        } = self;

        let scraper = cfg.scraper()?;
        let tracker = Arc::clone(&progress.tracker);
        let sem = Arc::new(Semaphore::new(cfg.max_in_flight.get() as usize));
        let handles = RunHandles {
            client: Arc::clone(&client),
            sem: Arc::clone(&sem),
            tracker: Arc::clone(&tracker),
        };
        let mut tasks = tokio::task::JoinSet::new();
        let mode = cfg.submit_mode();

        let run = LoadRun {
            cfg: &cfg,
            tracker: &tracker,
            scraper: &scraper,
            receipt_clients: cfg.receipt_clients(&client)?,
        };

        let ReceiptFeed {
            confirm: feed_confirm,
            task: feed,
        } = run.spawn_receipt_feed(&signers);
        // A live sweeper, in every mode. An entry the feed misses, or an
        // accepted submit whose immediate re-fetch answered null, is
        // re-fetched within 2 to 7 seconds, instead of waiting for the
        // end-of-run drain. Keep this cadence well inside the ingress receipt
        // cache's query horizon (capacity divided by rate, about 27 seconds at
        // 4,800 tx/s with the default 128k capacity, oldest out first), and
        // inside the window in which a receipt is still on some ingress: a
        // job update restarts every replica in turn, and a receipt that only
        // the drain asks for is then on none of them.
        let sweeper = Some(
            Arc::new(engine::Drainer::new(
                run.receipt_clients.clone(),
                Arc::clone(&tracker),
            ))
            .spawn_pending_sweeper(Duration::from_secs(5), Duration::from_secs(2)),
        );
        let background = BackgroundTasks([feed, sweeper]);

        // --- ramp (soak mode only) -------------------------------------------
        let mut ramp = Vec::new();
        let discovered_max: std::num::NonZeroU32 = if cfg.chaos_mode || cfg.fixed_rate {
            cfg.target_tps
        } else {
            run.ramp_to_max(&handles, &mut tasks, &mut queues, &mut ramp)
                .await
        };

        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "a discovered tx/s rate stays far under i64::MAX, rounded for the soak target"
        )]
        let soak_rate: std::num::NonZeroU32 = if cfg.chaos_mode || cfg.fixed_rate {
            cfg.target_tps
        } else {
            let rate = ((f64::from(discovered_max.get()) * cfg.soak_fraction).round() as u32)
                .clamp(1, cfg.target_tps.get());
            std::num::NonZeroU32::new(rate)
                .ok_or_else(|| anyhow::anyhow!("discovered soak rate rounds to zero"))?
        };

        // --- soak ------------------------------------------------------------
        tracing::info!(
            soak_rate = soak_rate.get(),
            discovered_max = discovered_max.get(),
            "kardamom-load: soaking"
        );
        let base = scraper.snapshot().await;
        let duration = Submission {
            progress: &progress,
            handles: &handles,
            tasks: &mut tasks,
            queues: &mut queues,
            rate: soak_rate,
            duration: cfg.duration,
            opts: SubmitOpts {
                retry: cfg.retry_submit,
                pause_wait: cfg.pause_wait,
                mode,
                feed_confirm,
            },
        }
        .run()
        .await;

        let settled = run.settle_and_snapshot(&mut tasks, background).await?;

        let verdict = run.build_verdict(&base, &settled.fin, settled.recheck.as_ref());
        let report = run.build_report(ReportInput {
            verdict,
            ramp,
            discovered_max: discovered_max.get(),
            soak_rate: soak_rate.get(),
            scrape_fallbacks: settled.scrape_fallbacks(&base),
            executor_queries_served_delta: settled.fin.query_deltas(&base),
            duration,
        });

        print_report(&report);
        run.write_report_json(&report)?;

        Ok(report.verdict.pass)
    }
}

struct Submission<'a> {
    progress: &'a Progress,
    handles: &'a RunHandles,
    tasks: &'a mut tokio::task::JoinSet<()>,
    queues: &'a mut super::Queues,
    rate: NonZeroU32,
    duration: Duration,
    opts: SubmitOpts,
}

impl Submission<'_> {
    async fn run(self) -> Duration {
        let _window = self.progress.begin();
        let mut stopped = self.progress.stop_requested();
        let started = Instant::now();
        tokio::select! {
            biased;
            () = tokio::time::sleep(self.duration) => {},
            _ = stopped.wait_for(|stop| *stop) => {},
            () = pacer(self.handles, self.tasks, self.queues, self.rate, self.duration, self.opts) => {},
        }
        started.elapsed()
    }
}

#[cfg(test)]
mod tests;
