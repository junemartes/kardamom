//! `kardamom-load` is an open-loop sustained-load and chaos verification
//! harness.
//!
//! It drives a pre-generated transaction stream through ingress at a
//! paced rate, tracks every transaction to a receipt (must-deliver),
//! reads the cluster's Prometheus metrics for drop and liveness
//! signals, and renders a pass-fail verdict. It runs in two modes:
//! - soak (the default): ramp to the sustainable maximum rate, then
//!   soak at a fraction of it for `duration`.
//! - chaos (`--chaos-mode`): skip the ramp, and soak at `target_tps`
//!   for `duration` while an external orchestrator injects failures.
//!   A transient gap or outage is only informational. Only a
//!   never-recovering executor or an undelivered receipt fails the run.

pub mod accounting;
pub mod config;
pub mod defi;
pub(crate) mod engine;
mod feed;
pub mod plan;
pub(crate) mod scrape;
mod tracker;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::Address;
use jsonrpsee::http_client::HttpClient;
use tokio::sync::Semaphore;

use crate::config::{preflight_chain_id, rpc_client};
use crate::load::accounting::{EvalInput, evaluate, print_report, step_gap_ok, step_seq_clean};
use crate::load::engine::{
    Queues, RunHandles, SubmitMode, SubmitOpts, Tracker, join_submit_tasks, pacer,
};
use crate::load::feed::receipt_feed_task;
use crate::load::plan::{PlannedTx, TxPlanParams};
use crate::load::scrape::{MetricsSnapshot, Scraper};
use crate::signers::{DerivedSigner, SignerSet};

pub use config::{
    ANVIL_MNEMONIC, Completeness, LoadConfig, LoadReport, RampStep, SenderRange, Workload,
};

/// Parse a `0x`-prefixed JSON-RPC hex quantity into a `u64`.
pub(crate) fn hex_u64(s: &str) -> Option<u64> {
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

/// Apply [`hex_u64`] to a JSON string field. Returns `None` for a null,
/// missing, or non-string value.
pub(crate) fn json_hex_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_str().and_then(hex_u64)
}

fn build_scraper(cfg: &LoadConfig) -> Scraper {
    Scraper {
        via_docker: cfg.metrics_via_docker,
        scrape: cfg
            .scrape
            .iter()
            .map(|s| s.to_lowercase())
            .collect::<BTreeSet<_>>(),
        executor_nodes: cfg.executor_nodes.clone(),
        ingress_node: cfg.ingress_node.clone(),
        sequencer_nodes: cfg.sequencer_nodes.clone(),
    }
}

/// [`LoadConfig::build_queues`]'s result: the presigned per-sender
/// submit queues, plus any `DeFi` deployment transactions to land
/// first.
struct BuiltQueues {
    queues: Queues,
    defi_deploys: Option<Vec<PlannedTx>>,
}

impl LoadConfig {
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "an estimated queue depth stays far under usize::MAX for any run this harness drives"
    )]
    fn per_sender_estimate(&self) -> usize {
        let ramp_steps = if self.chaos_mode || self.fixed_rate {
            0
        } else {
            u64::from(self.target_tps.get().div_ceil(self.ramp_step_tps.get()))
        };
        let total_secs = ramp_steps
            .saturating_mul(self.ramp_step_secs.get())
            .saturating_add(self.duration.as_secs());
        let est_total = u64::from(self.target_tps.get()).saturating_mul(total_secs);
        ((est_total as f64 * 1.2 / f64::from(self.sender_range.count().get())).ceil() as usize)
            .saturating_add(64)
    }

    /// Build the presigned per-sender queues for `self.workload`, and
    /// the `DeFi` deployment transactions to land first, if the
    /// workload needs them.
    fn build_queues(
        &self,
        signers: &SignerSet,
        chain_id: u64,
        per_sender: usize,
    ) -> anyhow::Result<BuiltQueues> {
        let plan_params = TxPlanParams {
            chain_id,
            nonce_start: self.nonce_start,
            gas_price: self.gas_price,
        };
        let (queues_vec, defi_deploys) = match self.workload {
            Workload::Transfers => (
                plan::pregenerate(signers, self.to, self.value, per_sender, plan_params)?,
                None,
            ),
            Workload::Defi => {
                let dep = defi::deployment_txs(signers, plan_params)?;
                tracing::info!(
                    pool = %dep.contracts.pool,
                    vault = %dep.contracts.vault,
                    clob = %dep.contracts.clob,
                    "defi workload: deploying bench contracts"
                );
                let queues =
                    defi::pregenerate_defi(signers, &dep.contracts, per_sender, plan_params)?;
                (queues, Some(dep.txs))
            }
        };
        Ok(BuiltQueues {
            queues: Queues::new(queues_vec),
            defi_deploys,
        })
    }
}

/// The result of [`LoadRun::spawn_receipt_feed`].
struct ReceiptFeed {
    /// Whether the pacer should trust the feed instead of a
    /// per-transaction re-fetch.
    confirm: bool,
    task: Option<tokio::task::JoinHandle<()>>,
}

/// The result of `settle_and_snapshot`.
struct Settled {
    fin: MetricsSnapshot,
    recheck: Option<MetricsSnapshot>,
}

/// One run's config, handles, and tracker: the settle, report-building,
/// and ramp steps at the end of [`run`] are methods on this struct.
struct LoadRun<'a> {
    cfg: &'a LoadConfig,
    tracker: &'a Arc<Tracker>,
    client: &'a Arc<HttpClient>,
    scraper: &'a Scraper,
}

impl LoadRun<'_> {
    /// Start the receipt feed, if the mode needs it: subscribe mode
    /// always needs it, and blocking mode needs it only when
    /// `feed_confirm` replaces the per-transaction re-fetch. Returns
    /// whether the pacer should trust the feed (`feed_confirm`)
    /// alongside its handle.
    ///
    /// Receipts arrive on one multiplexed WebSocket feed, filtered to
    /// this run's senders. In subscribe mode, this replaces each
    /// submit's parked connection. In blocking mode (`feed_confirm`),
    /// it replaces a per-transaction re-fetch after each accepted
    /// submit. The feed runs for the whole ramp and soak, and stops
    /// after the drain.
    fn spawn_receipt_feed(&self, signers: &[DerivedSigner]) -> ReceiptFeed {
        let confirm = self.cfg.feed_confirm_on();
        let task = if self.cfg.subscribe || confirm {
            let ws_url = self.cfg.rpc.replacen("http", "ws", 1);
            let addrs: Vec<Address> = signers.iter().map(|s| s.address).collect();
            Some(tokio::spawn(receipt_feed_task(
                ws_url,
                addrs,
                Arc::clone(self.tracker),
            )))
        } else {
            None
        };
        ReceiptFeed { confirm, task }
    }

    /// After the soak or fixed-rate phase ends: join every in-flight
    /// submit task, drain unconfirmed receipts to the drain timeout's
    /// tail, stop the feed and sweeper, then snapshot metrics after a
    /// short settle time.
    ///
    /// A chaos-restarted executor's block gauge resets to 0, so
    /// `final - base` can be zero or negative while it is replaying in a
    /// healthy way. In chaos mode, take a recheck sample a few seconds
    /// later, so `evaluate` can tell RECOVERING, where the gauge moves
    /// again, from FROZEN.
    async fn settle_and_snapshot(
        &self,
        tasks: &mut tokio::task::JoinSet<()>,
        feed: Option<tokio::task::JoinHandle<()>>,
        sweeper: Option<tokio::task::JoinHandle<()>>,
    ) -> Settled {
        let deadline = Instant::now() + self.cfg.drain_timeout;
        join_submit_tasks(tasks, deadline).await;
        engine::Drainer::new(Arc::clone(self.client), Arc::clone(self.tracker))
            .drain(deadline)
            .await;
        if let Some(feed) = feed {
            feed.abort();
        }
        if let Some(sweeper) = sweeper {
            sweeper.abort();
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        let fin = self.scraper.snapshot().await;
        let recheck = if self.cfg.chaos_mode {
            tokio::time::sleep(Duration::from_secs(3)).await;
            Some(self.scraper.snapshot().await)
        } else {
            None
        };
        Settled { fin, recheck }
    }

    /// Build the verdict from the tracker's final counts and the
    /// metric snapshots taken before and after the soak.
    fn build_verdict(
        &self,
        base: &MetricsSnapshot,
        fin: &MetricsSnapshot,
        recheck: Option<&MetricsSnapshot>,
    ) -> accounting::Verdict {
        let counts = self.tracker.counts();
        let pending = self.tracker.remaining_pending();
        // `sample_pending` reads the same map `pending`'s counts came
        // from, so it yields nothing when there is nothing left
        // pending: no separate empty check is needed here.
        self.tracker.sample_pending(32).into_iter().for_each(|s| {
            tracing::warn!(
                hash = %s.hash,
                accepted = s.accepted,
                age_secs = s.age.as_secs(),
                "UNRESOLVED pending tx (forensics: query per replica)"
            );
        });
        // In `Offered` mode, an unlanded transaction, one that was
        // offered but never receipted, is also a must-deliver
        // violation. Fold it into `missing` for the gate.
        let missing_gate = if self.cfg.completeness == Completeness::Offered {
            pending.missing + pending.unlanded
        } else {
            pending.missing
        };
        evaluate(&EvalInput {
            counts,
            missing: missing_gate,
            unlanded: pending.unlanded,
            base,
            fin,
            recheck,
            max_gap: self.cfg.max_gap,
            assert_all_delivered: self.cfg.assert_all_delivered,
            ack_proves_receipt: !self.cfg.subscribe,
            chaos_mode: self.cfg.chaos_mode,
        })
    }

    /// Assemble the final [`LoadReport`] from the run's timing, the
    /// tracker's latency percentiles and gas total, and the verdict.
    fn build_report(
        &self,
        verdict: accounting::Verdict,
        ramp: Vec<RampStep>,
        discovered_max: u32,
        soak_rate: u32,
    ) -> LoadReport {
        let lat = self.tracker.latency_us();
        LoadReport {
            mode: if self.cfg.chaos_mode {
                "chaos"
            } else if self.cfg.fixed_rate {
                "fixed"
            } else {
                "soak"
            }
            .to_string(),
            target_tps: self.cfg.target_tps.get(),
            discovered_max_tps: discovered_max,
            soak_rate_tps: soak_rate,
            duration_secs: self.cfg.duration.as_secs_f64(),
            ramp,
            lat_p50_us: lat.p50,
            lat_p95_us: lat.p95,
            lat_p99_us: lat.p99,
            lat_max_us: lat.max,
            total_gas: self.tracker.total_gas(),
            workload: self.cfg.workload.to_string(),
            verdict,
        }
    }

    /// Write the report as pretty JSON to `cfg.output`, if set.
    fn write_report_json(&self, report: &LoadReport) -> anyhow::Result<()> {
        let Some(path) = &self.cfg.output else {
            return Ok(());
        };
        crate::report::write_json_pretty(path, report)?;
        tracing::info!("wrote report to {}", path.display());
        Ok(())
    }

    /// Ramp `handles`/`queues` up in `self.cfg.ramp_step_tps`
    /// increments, appending each step's record to `ramp`, and return
    /// the highest sustainable rate found.
    async fn ramp_to_max(
        &self,
        handles: &RunHandles,
        tasks: &mut tokio::task::JoinSet<()>,
        queues: &mut Queues,
        ramp: &mut Vec<RampStep>,
    ) -> std::num::NonZeroU32 {
        let mut run = RampRun {
            cfg: self.cfg,
            handles,
            tasks,
            scraper: self.scraper,
            queues,
            ramp,
            mode: self.cfg.submit_mode(),
            step_dur: Duration::from_secs(self.cfg.ramp_step_secs.get()),
        };
        let mut discovered: Option<std::num::NonZeroU32> = None;
        let mut rate = self.cfg.ramp_step_tps;
        while rate.get() <= self.cfg.target_tps.get() {
            let Some(next) = run.ramp_step(rate).await else {
                break;
            };
            (discovered, rate) = (Some(rate), next);
        }
        // No step ran (the first step size already exceeds
        // `target_tps`), or none was sustainable: fall back to the
        // smallest step size.
        discovered.unwrap_or(self.cfg.ramp_step_tps)
    }
}

/// What `run` needs before the ramp and soak: a connected client, the
/// validated sender set, and the pre-signed submit queues, with any
/// `DeFi` setup already landed.
struct RunSetup {
    client: Arc<HttpClient>,
    signers: SignerSet,
    queues: Queues,
}

/// Connect, derive signers, pre-generate the submit queues, and land
/// any `DeFi` setup transactions before any load starts. Every
/// workload call targets their computed addresses; a call that
/// arrives before its contract exists would revert and spoil the
/// verdict.
async fn prepare_run(cfg: &LoadConfig) -> anyhow::Result<RunSetup> {
    let client = Arc::new(rpc_client(&cfg.rpc, cfg.max_in_flight.get())?);

    let chain_id = match cfg.chain_id {
        Some(c) => c,
        None => preflight_chain_id(&client).await?,
    };

    let signers = crate::mnemonic::derive_signers(&cfg.mnemonic, cfg.sender_range.derive_count())?;
    let signers = SignerSet::new(signers[cfg.sender_range.offset() as usize..].to_vec())?;

    let per_sender = cfg.per_sender_estimate();
    tracing::info!(
        senders = cfg.sender_range.count(),
        per_sender,
        target_tps = cfg.target_tps.get(),
        chaos = cfg.chaos_mode,
        "kardamom-load: pre-generating {} txs",
        per_sender.saturating_mul(signers.len())
    );
    let built = cfg.build_queues(&signers, chain_id, per_sender)?;
    if let Some(deploys) = built.defi_deploys {
        defi::deploy_and_confirm(&client, &deploys).await?;
    }

    Ok(RunSetup {
        client,
        signers,
        queues: built.queues,
    })
}

/// Run the harness. Returns whether the verdict passed.
///
/// # Errors
/// Returns an error on client construction failure, chain-id preflight
/// failure, signer derivation failure, or pre-generation failure. A
/// failing verdict is not an error: this function returns `Ok(false)`
/// so the caller can choose the exit code.
pub async fn run(cfg: LoadConfig) -> anyhow::Result<bool> {
    let RunSetup {
        client,
        signers,
        mut queues,
    } = prepare_run(&cfg).await?;

    let scraper = build_scraper(&cfg);
    let tracker = Arc::new(Tracker::new()?);
    let sem = Arc::new(Semaphore::new(cfg.max_in_flight.get() as usize));
    let handles = RunHandles {
        client: Arc::clone(&client),
        sem: Arc::clone(&sem),
        tracker: Arc::clone(&tracker),
    };
    let mut tasks = tokio::task::JoinSet::new();
    // Outside chaos mode, the ingress receipt cache is stable, with no
    // restarts. So an accepted transaction whose receipt cannot be
    // re-fetched is a real must-deliver violation, not restart noise.
    // Verify it independently.
    let verify_receipts = !cfg.chaos_mode;
    let mode = cfg.submit_mode();

    let run = LoadRun {
        cfg: &cfg,
        tracker: &tracker,
        client: &client,
        scraper: &scraper,
    };

    let ReceiptFeed {
        confirm: feed_confirm,
        task: feed,
    } = run.spawn_receipt_feed(&signers);
    // Back up the feed with a live sweeper. An entry the feed misses is
    // re-fetched within 2 to 7 seconds, instead of waiting for the
    // end-of-run drain. Keep this cadence well inside the ingress receipt
    // cache's query horizon (capacity divided by rate, about 27 seconds at
    // 4,800 tx/s with the default 128k capacity). Eviction order is
    // arbitrary, so a late poll can miss even a younger entry.
    let sweeper = feed.as_ref().map(|_| {
        Arc::new(engine::Drainer::new(
            Arc::clone(&client),
            Arc::clone(&tracker),
        ))
        .spawn_pending_sweeper(Duration::from_secs(5), Duration::from_secs(2))
    });

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
    pacer(
        &handles,
        &mut tasks,
        &mut queues,
        soak_rate,
        cfg.duration,
        SubmitOpts {
            retry: cfg.retry_submit,
            verify_receipts,
            mode,
            feed_confirm,
        },
    )
    .await;

    let settled = run.settle_and_snapshot(&mut tasks, feed, sweeper).await;

    let verdict = run.build_verdict(&base, &settled.fin, settled.recheck.as_ref());
    let report = run.build_report(verdict, ramp, discovered_max.get(), soak_rate.get());

    print_report(&report);
    run.write_report_json(&report)?;

    Ok(report.verdict.pass)
}

/// The state one ramp-to-max run needs across every step: the fixed
/// config and handles, plus the ramp record each step appends to.
struct RampRun<'a> {
    cfg: &'a LoadConfig,
    handles: &'a RunHandles,
    tasks: &'a mut tokio::task::JoinSet<()>,
    scraper: &'a Scraper,
    queues: &'a mut Queues,
    ramp: &'a mut Vec<RampStep>,
    mode: SubmitMode,
    step_dur: Duration,
}

impl RampRun<'_> {
    /// Pace at `rate` tx/s for one step, and measure whether the
    /// ingress kept pace and the cluster stayed clean.
    async fn run_step(&mut self, rate: std::num::NonZeroU32) -> RampStep {
        let before = self.handles.tracker.counts();
        let s0 = self.scraper.snapshot().await;
        pacer(
            self.handles,
            self.tasks,
            self.queues,
            rate,
            self.step_dur,
            SubmitOpts {
                retry: self.cfg.retry_submit,
                verify_receipts: !self.cfg.chaos_mode,
                mode: self.mode,
                feed_confirm: self.cfg.feed_confirm_on(),
            },
        )
        .await;
        let after = self.handles.tracker.counts();
        let s1 = self.scraper.snapshot().await;

        let offered = after.offered - before.offered;
        let accepted = after.accepted - before.accepted;
        #[allow(
            clippy::cast_precision_loss,
            reason = "an offered/accepted count over one ramp step stays far under 2^52"
        )]
        let accept_ratio = if offered > 0 {
            accepted as f64 / offered as f64
        } else {
            0.0
        };
        // In subscribe mode, an ack means published, not receipted. So the
        // accept ratio alone would let the ramp go past the pipeline's
        // drain rate, because admission stays at 1.0 while receipts queue
        // up. Require receipts to keep pace with offers within the step,
        // with slack for the in-flight tail at the step boundary.
        #[allow(
            clippy::cast_precision_loss,
            reason = "an offered/receipted count over one ramp step stays far under 2^52"
        )]
        let recv_ok = if self.mode == SubmitMode::Subscribe && offered > 0 {
            let receipted = after.receipted - before.receipted;
            receipted as f64 / offered as f64 >= 0.95
        } else {
            true
        };
        let gap_ok = step_gap_ok(&s0, &s1, self.cfg.max_gap);
        let seq_clean = step_seq_clean(&s0, &s1);
        let sustainable = accept_ratio >= 0.99 && recv_ok && gap_ok && seq_clean;
        let lat = self.handles.tracker.take_step_latency_us();
        let gas_used = self.handles.tracker.take_step_gas();
        #[allow(
            clippy::cast_precision_loss,
            reason = "a per-step gas total stays far under 2^52"
        )]
        let mgas_s = gas_used as f64 / 1e6 / self.cfg.ramp_step_secs.get() as f64;
        tracing::info!(
            rate = rate.get(),
            offered,
            accepted,
            accept_ratio = format!("{accept_ratio:.3}"),
            p50_ms = lat.p50 / 1000,
            p95_ms = lat.p95 / 1000,
            p99_ms = lat.p99 / 1000,
            mgas_s = format!("{mgas_s:.1}"),
            gap_ok,
            seq_clean,
            sustainable,
            "ramp step"
        );
        RampStep {
            rate: rate.get(),
            accept_ratio,
            gap_ok,
            seq_clean,
            sustainable,
            lat_p50_us: lat.p50,
            lat_p95_us: lat.p95,
            gas_used,
            lat_p99_us: lat.p99,
        }
    }

    /// Run one ramp step at `rate`, record it, and return the next
    /// rate to try if it was sustainable, else `None` to stop ramping.
    async fn ramp_step(&mut self, rate: std::num::NonZeroU32) -> Option<std::num::NonZeroU32> {
        let step = self.run_step(rate).await;
        let sustainable = step.sustainable;
        self.ramp.push(step);
        sustainable.then(|| rate.saturating_add(self.cfg.ramp_step_tps.get()))
    }
}
