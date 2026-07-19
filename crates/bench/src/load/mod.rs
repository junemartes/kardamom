//! `kardamom-load` — open-loop sustained-load + chaos verification harness.
//!
//! Drives a pre-generated transaction stream through ingress at a paced rate,
//! tracks every tx to a receipt (must-deliver), scrapes the cluster's
//! Prometheus metrics for drop/liveness signals, and renders a pass/fail
//! verdict. Two shapes:
//! - **soak** (default): ramp to the sustainable max, then soak at a fraction
//!   of it for `duration`.
//! - **chaos** (`--chaos-mode`): skip the ramp, soak at `target_tps` for
//!   `duration` while an external orchestrator injects failures; transient
//!   gaps/outages are informational, only never-recovering executors and
//!   undelivered receipts fail.

pub mod accounting;
pub mod engine;
pub mod plan;
pub mod scrape;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use serde::Serialize;
use tokio::sync::Semaphore;

use crate::config::{MAX_IN_FLIGHT_SLACK, REQUEST_TIMEOUT};
use crate::load::accounting::{EvalInput, Verdict, evaluate};
use crate::load::engine::{Queues, Tracker, drain, join_submit_tasks, pacer};
use crate::load::scrape::Scraper;

/// Default Anvil/Hardhat test mnemonic (genesis prefunds accounts #0..#15).
pub const ANVIL_MNEMONIC: &str = "test test test test test test test test test test test junk";

/// Which set of txs must be 100% receipted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completeness {
    /// Every tx ingress *accepted* must receipt (chaos-safe: a submit that
    /// fails during an outage is retried, not held against must-deliver).
    Accepted,
    /// Every tx *offered* must receipt (strict; non-chaos soak only).
    Offered,
}

/// Full harness configuration (built by the CLI).
#[derive(Debug, Clone)]
pub struct LoadConfig {
    /// Ingress JSON-RPC URL.
    pub rpc: String,
    /// L2 chain id (probed via `eth_chainId` when `None`).
    pub chain_id: Option<u64>,
    /// Soak duration.
    pub duration: Duration,
    /// Ramp ceiling / chaos-mode fixed rate (tx/s).
    pub target_tps: u32,
    /// Number of sender accounts.
    pub senders: u32,
    /// First account index in the mnemonic table (reserve low accounts).
    pub sender_offset: u32,
    /// Per-sender starting nonce.
    pub nonce_start: u64,
    /// BIP-39 mnemonic the senders derive from.
    pub mnemonic: String,
    /// Transfer sink address.
    pub to: Address,
    /// Wei per transfer.
    pub value: U256,
    /// Legacy gas price (wei).
    pub gas_price: u128,
    /// Max outstanding submits (open-loop back-pressure bound).
    pub max_in_flight: u32,
    /// Max allowed sealer-minus-executor block gap.
    pub max_gap: u64,
    /// How long to keep draining receipts after the send window.
    pub drain_timeout: Duration,
    /// Per-submit retry attempts on transient failure.
    pub retry_submit: u32,
    /// Ramp increment per step (tx/s).
    pub ramp_step_tps: u32,
    /// Seconds held per ramp step.
    pub ramp_step_secs: u64,
    /// Fraction of the discovered max to soak at.
    pub soak_fraction: f64,
    /// Completeness criterion.
    pub completeness: Completeness,
    /// Fail unless completeness is met.
    pub assert_all_delivered: bool,
    /// Chaos framing (skip ramp; tolerate transient blips).
    pub chaos_mode: bool,
    /// Services to scrape.
    pub scrape: Vec<String>,
    /// `docker exec` scrape vs direct.
    pub metrics_via_docker: bool,
    /// Executor node-container names.
    pub executor_nodes: Vec<String>,
    /// Ingress node-container name.
    pub ingress_node: String,
    /// Sequencer node-container names.
    pub sequencer_nodes: Vec<String>,
    /// Optional JSON report path.
    pub output: Option<PathBuf>,
}

/// One ramp step's sustainability evaluation.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct RampStep {
    /// Offered rate for this step (tx/s).
    pub rate: u32,
    /// Accepted / offered over the step.
    pub accept_ratio: f64,
    /// Executors advanced and stayed within `max_gap`.
    pub gap_ok: bool,
    /// No sequencer drops/evictions over the step.
    pub seq_clean: bool,
    /// All three signals held.
    pub sustainable: bool,
}

/// Serialized harness report.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct LoadReport {
    /// `"soak"` or `"chaos"`.
    pub mode: String,
    /// Configured ramp ceiling / chaos rate.
    pub target_tps: u32,
    /// Highest sustainable ramp rate (== target in chaos mode).
    pub discovered_max_tps: u32,
    /// Rate the soak ran at.
    pub soak_rate_tps: u32,
    /// Soak duration in seconds.
    pub duration_secs: f64,
    /// Ramp curve.
    pub ramp: Vec<RampStep>,
    /// Receipt latency p50 (µs).
    pub lat_p50_us: u64,
    /// Receipt latency p99 (µs).
    pub lat_p99_us: u64,
    /// Receipt latency max (µs).
    pub lat_max_us: u64,
    /// Completeness + drop accounting + keep-pace.
    pub verdict: Verdict,
}

async fn preflight_chain_id(client: &HttpClient) -> anyhow::Result<u64> {
    let v: U256 = client
        .request("eth_chainId", rpc_params![])
        .await
        .map_err(|e| {
            anyhow::anyhow!("eth_chainId failed (is ingress up at the --rpc url?): {e}")
        })?;
    v.try_into()
        .map_err(|e| anyhow::anyhow!("chain_id overflow: {e}"))
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

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn per_sender_estimate(cfg: &LoadConfig) -> usize {
    let ramp_steps = if cfg.chaos_mode {
        0
    } else {
        u64::from(cfg.target_tps.div_ceil(cfg.ramp_step_tps.max(1)))
    };
    let total_secs = ramp_steps * cfg.ramp_step_secs + cfg.duration.as_secs();
    let est_total = u64::from(cfg.target_tps) * total_secs;
    ((est_total as f64 * 1.2 / f64::from(cfg.senders.max(1))).ceil() as usize) + 64
}

/// Run the harness; returns whether the verdict passed.
///
/// # Errors
/// Errors on client construction, chain-id preflight, signer derivation, or
/// pre-generation failures. A failing *verdict* is not an error — it returns
/// `Ok(false)` so the caller can choose the exit code.
pub async fn run(cfg: LoadConfig) -> anyhow::Result<bool> {
    let client = Arc::new(
        HttpClientBuilder::default()
            .request_timeout(REQUEST_TIMEOUT)
            .max_concurrent_requests(cfg.max_in_flight as usize + MAX_IN_FLIGHT_SLACK)
            .build(&cfg.rpc)?,
    );

    let chain_id = match cfg.chain_id {
        Some(c) => c,
        None => preflight_chain_id(&client).await?,
    };

    let signers = crate::mnemonic::derive_signers(&cfg.mnemonic, cfg.sender_offset + cfg.senders)?;
    let signers = &signers[cfg.sender_offset as usize..];

    let per_sender = per_sender_estimate(&cfg);
    tracing::info!(
        senders = cfg.senders,
        per_sender,
        target_tps = cfg.target_tps,
        chaos = cfg.chaos_mode,
        "kardamom-load: pre-generating {} txs",
        per_sender * signers.len()
    );
    let queues_vec = plan::pregenerate(
        signers,
        chain_id,
        cfg.to,
        cfg.value,
        per_sender,
        cfg.nonce_start,
        cfg.gas_price,
    )?;
    let mut queues = Queues::new(queues_vec);

    let scraper = build_scraper(&cfg);
    let tracker = Arc::new(Tracker::new()?);
    let sem = Arc::new(Semaphore::new(cfg.max_in_flight.max(1) as usize));
    let mut tasks = tokio::task::JoinSet::new();
    // Outside chaos the ingress receipt cache is stable (no restarts), so an
    // accepted tx whose receipt can't be re-fetched is a real must-deliver
    // violation rather than restart noise — verify independently.
    let verify_receipts = !cfg.chaos_mode;

    // --- ramp (soak mode only) -------------------------------------------
    let mut ramp = Vec::new();
    let discovered_max = if cfg.chaos_mode {
        cfg.target_tps
    } else {
        ramp_to_max(
            &cfg,
            &client,
            &sem,
            &tracker,
            &mut tasks,
            &scraper,
            &mut queues,
            &mut ramp,
        )
        .await
    };

    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let soak_rate = if cfg.chaos_mode {
        cfg.target_tps
    } else {
        ((f64::from(discovered_max) * cfg.soak_fraction).round() as u32)
            .clamp(1, cfg.target_tps.max(1))
    };

    // --- soak ------------------------------------------------------------
    tracing::info!(soak_rate, discovered_max, "kardamom-load: soaking");
    let base = scraper.snapshot().await;
    pacer(
        Arc::clone(&client),
        Arc::clone(&sem),
        Arc::clone(&tracker),
        &mut tasks,
        &mut queues,
        soak_rate,
        cfg.duration,
        cfg.retry_submit,
        verify_receipts,
    )
    .await;

    // Join the in-flight submit tasks (so the tail is classified, not merely
    // "offered"), drain the receipt tail, then a short settle before the
    // final read.
    let deadline = Instant::now() + cfg.drain_timeout;
    join_submit_tasks(&mut tasks, deadline).await;
    drain(Arc::clone(&client), Arc::clone(&tracker), deadline).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let fin = scraper.snapshot().await;
    // A chaos-restarted executor's block gauge resets to 0, so `final − base`
    // can be ≤ 0 while it is healthily replaying. Take a recheck sample a few
    // seconds later so `evaluate` can tell RECOVERING (gauge moving again)
    // from FROZEN.
    let recheck = if cfg.chaos_mode {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Some(scraper.snapshot().await)
    } else {
        None
    };

    // --- verdict ---------------------------------------------------------
    let counts = tracker.counts();
    let (missing, unlanded) = tracker.remaining_pending();
    // In `Offered` mode an unlanded (offered-but-never-receipted) tx is also a
    // must-deliver violation; fold it into `missing` for the gate.
    let missing_gate = if cfg.completeness == Completeness::Offered {
        missing + unlanded
    } else {
        missing
    };
    let verdict = evaluate(&EvalInput {
        counts,
        missing: missing_gate,
        unlanded,
        base: &base,
        fin: &fin,
        recheck: recheck.as_ref(),
        max_gap: cfg.max_gap,
        assert_all_delivered: cfg.assert_all_delivered,
        chaos_mode: cfg.chaos_mode,
    });
    let (p50, p99, max) = tracker.latency_us();

    let report = LoadReport {
        mode: if cfg.chaos_mode { "chaos" } else { "soak" }.to_string(),
        target_tps: cfg.target_tps,
        discovered_max_tps: discovered_max,
        soak_rate_tps: soak_rate,
        duration_secs: cfg.duration.as_secs_f64(),
        ramp,
        lat_p50_us: p50,
        lat_p99_us: p99,
        lat_max_us: max,
        verdict,
    };

    print_report(&report);
    if let Some(path) = &cfg.output {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
        tracing::info!("wrote report to {}", path.display());
    }

    Ok(report.verdict.pass)
}

#[allow(clippy::too_many_arguments)]
async fn ramp_to_max(
    cfg: &LoadConfig,
    client: &Arc<HttpClient>,
    sem: &Arc<Semaphore>,
    tracker: &Arc<Tracker>,
    tasks: &mut tokio::task::JoinSet<()>,
    scraper: &Scraper,
    queues: &mut Queues,
    ramp: &mut Vec<RampStep>,
) -> u32 {
    let step_dur = Duration::from_secs(cfg.ramp_step_secs.max(1));
    let mut discovered = 0u32;
    let mut rate = cfg.ramp_step_tps.max(1);
    while rate <= cfg.target_tps {
        let before = tracker.counts();
        let s0 = scraper.snapshot().await;
        pacer(
            Arc::clone(client),
            Arc::clone(sem),
            Arc::clone(tracker),
            tasks,
            queues,
            rate,
            step_dur,
            cfg.retry_submit,
            !cfg.chaos_mode,
        )
        .await;
        let after = tracker.counts();
        let s1 = scraper.snapshot().await;

        let offered = after.offered - before.offered;
        let accepted = after.accepted - before.accepted;
        #[allow(clippy::cast_precision_loss)]
        let accept_ratio = if offered > 0 {
            accepted as f64 / offered as f64
        } else {
            0.0
        };
        let gap_ok = step_gap_ok(&s0, &s1, cfg.max_gap);
        let seq_clean = step_seq_clean(&s0, &s1);
        let sustainable = accept_ratio >= 0.99 && gap_ok && seq_clean;
        tracing::info!(
            rate,
            offered,
            accepted,
            accept_ratio = format!("{accept_ratio:.3}"),
            gap_ok,
            seq_clean,
            sustainable,
            "ramp step"
        );
        ramp.push(RampStep {
            rate,
            accept_ratio,
            gap_ok,
            seq_clean,
            sustainable,
        });
        if sustainable {
            discovered = rate;
        } else {
            break;
        }
        rate = rate.saturating_add(cfg.ramp_step_tps.max(1));
    }
    discovered.max(cfg.ramp_step_tps.max(1))
}

fn step_gap_ok(s0: &scrape::MetricsSnapshot, s1: &scrape::MetricsSnapshot, max_gap: u64) -> bool {
    let sealer_adv = match (s0.sealer_block, s1.sealer_block) {
        (Some(a), Some(b)) => b > a,
        _ => false,
    };
    for (node, b1) in &s1.executor_blocks {
        let b0 = s0
            .executor_blocks
            .iter()
            .find(|(n, _)| n == node)
            .and_then(|(_, b)| *b);
        // Missing metric → can't assert, stay lenient.
        if let (Some(b0), Some(b1), Some(sealer)) = (b0, *b1, s1.sealer_block) {
            if sealer_adv && b1 <= b0 {
                return false; // frozen
            }
            if sealer.saturating_sub(b1) > max_gap {
                return false; // lagging
            }
        }
    }
    true
}

fn step_seq_clean(s0: &scrape::MetricsSnapshot, s1: &scrape::MetricsSnapshot) -> bool {
    let grew = |a: Option<u64>, b: Option<u64>| matches!((a, b), (Some(a), Some(b)) if b > a);
    !grew(s0.seq_dropped_past, s1.seq_dropped_past) && !grew(s0.seq_evictions, s1.seq_evictions)
}

#[allow(clippy::cast_precision_loss)]
fn print_report(r: &LoadReport) {
    println!(
        "================= KARDAMOM-LOAD ({}) =================",
        r.mode
    );
    println!(
        "target_tps={}  discovered_max={}  soak_rate={}  duration={:.0}s",
        r.target_tps, r.discovered_max_tps, r.soak_rate_tps, r.duration_secs
    );
    if !r.ramp.is_empty() {
        println!("---- ramp ----");
        for s in &r.ramp {
            println!(
                "  rate={:<6} accept={:.3} gap_ok={:<5} seq_clean={:<5} {}",
                s.rate,
                s.accept_ratio,
                s.gap_ok,
                s.seq_clean,
                if s.sustainable {
                    "SUSTAINABLE"
                } else {
                    "UNSUSTAINABLE"
                }
            );
        }
    }
    let v = &r.verdict;
    println!(
        "offered={}  accepted={}  receipted={}  missing={}  unlanded={}  bad_status={}",
        v.offered, v.accepted, v.receipted, v.missing, v.unlanded, v.bad_status
    );
    println!(
        "drop-accounting: inferred_ingress_drop={:?}  seq_dropped={:?}  seq_evicted={:?}  seq_backpressure={:?}",
        v.inferred_ingress_drop, v.seq_dropped, v.seq_evicted, v.seq_backpressure
    );
    println!(
        "receipt-latency p50={}ms p99={}ms max={}ms",
        r.lat_p50_us / 1000,
        r.lat_p99_us / 1000,
        r.lat_max_us / 1000
    );
    println!("---- keep-pace ----");
    for k in &v.keep_pace {
        println!(
            "  {:<22} base={:?} final={:?} advanced={:?} gap={:?} {}",
            k.node, k.base, k.final_block, k.advanced, k.gap, k.verdict
        );
    }
    if v.pass {
        println!("RESULT: PASS");
    } else {
        println!("RESULT: FAIL");
        for f in &v.failures {
            println!("  FAIL: {f}");
        }
    }
    println!("=====================================================");
}
