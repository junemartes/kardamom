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
pub mod engine;
mod feed;
pub mod plan;
pub mod scrape;
mod tracker;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;
use tokio::sync::Semaphore;

use crate::config::{MAX_IN_FLIGHT_SLACK, REQUEST_TIMEOUT};
use crate::load::accounting::{EvalInput, evaluate, print_report, step_gap_ok, step_seq_clean};
use crate::load::engine::{Queues, SubmitMode, Tracker, drain, join_submit_tasks, pacer};
use crate::load::feed::receipt_feed_task;
use crate::load::scrape::Scraper;

pub use config::{ANVIL_MNEMONIC, Completeness, LoadConfig, LoadReport, RampStep, Workload};

/// Parse a `0x`-prefixed JSON-RPC hex quantity into a `u64`.
pub(crate) fn hex_u64(s: &str) -> Option<u64> {
    u64::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

/// Apply [`hex_u64`] to a JSON string field. Returns `None` for a null,
/// missing, or non-string value.
pub(crate) fn json_hex_u64(v: &serde_json::Value) -> Option<u64> {
    v.as_str().and_then(hex_u64)
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
    let ramp_steps = if cfg.chaos_mode || cfg.fixed_rate {
        0
    } else {
        u64::from(cfg.target_tps.div_ceil(cfg.ramp_step_tps.max(1)))
    };
    let total_secs = ramp_steps * cfg.ramp_step_secs + cfg.duration.as_secs();
    let est_total = u64::from(cfg.target_tps) * total_secs;
    ((est_total as f64 * 1.2 / f64::from(cfg.senders.max(1))).ceil() as usize) + 64
}

/// Run the harness. Returns whether the verdict passed.
///
/// # Errors
/// Returns an error on client construction failure, chain-id preflight
/// failure, signer derivation failure, or pre-generation failure. A
/// failing verdict is not an error: this function returns `Ok(false)`
/// so the caller can choose the exit code.
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
    let (queues_vec, defi_deploys) = match cfg.workload {
        Workload::Transfers => (
            plan::pregenerate(
                signers,
                chain_id,
                cfg.to,
                cfg.value,
                per_sender,
                cfg.nonce_start,
                cfg.gas_price,
            )?,
            None,
        ),
        Workload::Defi => {
            let (deploys, contracts) =
                defi::deployment_txs(signers, chain_id, cfg.nonce_start, cfg.gas_price)?;
            tracing::info!(
                pool = %contracts.pool,
                vault = %contracts.vault,
                clob = %contracts.clob,
                "defi workload: deploying bench contracts"
            );
            let queues = defi::pregenerate_defi(
                signers,
                chain_id,
                &contracts,
                per_sender,
                cfg.nonce_start,
                cfg.gas_price,
            )?;
            (queues, Some(deploys))
        }
    };
    let mut queues = Queues::new(queues_vec);

    // DeFi setup: land the three deployments before any load starts.
    // Every workload call targets their computed addresses. A call
    // that arrives before its contract exists would revert and
    // spoil the verdict.
    if let Some(deploys) = defi_deploys {
        defi::deploy_and_confirm(&client, &deploys).await?;
    }

    let scraper = build_scraper(&cfg);
    let tracker = Arc::new(Tracker::new()?);
    let sem = Arc::new(Semaphore::new(cfg.max_in_flight.max(1) as usize));
    let mut tasks = tokio::task::JoinSet::new();
    // Outside chaos mode, the ingress receipt cache is stable, with no
    // restarts. So an accepted transaction whose receipt cannot be
    // re-fetched is a real must-deliver violation, not restart noise.
    // Verify it independently.
    let verify_receipts = !cfg.chaos_mode;
    let mode = if cfg.subscribe {
        SubmitMode::Subscribe
    } else {
        SubmitMode::Blocking
    };

    // Receipts arrive on one multiplexed WebSocket feed, filtered to this
    // run's senders. In subscribe mode, this replaces each submit's
    // parked connection. In blocking mode (feed_confirm), it replaces a
    // per-transaction re-fetch after each accepted submit. The feed runs
    // for the whole ramp and soak, and stops after the drain.
    let feed_confirm = cfg.feed_confirm && !cfg.subscribe;
    let feed = if cfg.subscribe || feed_confirm {
        let ws_url = cfg.rpc.replacen("http", "ws", 1);
        let addrs: Vec<Address> = signers.iter().map(|s| s.address).collect();
        Some(tokio::spawn(receipt_feed_task(
            ws_url,
            addrs,
            Arc::clone(&tracker),
        )))
    } else {
        None
    };
    // Back up the feed with a live sweeper. An entry the feed misses is
    // re-fetched within 2 to 7 seconds, instead of waiting for the
    // end-of-run drain. Keep this cadence well inside the ingress receipt
    // cache's query horizon (capacity divided by rate, about 27 seconds at
    // 4,800 tx/s with the default 128k capacity). Eviction order is
    // arbitrary, so a late poll can miss even a younger entry.
    let sweeper = feed.as_ref().map(|_| {
        engine::spawn_pending_sweeper(
            Arc::clone(&client),
            Arc::clone(&tracker),
            Duration::from_secs(5),
            Duration::from_secs(2),
        )
    });

    // --- ramp (soak mode only) -------------------------------------------
    let mut ramp = Vec::new();
    let discovered_max = if cfg.chaos_mode || cfg.fixed_rate {
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
    let soak_rate = if cfg.chaos_mode || cfg.fixed_rate {
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
        mode,
        feed_confirm,
    )
    .await;

    // Join the in-flight submit tasks, so the tail is classified and not
    // left as merely "offered". Then drain the receipt tail, and wait a
    // short settle time before the final read.
    let deadline = Instant::now() + cfg.drain_timeout;
    join_submit_tasks(&mut tasks, deadline).await;
    drain(Arc::clone(&client), Arc::clone(&tracker), deadline).await;
    if let Some(feed) = feed {
        feed.abort();
    }
    if let Some(sweeper) = sweeper {
        sweeper.abort();
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let fin = scraper.snapshot().await;
    // A chaos-restarted executor's block gauge resets to 0. So
    // `final - base` can be zero or negative while it is replaying in a
    // healthy way. Take a recheck sample a few seconds later, so
    // `evaluate` can tell RECOVERING, where the gauge moves again, from
    // FROZEN.
    let recheck = if cfg.chaos_mode {
        tokio::time::sleep(Duration::from_secs(3)).await;
        Some(scraper.snapshot().await)
    } else {
        None
    };

    // --- verdict ---------------------------------------------------------
    let counts = tracker.counts();
    let (missing, unlanded) = tracker.remaining_pending();
    if missing + unlanded > 0 {
        for (hash, accepted, age) in tracker.sample_pending(32) {
            tracing::warn!(
                %hash,
                accepted,
                age_secs = age.as_secs(),
                "UNRESOLVED pending tx (forensics: query per replica)"
            );
        }
    }
    // In `Offered` mode, an unlanded transaction, one that was offered but
    // never receipted, is also a must-deliver violation. Fold it into
    // `missing` for the gate.
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
        ack_proves_receipt: !cfg.subscribe,
        chaos_mode: cfg.chaos_mode,
    });
    let (p50, p95, p99, max) = tracker.latency_us();

    let report = LoadReport {
        mode: if cfg.chaos_mode {
            "chaos"
        } else if cfg.fixed_rate {
            "fixed"
        } else {
            "soak"
        }
        .to_string(),
        target_tps: cfg.target_tps,
        discovered_max_tps: discovered_max,
        soak_rate_tps: soak_rate,
        duration_secs: cfg.duration.as_secs_f64(),
        ramp,
        lat_p50_us: p50,
        lat_p95_us: p95,
        lat_p99_us: p99,
        lat_max_us: max,
        total_gas: tracker.total_gas(),
        workload: match cfg.workload {
            Workload::Transfers => "transfers".to_string(),
            Workload::Defi => "defi".to_string(),
        },
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
    let mode = if cfg.subscribe {
        SubmitMode::Subscribe
    } else {
        SubmitMode::Blocking
    };
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
            mode,
            cfg.feed_confirm && !cfg.subscribe,
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
        // In subscribe mode, an ack means published, not receipted. So the
        // accept ratio alone would let the ramp go past the pipeline's
        // drain rate, because admission stays at 1.0 while receipts queue
        // up. Require receipts to keep pace with offers within the step,
        // with slack for the in-flight tail at the step boundary.
        #[allow(clippy::cast_precision_loss)]
        let recv_ok = if mode == SubmitMode::Subscribe && offered > 0 {
            let receipted = after.receipted - before.receipted;
            receipted as f64 / offered as f64 >= 0.95
        } else {
            true
        };
        let gap_ok = step_gap_ok(&s0, &s1, cfg.max_gap);
        let seq_clean = step_seq_clean(&s0, &s1);
        let sustainable = accept_ratio >= 0.99 && recv_ok && gap_ok && seq_clean;
        let (lat_p50_us, lat_p95_us, lat_p99_us) = tracker.take_step_latency_us();
        let gas_used = tracker.take_step_gas();
        let mgas_s = gas_used as f64 / 1e6 / cfg.ramp_step_secs.max(1) as f64;
        tracing::info!(
            rate,
            offered,
            accepted,
            accept_ratio = format!("{accept_ratio:.3}"),
            p50_ms = lat_p50_us / 1000,
            p95_ms = lat_p95_us / 1000,
            p99_ms = lat_p99_us / 1000,
            mgas_s = format!("{mgas_s:.1}"),
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
            lat_p50_us,
            lat_p95_us,
            lat_p99_us,
            gas_used,
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
