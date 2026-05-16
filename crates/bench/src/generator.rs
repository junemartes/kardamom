//! Open-loop RPC dispatcher.
//!
//! A single `tokio::time::interval(1/rate)` drives request emission. Each
//! tick spawns a task that times one RPC and records the elapsed micros into
//! a per-method `hdrhistogram::Histogram<u64>`. A bounded semaphore (4 ×
//! concurrency permits) caps in-flight: overflow counts as `dropped`, never
//! blocking the dispatcher — open-loop preserved.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use alloy_primitives::{B256, Bytes, TxKind, U256};
use alloy_rpc_types_eth::{BlockNumberOrTag, TransactionRequest};
use hdrhistogram::Histogram;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::HttpClient;
use jsonrpsee::rpc_params;
use tokio::sync::{Mutex, Semaphore};

use crate::config::{Config, Workload};
use crate::report::Counters;
use crate::signers::{self, DerivedSigner};

const HIST_LOWEST_US: u64 = 1;
const HIST_HIGHEST_US: u64 = 60_000_000; // 60s
const HIST_SIGFIGS: u8 = 3;

const SEND_RAW: &str = "eth_sendRawTransaction";
const CALL: &str = "eth_call";

#[derive(Clone)]
enum WorkItem {
    Transfer(Bytes),
    Call,
}

pub struct Outputs {
    pub counters: Counters,
    pub histograms: BTreeMap<String, Histogram<u64>>,
}

/// Preflight checks before the main load phase.
pub async fn preflight(
    client: &HttpClient,
    config: &Config,
    signers: &[DerivedSigner],
) -> anyhow::Result<u64> {
    let chain_id: U256 = client
        .request("eth_chainId", rpc_params![])
        .await
        .map_err(|e| anyhow::anyhow!("eth_chainId failed (is kardamom running?): {e}"))?;
    let chain_id: u64 = chain_id
        .try_into()
        .map_err(|e| anyhow::anyhow!("chain_id overflow: {e}"))?;

    for s in signers {
        let _: U256 = client
            .request(
                "eth_getTransactionCount",
                rpc_params![s.address, BlockNumberOrTag::Latest],
            )
            .await
            .map_err(|e| anyhow::anyhow!("eth_getTransactionCount for {}: {e}", s.address))?;
    }

    if matches!(config.workload, Workload::Calls | Workload::Mixed) {
        let calls_cfg = config
            .calls
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("calls workload requires a [calls] config"))?;
        let req = TransactionRequest {
            to: Some(TxKind::Call(calls_cfg.contract)),
            ..Default::default()
        };
        let result: Result<Bytes, _> = client
            .request("eth_call", rpc_params![req, BlockNumberOrTag::Latest])
            .await;
        match result {
            Ok(b) if !b.is_empty() => {}
            Ok(_) => anyhow::bail!(
                "eth_call to {} returned empty output. Start kardamom with --insert-code {}=0x604260005260206000f3 to provision the bench's expected contract.",
                calls_cfg.contract,
                calls_cfg.contract,
            ),
            Err(e) => anyhow::bail!(
                "eth_call to {} failed: {e}. Start kardamom with --insert-code {}=0x604260005260206000f3 to provision the bench's expected contract.",
                calls_cfg.contract,
                calls_cfg.contract,
            ),
        }
    }

    Ok(chain_id)
}

/// Drive the workload for `config.warmup + config.duration`.
pub async fn run(client: HttpClient, config: Config) -> anyhow::Result<Outputs> {
    let signers = signers::derive(config.seed, config.concurrency as usize)?;

    let chain_id = preflight(&client, &config, &signers).await?;

    let needs_transfers = matches!(config.workload, Workload::Transfers | Workload::Mixed);
    let total_load_secs = config.duration.as_secs_f64() + config.warmup.as_secs_f64();
    let presigned: Vec<Bytes> = if needs_transfers {
        let cycle_len = match config.workload {
            Workload::Transfers => 1.0,
            Workload::Mixed => {
                let total = (config.mix.transfers + config.mix.calls) as f64;
                if total == 0.0 {
                    anyhow::bail!("mix.transfers + mix.calls must be > 0");
                }
                (config.mix.transfers as f64) / total
            }
            Workload::Calls => 0.0,
        };
        let n_transfers =
            ((config.rate as f64) * total_load_secs * cycle_len * 1.5).ceil() as usize;
        let to = alloy_primitives::Address::from([0xBEu8; 20]);
        signers::presign_transfers(&signers, chain_id, to, U256::from(1u64), n_transfers, 0)?
    } else {
        Vec::new()
    };

    let calls_target = config.calls.as_ref().map(|c| c.contract);

    let cycle: Vec<WorkItem> = build_cycle(&config, &presigned);
    if cycle.is_empty() {
        anyhow::bail!("empty workload cycle (check config)");
    }

    // The cycle vector contains *all* pre-signed transfers in order, plus
    // call markers interleaved per the mix ratio. Dispatching by `i % len`
    // re-uses the call marker but each transfer is consumed once. To keep
    // the math simple, we precompute the dispatch index sequence: a
    // round-robin of transfer slots and call slots, then map back.
    let schedule = build_schedule(&config, presigned.len());
    let presigned = Arc::new(presigned);
    let schedule = Arc::new(schedule);

    let sent = Arc::new(AtomicU64::new(0));
    let ok = Arc::new(AtomicU64::new(0));
    let err = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));

    let mut histograms = BTreeMap::new();
    histograms.insert(
        SEND_RAW.to_string(),
        Histogram::<u64>::new_with_bounds(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIGFIGS)?,
    );
    histograms.insert(
        CALL.to_string(),
        Histogram::<u64>::new_with_bounds(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIGFIGS)?,
    );
    let histograms = Arc::new(Mutex::new(histograms));

    let permits = (config.concurrency.max(1) * 4) as usize;
    let semaphore = Arc::new(Semaphore::new(permits));

    let period = Duration::from_secs_f64(1.0 / config.rate.max(1) as f64);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let warmup_end = Instant::now() + config.warmup;
    let deadline = warmup_end + config.duration;

    let mut i: u64 = 0;
    let client = Arc::new(client);

    while Instant::now() < deadline {
        ticker.tick().await;
        let permit = match Arc::clone(&semaphore).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };

        let schedule_idx = (i as usize) % schedule.len();
        let item = match schedule[schedule_idx] {
            ScheduleSlot::Transfer(t_idx) => {
                if t_idx >= presigned.len() {
                    dropped.fetch_add(1, Ordering::Relaxed);
                    drop(permit);
                    i = i.wrapping_add(1);
                    continue;
                }
                WorkItem::Transfer(presigned[t_idx].clone())
            }
            ScheduleSlot::Call => WorkItem::Call,
        };
        i = i.wrapping_add(1);

        sent.fetch_add(1, Ordering::Relaxed);

        let client = Arc::clone(&client);
        let histograms = Arc::clone(&histograms);
        let ok = Arc::clone(&ok);
        let err = Arc::clone(&err);
        let call_target = calls_target;

        tokio::spawn(async move {
            let t0 = Instant::now();
            let (method, result_ok) = match item {
                WorkItem::Transfer(bytes) => {
                    let r: Result<B256, _> = client
                        .request("eth_sendRawTransaction", rpc_params![bytes])
                        .await;
                    (SEND_RAW, r.is_ok())
                }
                WorkItem::Call => {
                    let req = TransactionRequest {
                        to: call_target.map(TxKind::Call),
                        ..Default::default()
                    };
                    let r: Result<Bytes, _> = client
                        .request("eth_call", rpc_params![req, BlockNumberOrTag::Latest])
                        .await;
                    (CALL, r.is_ok())
                }
            };
            let elapsed_us = t0.elapsed().as_micros() as u64;
            if result_ok {
                ok.fetch_add(1, Ordering::Relaxed);
            } else {
                err.fetch_add(1, Ordering::Relaxed);
            }
            if Instant::now() >= warmup_end {
                let mut h = histograms.lock().await;
                if let Some(hist) = h.get_mut(method) {
                    let _ = hist.record(elapsed_us.clamp(HIST_LOWEST_US, HIST_HIGHEST_US));
                }
            }
            drop(permit);
        });
    }

    // Wait for outstanding requests to drain (bounded by permits).
    let total_permits = permits as u32;
    let _ = semaphore.acquire_many(total_permits).await;

    let histograms = Arc::try_unwrap(histograms)
        .map_err(|_| anyhow::anyhow!("histograms still shared at end"))?
        .into_inner();

    Ok(Outputs {
        counters: Counters {
            sent: sent.load(Ordering::Relaxed),
            ok: ok.load(Ordering::Relaxed),
            err: err.load(Ordering::Relaxed),
            dropped: dropped.load(Ordering::Relaxed),
        },
        histograms,
    })
}

#[derive(Clone, Copy, Debug)]
enum ScheduleSlot {
    Transfer(usize),
    Call,
}

fn build_cycle(config: &Config, presigned: &[Bytes]) -> Vec<WorkItem> {
    let _ = presigned;
    match config.workload {
        Workload::Transfers => vec![WorkItem::Transfer(Bytes::new())],
        Workload::Calls => vec![WorkItem::Call],
        Workload::Mixed => {
            let mut v = Vec::with_capacity((config.mix.transfers + config.mix.calls) as usize);
            for _ in 0..config.mix.transfers {
                v.push(WorkItem::Transfer(Bytes::new()));
            }
            for _ in 0..config.mix.calls {
                v.push(WorkItem::Call);
            }
            v
        }
    }
}

fn build_schedule(config: &Config, total_transfers: usize) -> Vec<ScheduleSlot> {
    match config.workload {
        Workload::Transfers => (0..total_transfers).map(ScheduleSlot::Transfer).collect(),
        Workload::Calls => vec![ScheduleSlot::Call; 1],
        Workload::Mixed => {
            let mut out = Vec::new();
            let mut next_transfer = 0usize;
            // Cycle of length transfers + calls.
            // Repeat enough times to cover all transfers and then some.
            let cycle_len = (config.mix.transfers + config.mix.calls).max(1);
            let cycles = total_transfers
                .saturating_mul(cycle_len as usize)
                .saturating_div(config.mix.transfers.max(1) as usize)
                + cycle_len as usize;
            for _ in 0..cycles {
                for _ in 0..config.mix.transfers {
                    out.push(ScheduleSlot::Transfer(next_transfer));
                    next_transfer += 1;
                }
                for _ in 0..config.mix.calls {
                    out.push(ScheduleSlot::Call);
                }
                if next_transfer >= total_transfers && total_transfers > 0 {
                    break;
                }
            }
            if out.is_empty() {
                out.push(ScheduleSlot::Call);
            }
            out
        }
    }
}
