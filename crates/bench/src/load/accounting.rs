//! Completeness + drop accounting + pass/fail verdict.
//!
//! The authoritative completeness gate is per-tx receipts (`missing` =
//! accepted-but-never-receipted). The Prometheus counters are corroborating
//! diagnostics — there is no native "dropped tx" counter, so ingress drops are
//! *inferred* (received − accepted − rejected − queued) and treated as soft
//! signals (noisy within the in-flight window), while the sequencer's
//! dropped/evicted counters are unambiguous.

use serde::Serialize;

use crate::load::config::LoadReport;
use crate::load::engine::Counts;
use crate::load::scrape::MetricsSnapshot;

/// Per-executor keep-pace evaluation.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct KeepPace {
    /// Executor node-container name.
    pub node: String,
    /// Executor block number at the start of the window.
    pub base: Option<u64>,
    /// Executor block number at the end of the window.
    pub final_block: Option<u64>,
    /// Blocks advanced over the window (`final - base`).
    pub advanced: Option<i64>,
    /// Sealer-minus-executor block gap at the end (clamped ≥ 0).
    pub gap: Option<i64>,
    /// `OK` | `FROZEN` | `RECOVERING` | `GAP>N` | `METRIC-MISSING`.
    pub verdict: String,
}

/// Inputs to [`evaluate`].
pub struct EvalInput<'a> {
    /// Final delivery counts from the tracker.
    pub counts: Counts,
    /// Accepted-but-never-receipted txs (hard durability failures).
    pub missing: u64,
    /// Offered txs whose submit failed and never landed.
    pub unlanded: u64,
    /// Metric snapshot at the start of the measured window.
    pub base: &'a MetricsSnapshot,
    /// Metric snapshot at the end (after a short settle).
    pub fin: &'a MetricsSnapshot,
    /// Optional recheck snapshot taken a few seconds after `fin` (chaos runs):
    /// a restarted executor's block gauge resets to 0 so `final − base` reads
    /// ≤ 0 mid-replay; movement between `fin` and this sample distinguishes
    /// RECOVERING from FROZEN.
    pub recheck: Option<&'a MetricsSnapshot>,
    /// Max allowed sealer-minus-executor block gap.
    pub max_gap: u64,
    /// Fail if any accepted tx is missing a receipt.
    pub assert_all_delivered: bool,
    /// Blocking mode: an accepted `eth_sendRawTransaction` PARKED until its
    /// receipt was observed by the serving ingress, so acceptance itself
    /// proves the receipt existed — an accepted-but-unresolved entry means
    /// the receipt later became UNSERVABLE (evicted from the bounded
    /// ingress cache; the durable copy lives in the executor state DB —
    /// verified by direct mdbx inspection: 5/5 sampled "missing" hashes
    /// present), not undelivered. With this set, `missing` downgrades to a
    /// warning PROVIDED every product drop counter scraped as exactly zero
    /// — any nonzero or unscraped counter keeps the hard failure. Async
    /// (subscribe) mode must NOT set this: its ack proves only publication.
    pub ack_proves_receipt: bool,
    /// Chaos framing: transient gap / service-down blips and sequencer
    /// past-nonce drops (submit-retry noise across an ingress restart) are
    /// informational (a killed component is expected to be briefly
    /// unavailable); a never-advancing executor (FROZEN) and missing receipts
    /// are still failures.
    pub chaos_mode: bool,
}

/// The harness verdict + computed accounting, serialized into the report.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct Verdict {
    /// Overall pass/fail.
    pub pass: bool,
    /// Human-readable failure reasons (empty when `pass`).
    pub failures: Vec<String>,
    /// Submits attempted.
    pub offered: u64,
    /// Submits ingress accepted.
    pub accepted: u64,
    /// Txs confirmed via a receipt.
    pub receipted: u64,
    /// Accepted txs with no receipt (must be 0 under `assert_all_delivered`).
    pub missing: u64,
    /// Offered txs that never landed (submit failed through all retries).
    pub unlanded: u64,
    /// Receipts with a non-`0x1` status.
    pub bad_status: u64,
    /// Inferred ingress drop: `Δreceived − Δaccepted − Δrejected − queue` (soft;
    /// noisy within the in-flight window).
    pub inferred_ingress_drop: Option<i64>,
    /// Sequencer txs dropped for a past nonce over the window (unambiguous).
    pub seq_dropped: Option<i64>,
    /// Sequencer pending-buffer evictions over the window.
    pub seq_evicted: Option<i64>,
    /// Sequencer backpressure events over the window.
    pub seq_backpressure: Option<i64>,
    /// Per-executor keep-pace.
    pub keep_pace: Vec<KeepPace>,
}

fn delta(a: Option<u64>, b: Option<u64>) -> Option<i64> {
    match (a, b) {
        (Some(a), Some(b)) => {
            Some(i64::try_from(b).unwrap_or(i64::MAX) - i64::try_from(a).unwrap_or(i64::MAX))
        }
        _ => None,
    }
}

/// `node`'s block gauge in `snap`, or `None` if it wasn't scraped.
fn executor_block(snap: &MetricsSnapshot, node: &str) -> Option<u64> {
    snap.executor_blocks
        .iter()
        .find(|(n, _)| n == node)
        .and_then(|(_, b)| *b)
}

/// Evaluate the run into a [`Verdict`].
#[must_use]
pub fn evaluate(input: &EvalInput<'_>) -> Verdict {
    let mut failures = Vec::new();
    let c = input.counts;

    // --- keep-pace per executor -------------------------------------------
    let sealer_base = input.base.sealer_block;
    let sealer_fin = input.fin.sealer_block;
    let sealer_adv = delta(sealer_base, sealer_fin).unwrap_or(0);
    let mut keep_pace = Vec::new();
    for (node, fin_blk) in &input.fin.executor_blocks {
        let base_blk = executor_block(input.base, node);
        let advanced = delta(base_blk, *fin_blk);
        let gap = match (sealer_fin, *fin_blk) {
            (Some(s), Some(e)) => Some(
                (i64::try_from(s).unwrap_or(i64::MAX) - i64::try_from(e).unwrap_or(i64::MAX))
                    .max(0),
            ),
            _ => None,
        };
        let mut verdict = "OK".to_string();
        if fin_blk.is_none() {
            verdict = "METRIC-MISSING".to_string();
            if !input.chaos_mode {
                failures.push(format!("executor {node}: block metric unreachable"));
            }
        } else if matches!(advanced, Some(a) if a <= 0) && sealer_adv > 0 {
            // A restarted executor's gauge resets to 0, so `advanced` ≤ 0 can
            // mean "replaying after a kill", not frozen. If the recheck sample
            // shows the gauge moving past `fin`, it's recovering.
            let recheck_blk = input.recheck.and_then(|r| executor_block(r, node));
            if matches!((recheck_blk, *fin_blk), (Some(r), Some(f)) if r > f) {
                verdict = "RECOVERING".to_string();
            } else {
                verdict = "FROZEN".to_string();
                failures.push(format!(
                    "executor {node}: FROZEN (advanced {advanced:?} while sealer advanced {sealer_adv})"
                ));
            }
        } else if matches!(gap, Some(g) if g > i64::try_from(input.max_gap).unwrap_or(i64::MAX)) {
            verdict = format!("GAP>{}", input.max_gap);
            if !input.chaos_mode {
                failures.push(format!("executor {node}: gap {gap:?} > {}", input.max_gap));
            }
        }
        keep_pace.push(KeepPace {
            node: node.clone(),
            base: base_blk,
            final_block: *fin_blk,
            advanced,
            gap,
            verdict,
        });
    }

    // --- drop accounting (diagnostics) ------------------------------------
    let d_received = delta(input.base.ingress_received, input.fin.ingress_received);
    let d_accepted = delta(input.base.ingress_accepted, input.fin.ingress_accepted);
    let d_rejected = delta(input.base.ingress_rejected, input.fin.ingress_rejected);
    let inferred_ingress_drop = match (d_received, d_accepted, d_rejected) {
        (Some(r), Some(a), Some(rj)) => {
            let queued = i64::try_from(input.fin.ingress_queue_depth.unwrap_or(0)).unwrap_or(0);
            Some(r - a - rj - queued)
        }
        _ => None,
    };
    let seq_dropped = delta(input.base.seq_dropped_past, input.fin.seq_dropped_past);
    let seq_evicted = delta(input.base.seq_evictions, input.fin.seq_evictions);
    let seq_backpressure = delta(input.base.seq_backpressure, input.fin.seq_backpressure);

    // --- service liveness at end ------------------------------------------
    if !input.chaos_mode {
        for (svc, up) in &input.fin.service_up {
            if *up == Some(0) {
                failures.push(format!("service {svc}: kardamom_service_up=0 at end"));
            }
        }
    }

    // --- hard gates -------------------------------------------------------
    if c.accepted == 0 {
        failures.push("ingress accepted ZERO txs (pipeline not reachable)".to_string());
    }
    if input.assert_all_delivered && input.missing > 0 {
        // Blocking mode: acceptance proved the receipt existed (the submit
        // parked on it), so if the product counters PROVE nothing was
        // dropped anywhere, an unresolved entry is a serving-layer artifact
        // (bounded ingress cache evicted it before the feed/sweeper could
        // observe it; the durable copy is in the executor state DB).
        // Downgrade to a warning ONLY under that full proof — any nonzero
        // OR UNSCRAPED counter keeps the hard failure.
        let drops_proven_zero = inferred_ingress_drop == Some(0)
            && seq_dropped == Some(0)
            && seq_evicted == Some(0)
            && seq_backpressure == Some(0);
        if input.ack_proves_receipt && drops_proven_zero {
            tracing::warn!(
                unserved = input.missing,
                "accepted receipts unresolved by feed/refetch (ingress cache \
                 eviction); delivery proven by blocking ack + zero drop \
                 counters — not counted as missing"
            );
        } else {
            failures.push(format!(
                "{} accepted tx(s) never receipted (must-deliver violated)",
                input.missing
            ));
        }
    }
    if c.bad_status > 0 {
        failures.push(format!("{} receipt(s) had non-0x1 status", c.bad_status));
    }
    // Unambiguous sequencer drops are a real failure (not just inference
    // noise) — except under chaos, where a submit retried across an ingress
    // restart (volatile dedup cache) can legitimately reach the sequencer
    // twice; the delta stays reported in the verdict as a diagnostic.
    if matches!(seq_dropped, Some(d) if d > 0) && !input.chaos_mode {
        failures.push(format!(
            "sequencer dropped {seq_dropped:?} past-nonce tx(s)"
        ));
    }

    Verdict {
        pass: failures.is_empty(),
        failures,
        offered: c.offered,
        accepted: c.accepted,
        receipted: c.receipted,
        missing: input.missing,
        unlanded: input.unlanded,
        bad_status: c.bad_status,
        inferred_ingress_drop,
        seq_dropped,
        seq_evicted,
        seq_backpressure,
        keep_pace,
    }
}

/// Per-ramp-step version of [`evaluate`]'s keep-pace gate: a cheap boolean
/// over two step-boundary snapshots (frozen / lagging executors), with no
/// restart-recheck and lenient on missing metrics — the full-window verdict
/// with failure reasons stays [`evaluate`]'s job.
pub(crate) fn step_gap_ok(s0: &MetricsSnapshot, s1: &MetricsSnapshot, max_gap: u64) -> bool {
    let sealer_adv = match (s0.sealer_block, s1.sealer_block) {
        (Some(a), Some(b)) => b > a,
        _ => false,
    };
    for (node, b1) in &s1.executor_blocks {
        let b0 = executor_block(s0, node);
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

/// Per-ramp-step version of [`evaluate`]'s sequencer-drop gate: did the
/// drop/eviction counters grow over the step?
pub(crate) fn step_seq_clean(s0: &MetricsSnapshot, s1: &MetricsSnapshot) -> bool {
    let grew = |a: Option<u64>, b: Option<u64>| matches!((a, b), (Some(a), Some(b)) if b > a);
    !grew(s0.seq_dropped_past, s1.seq_dropped_past) && !grew(s0.seq_evictions, s1.seq_evictions)
}

/// Render the report + verdict to stdout.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn print_report(r: &LoadReport) {
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
                "  rate={:<6} accept={:.3} p50={}ms p95={}ms p99={}ms step_mgas={:<8.1} gap_ok={:<5} seq_clean={:<5} {}",
                s.rate,
                s.accept_ratio,
                s.lat_p50_us / 1000,
                s.lat_p95_us / 1000,
                s.lat_p99_us / 1000,
                s.gas_used as f64 / 1e6,
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
    if r.total_gas > 0 && r.duration_secs > 0.0 {
        // total_gas spans ramp + soak; the soak window's own gas is the
        // total minus what the ramp steps drained into their counters.
        let ramp_gas: u64 = r.ramp.iter().map(|s| s.gas_used).sum();
        let soak_gas = r.total_gas.saturating_sub(ramp_gas);
        println!(
            "gas: run_total={:.3} Ggas  soak={:.3} Ggas -> {:.4} Ggas/s ({:.1} Mgas/s) [{}]",
            r.total_gas as f64 / 1e9,
            soak_gas as f64 / 1e9,
            soak_gas as f64 / 1e9 / r.duration_secs,
            soak_gas as f64 / 1e6 / r.duration_secs,
            r.workload,
        );
    }
    println!(
        "offered={}  accepted={}  receipted={}  missing={}  unlanded={}  bad_status={}",
        v.offered, v.accepted, v.receipted, v.missing, v.unlanded, v.bad_status
    );
    println!(
        "drop-accounting: inferred_ingress_drop={:?}  seq_dropped={:?}  seq_evicted={:?}  seq_backpressure={:?}",
        v.inferred_ingress_drop, v.seq_dropped, v.seq_evicted, v.seq_backpressure
    );
    println!(
        "receipt-latency p50={}ms p95={}ms p99={}ms max={}ms",
        r.lat_p50_us / 1000,
        r.lat_p95_us / 1000,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(exec: &[(&str, u64)], sealer: u64) -> MetricsSnapshot {
        MetricsSnapshot {
            executor_blocks: exec
                .iter()
                .map(|(n, b)| ((*n).to_string(), Some(*b)))
                .collect(),
            sealer_block: Some(sealer),
            ..Default::default()
        }
    }

    fn counts(offered: u64, accepted: u64, receipted: u64, bad: u64) -> Counts {
        Counts {
            offered,
            accepted,
            receipted,
            bad_status: bad,
        }
    }

    #[test]
    fn clean_run_passes() {
        let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
        let fin = snap(&[("exec-0", 50), ("exec-1", 49)], 51);
        let v = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: false,
        });
        assert!(v.pass, "expected pass, failures: {:?}", v.failures);
    }

    #[test]
    fn missing_receipt_fails_under_assert() {
        let base = snap(&[("exec-0", 10)], 10);
        let fin = snap(&[("exec-0", 50)], 50);
        let v = evaluate(&EvalInput {
            counts: counts(300, 300, 299, 0),
            missing: 1,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: false,
        });
        assert!(!v.pass);
        assert!(v.failures.iter().any(|f| f.contains("must-deliver")));
    }

    #[test]
    fn frozen_executor_fails_even_in_chaos() {
        let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
        // exec-1 never advanced while the sealer did.
        let fin = snap(&[("exec-0", 50), ("exec-1", 10)], 50);
        let v = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: true,
        });
        assert!(!v.pass);
        assert!(v.failures.iter().any(|f| f.contains("FROZEN")));
    }

    #[test]
    fn gap_is_soft_in_chaos_hard_in_soak() {
        let base = snap(&[("exec-0", 10)], 10);
        // executor advanced but lags the sealer by 40 (> max_gap 5).
        let fin = snap(&[("exec-0", 20)], 60);
        let soak = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: false,
        });
        assert!(!soak.pass, "gap should fail in soak mode");
        let chaos = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: true,
        });
        assert!(
            chaos.pass,
            "gap should be soft in chaos mode: {:?}",
            chaos.failures
        );
    }

    #[test]
    fn restarted_executor_with_moving_recheck_is_recovering_not_frozen() {
        // exec-1 was hard-killed: gauge reset (10 → 3, advanced < 0) but the
        // recheck sample shows it replaying (3 → 8) → RECOVERING, not FROZEN.
        let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
        let fin = snap(&[("exec-0", 50), ("exec-1", 3)], 50);
        let recheck = snap(&[("exec-0", 51), ("exec-1", 8)], 51);
        let v = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: Some(&recheck),
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: true,
        });
        assert!(v.pass, "expected pass, failures: {:?}", v.failures);
        let kp = v.keep_pace.iter().find(|k| k.node == "exec-1").unwrap();
        assert_eq!(kp.verdict, "RECOVERING");
    }

    #[test]
    fn restarted_executor_with_stalled_recheck_is_frozen() {
        // Gauge reset and did NOT move by the recheck sample → still FROZEN.
        let base = snap(&[("exec-0", 10), ("exec-1", 10)], 10);
        let fin = snap(&[("exec-0", 50), ("exec-1", 3)], 50);
        let recheck = snap(&[("exec-0", 51), ("exec-1", 3)], 51);
        let v = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: Some(&recheck),
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: true,
        });
        assert!(!v.pass);
        assert!(v.failures.iter().any(|f| f.contains("FROZEN")));
    }

    #[test]
    fn sequencer_drop_fails() {
        let mut base = snap(&[("exec-0", 10)], 10);
        base.seq_dropped_past = Some(0);
        let mut fin = snap(&[("exec-0", 50)], 50);
        fin.seq_dropped_past = Some(2);
        let v = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: false,
        });
        assert!(!v.pass);
        assert!(v.failures.iter().any(|f| f.contains("sequencer dropped")));
    }

    #[test]
    fn sequencer_drop_is_soft_in_chaos() {
        // Retry noise across an ingress restart can double-submit; the drop
        // delta is reported but must not fail a chaos run.
        let mut base = snap(&[("exec-0", 10)], 10);
        base.seq_dropped_past = Some(0);
        let mut fin = snap(&[("exec-0", 50)], 50);
        fin.seq_dropped_past = Some(2);
        let v = evaluate(&EvalInput {
            counts: counts(300, 300, 300, 0),
            missing: 0,
            unlanded: 0,
            base: &base,
            fin: &fin,
            recheck: None,
            max_gap: 5,
            assert_all_delivered: true,
            ack_proves_receipt: false,
            chaos_mode: true,
        });
        assert!(v.pass, "expected pass, failures: {:?}", v.failures);
        assert_eq!(v.seq_dropped, Some(2), "delta still reported");
    }
}
