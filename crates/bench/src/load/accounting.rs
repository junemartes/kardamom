//! Completeness + drop accounting + pass/fail verdict.
//!
//! The authoritative completeness gate is per-tx receipts (`missing` =
//! accepted-but-never-receipted). The Prometheus counters are corroborating
//! diagnostics — there is no native "dropped tx" counter, so ingress drops are
//! *inferred* (received − accepted − rejected − queued) and treated as soft
//! signals (noisy within the in-flight window), while the sequencer's
//! dropped/evicted counters are unambiguous.

use serde::Serialize;

use crate::load::engine::Counts;
use crate::load::scrape::MetricsSnapshot;

/// Per-executor keep-pace evaluation.
#[derive(Debug, Clone, Serialize)]
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
    /// Chaos framing: transient gap / service-down blips and sequencer
    /// past-nonce drops (submit-retry noise across an ingress restart) are
    /// informational (a killed component is expected to be briefly
    /// unavailable); a never-advancing executor (FROZEN) and missing receipts
    /// are still failures.
    pub chaos_mode: bool,
}

/// The harness verdict + computed accounting, serialized into the report.
#[derive(Debug, Clone, Serialize)]
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
        let base_blk = input
            .base
            .executor_blocks
            .iter()
            .find(|(n, _)| n == node)
            .and_then(|(_, b)| *b);
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
            let recheck_blk = input.recheck.and_then(|r| {
                r.executor_blocks
                    .iter()
                    .find(|(n, _)| n == node)
                    .and_then(|(_, b)| *b)
            });
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
        failures.push(format!(
            "{} accepted tx(s) never receipted (must-deliver violated)",
            input.missing
        ));
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
            chaos_mode: true,
        });
        assert!(v.pass, "expected pass, failures: {:?}", v.failures);
        assert_eq!(v.seq_dropped, Some(2), "delta still reported");
    }
}
