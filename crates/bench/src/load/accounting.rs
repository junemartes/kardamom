//! This module does completeness accounting, drop accounting, and the
//! pass-fail verdict.
//!
//! The authoritative completeness gate is per-transaction receipts:
//! `missing` means accepted but never receipted. The Prometheus counters
//! are corroborating diagnostics. There is no native dropped-transaction
//! counter, so the code infers ingress drops as
//! `received - accepted - rejected - queued`, and treats this as a soft
//! signal, since it is noisy within the in-flight window. The
//! sequencer's dropped and evicted counters are unambiguous.

use serde::Serialize;

use crate::load::config::{LoadReport, RampStep};
use crate::load::engine::Counts;
use crate::load::scrape::MetricsSnapshot;

/// The keep-pace evaluation for one executor.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct KeepPace {
    /// The executor node-container name.
    pub node: String,
    /// The executor block number at the start of the window.
    pub base: Option<u64>,
    /// The executor block number at the end of the window.
    pub final_block: Option<u64>,
    /// The blocks advanced over the window: `final - base`.
    pub advanced: Option<i64>,
    /// The sealer-minus-executor block gap at the end. `sealer` and
    /// `executor` are both cumulative block counters, so a negative gap
    /// never arises; `saturating_sub` gives the same clamp-at-0 result
    /// as a signed subtraction would, without the signed type.
    pub gap: Option<u64>,
    /// One of `OK`, `FROZEN`, `RECOVERING`, `GAP>N`, or `METRIC-MISSING`.
    pub verdict: String,
}

/// The inputs to [`evaluate`].
pub(crate) struct EvalInput<'a> {
    /// The final delivery counts from the tracker.
    pub counts: Counts,
    /// The transactions accepted but never receipted. These are hard
    /// durability failures.
    pub missing: u64,
    /// The offered transactions whose submit failed and never landed.
    pub unlanded: u64,
    /// The metric snapshot at the start of the measured window.
    pub base: &'a MetricsSnapshot,
    /// The metric snapshot at the end, after a short settle time.
    pub fin: &'a MetricsSnapshot,
    /// An optional recheck snapshot, taken a few seconds after `fin`,
    /// for chaos runs. A restarted executor's block gauge resets to 0,
    /// so `final - base` can read at or below 0 mid-replay. Movement
    /// between `fin` and this sample tells RECOVERING apart from FROZEN.
    pub recheck: Option<&'a MetricsSnapshot>,
    /// The maximum allowed sealer-minus-executor block gap.
    pub max_gap: u64,
    /// Fail the run if any accepted transaction is missing a receipt.
    pub assert_all_delivered: bool,
    /// In blocking mode, an accepted `eth_sendRawTransaction` parked
    /// until the serving ingress observed its receipt. So acceptance
    /// itself proves the receipt existed. An accepted-but-unresolved
    /// entry then means the receipt later became unservable, for
    /// example evicted from the bounded ingress cache, with the durable
    /// copy still in the executor state database, not that it was
    /// undelivered. With this set, `missing` downgrades to
    /// a warning, but only if every product drop counter scraped as
    /// exactly zero; a nonzero or unscraped counter keeps the hard
    /// failure. Async (subscribe) mode must not set this: its ack
    /// proves only publication, not a receipt.
    pub ack_proves_receipt: bool,
    /// Under the chaos framing, a transient gap or service-down blip,
    /// and a sequencer past-nonce drop from submit-retry noise across
    /// an ingress restart, are informational: a killed component is
    /// expected to be briefly unavailable. A never-advancing executor
    /// (FROZEN) and a missing receipt are still failures.
    pub chaos_mode: bool,
}

/// The harness verdict, with the computed accounting, serialized into
/// the report.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct Verdict {
    /// The overall pass or fail result.
    pub pass: bool,
    /// The human-readable failure reasons. Empty when `pass` is true.
    pub failures: Vec<String>,
    /// The submits attempted.
    pub offered: u64,
    /// The submits ingress accepted.
    pub accepted: u64,
    /// The transactions confirmed by a receipt.
    pub receipted: u64,
    /// The accepted transactions with no receipt. This must be 0 under
    /// `assert_all_delivered`.
    pub missing: u64,
    /// The offered transactions that never landed, because the submit
    /// failed through all retries.
    pub unlanded: u64,
    /// The receipts with a status other than `0x1`.
    pub bad_status: u64,
    /// The inferred ingress drop:
    /// `Δreceived - Δaccepted - Δrejected - queue`. This is a soft
    /// signal, noisy within the in-flight window.
    pub inferred_ingress_drop: Option<i64>,
    /// The sequencer transactions dropped for a past nonce over the
    /// window. This value is unambiguous.
    pub seq_dropped: Option<i64>,
    /// The sequencer pending-buffer evictions over the window.
    pub seq_evicted: Option<i64>,
    /// The sequencer backpressure events over the window.
    pub seq_backpressure: Option<i64>,
    /// The keep-pace result for each executor.
    pub keep_pace: Vec<KeepPace>,
}

/// `b - a`, as a signed delta (a block count can regress across an
/// executor restart, so this cannot use `saturating_sub`). Folds a
/// counter too large for `i64` into `None`, the same channel an
/// unscraped metric uses, instead of clamping it to a sentinel.
fn delta(a: Option<u64>, b: Option<u64>) -> Option<i64> {
    let a = i64::try_from(a?).ok()?;
    let b = i64::try_from(b?).ok()?;
    Some(b - a)
}

/// The block gauge for `node` in `snap`. Returns `None` if the code
/// did not scrape it.
fn executor_block(snap: &MetricsSnapshot, node: &str) -> Option<u64> {
    snap.executor_blocks
        .iter()
        .find(|(n, _)| n == node)
        .and_then(|(_, b)| *b)
}

/// The result of [`keep_pace_rows`]: one row per executor, plus the
/// failure reasons found while building them. A reason names a frozen
/// executor, an unreachable metric, or a gap over `input.max_gap`.
struct KeepPaceResult {
    rows: Vec<KeepPace>,
    failures: Vec<String>,
}

/// One executor's keep-pace verdict. `Display` renders the strings the
/// printed table and the JSON report carry in `KeepPace.verdict`.
enum PaceVerdict {
    Ok,
    MetricMissing,
    Recovering,
    Frozen,
    Gap(u64),
}

impl std::fmt::Display for PaceVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => write!(f, "OK"),
            Self::MetricMissing => write!(f, "METRIC-MISSING"),
            Self::Recovering => write!(f, "RECOVERING"),
            Self::Frozen => write!(f, "FROZEN"),
            Self::Gap(max_gap) => write!(f, "GAP>{max_gap}"),
        }
    }
}

/// The inputs every [`PaceInputs::row`] call shares: the eval input,
/// and the sealer's own final block and advance over the window
/// (computed once, not per executor).
struct PaceInputs<'a> {
    input: &'a EvalInput<'a>,
    sealer_fin: Option<u64>,
    sealer_adv: i64,
}

impl PaceInputs<'_> {
    /// One executor's keep-pace row, and any failure it adds to
    /// `failures` (a frozen executor, an unreachable metric, or a gap
    /// over `input.max_gap`).
    fn row(&self, node: &str, fin_blk: Option<u64>, failures: &mut Vec<String>) -> KeepPace {
        let input = self.input;
        let base_blk = executor_block(input.base, node);
        let advanced = delta(base_blk, fin_blk);
        let gap = match (self.sealer_fin, fin_blk) {
            (Some(s), Some(e)) => Some(s.saturating_sub(e)),
            _ => None,
        };
        let mut verdict = PaceVerdict::Ok;
        if fin_blk.is_none() {
            verdict = PaceVerdict::MetricMissing;
            if !input.chaos_mode {
                failures.push(format!("executor {node}: block metric unreachable"));
            }
        } else if matches!(advanced, Some(a) if a <= 0) && self.sealer_adv > 0 {
            // A restarted executor's gauge resets to 0. So `advanced` at or
            // below 0 can mean "replaying after a kill", not frozen. If the
            // recheck sample shows the gauge moving past `fin`, it is recovering.
            let recheck_blk = input.recheck.and_then(|r| executor_block(r, node));
            if matches!((recheck_blk, fin_blk), (Some(r), Some(f)) if r > f) {
                verdict = PaceVerdict::Recovering;
            } else {
                verdict = PaceVerdict::Frozen;
                let sealer_adv = self.sealer_adv;
                failures.push(format!(
                    "executor {node}: FROZEN (advanced {advanced:?} while sealer advanced {sealer_adv})"
                ));
            }
        } else if matches!(gap, Some(g) if g > input.max_gap) {
            verdict = PaceVerdict::Gap(input.max_gap);
            if !input.chaos_mode {
                failures.push(format!("executor {node}: gap {gap:?} > {}", input.max_gap));
            }
        }
        KeepPace {
            node: node.to_string(),
            base: base_blk,
            final_block: fin_blk,
            advanced,
            gap,
            verdict: verdict.to_string(),
        }
    }
}

/// The keep-pace row for each executor, and the failure reasons found
/// while building them. See [`PaceInputs::row`].
fn keep_pace_rows(input: &EvalInput<'_>) -> KeepPaceResult {
    let mut failures = Vec::new();
    let sealer_fin = input.fin.sealer_block;
    let sealer_adv = delta(input.base.sealer_block, sealer_fin).unwrap_or(0);
    let pace = PaceInputs {
        input,
        sealer_fin,
        sealer_adv,
    };
    let rows = input
        .fin
        .executor_blocks
        .iter()
        .map(|(node, fin_blk)| pace.row(node, *fin_blk, &mut failures))
        .collect();
    KeepPaceResult { rows, failures }
}

/// The drop-accounting diagnostics: the inferred ingress drop, and the
/// sequencer's unambiguous drop, eviction, and backpressure counters.
struct DropAccounting {
    inferred_ingress_drop: Option<i64>,
    seq_dropped: Option<i64>,
    seq_evicted: Option<i64>,
    seq_backpressure: Option<i64>,
}

fn drop_accounting(input: &EvalInput<'_>) -> DropAccounting {
    let d_received = delta(input.base.ingress_received, input.fin.ingress_received);
    let d_accepted = delta(input.base.ingress_accepted, input.fin.ingress_accepted);
    let d_rejected = delta(input.base.ingress_rejected, input.fin.ingress_rejected);
    let inferred_ingress_drop = match (d_received, d_accepted, d_rejected) {
        (Some(r), Some(a), Some(rj)) => {
            let queued = i64::try_from(input.fin.ingress_queue_depth.unwrap_or(0)).unwrap_or(0);
            // This is a soft, noisy signal (see the module doc): the four
            // counters are not read atomically, so the subtraction can run
            // past an `i64` bound in a pathological case. Saturating keeps
            // that a clamp, not a panic, on a diagnostic value.
            Some(
                r.saturating_sub(a)
                    .saturating_sub(rj)
                    .saturating_sub(queued),
            )
        }
        _ => None,
    };
    DropAccounting {
        inferred_ingress_drop,
        seq_dropped: delta(input.base.seq_dropped_past, input.fin.seq_dropped_past),
        seq_evicted: delta(input.base.seq_evictions, input.fin.seq_evictions),
        seq_backpressure: delta(input.base.seq_backpressure, input.fin.seq_backpressure),
    }
}

/// Every service reporting `kardamom_service_up=0` at the end of the
/// window, unless the run is in chaos mode, where a down service is
/// informational.
fn liveness_failures(input: &EvalInput<'_>) -> Vec<String> {
    if input.chaos_mode {
        return Vec::new();
    }
    input
        .fin
        .service_up
        .iter()
        .filter(|(_, up)| *up == Some(0))
        .map(|(svc, _)| format!("service {svc}: kardamom_service_up=0 at end"))
        .collect()
}

/// The hard-gate failures: zero acceptance, an undelivered accepted
/// transaction, a non-`0x1` receipt status, or an unambiguous
/// sequencer drop.
fn completeness_failures(input: &EvalInput<'_>, drops: &DropAccounting) -> Vec<String> {
    let mut failures = Vec::new();
    let c = input.counts;
    if c.accepted == 0 {
        failures.push("ingress accepted ZERO txs (pipeline not reachable)".to_string());
    }
    if input.assert_all_delivered && input.missing > 0 {
        // In blocking mode, acceptance proved the receipt existed, because
        // the submit parked on it. So if the product counters prove nothing
        // was dropped anywhere, an unresolved entry is a serving-layer
        // artifact: the bounded ingress cache evicted it before the feed or
        // sweeper could observe it, and the durable copy is in the executor
        // state database. Downgrade to a warning only under that full proof.
        // A nonzero or unscraped counter keeps the hard failure.
        let drops_proven_zero = drops.inferred_ingress_drop == Some(0)
            && drops.seq_dropped == Some(0)
            && drops.seq_evicted == Some(0)
            && drops.seq_backpressure == Some(0);
        // The eviction explanation covers only a thin tail. If a material
        // fraction of accepted traffic is unresolved, "every counter reads
        // zero" points to a counter blind spot, not cache eviction. That
        // case must stay a hard failure.
        let within_eviction_tail = input.missing <= (c.accepted / 100).max(50);
        if input.ack_proves_receipt && drops_proven_zero && within_eviction_tail {
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
    // An unambiguous sequencer drop is a real failure, not just inference
    // noise, except under chaos. There, a submit retried across an ingress
    // restart can legitimately reach the sequencer twice, because the dedup
    // cache is volatile. The delta still appears in the verdict as a diagnostic.
    if matches!(drops.seq_dropped, Some(d) if d > 0) && !input.chaos_mode {
        failures.push(format!(
            "sequencer dropped {:?} past-nonce tx(s)",
            drops.seq_dropped
        ));
    }
    failures
}

/// Evaluate the run into a [`Verdict`].
#[must_use]
pub(crate) fn evaluate(input: &EvalInput<'_>) -> Verdict {
    let c = input.counts;
    let KeepPaceResult {
        rows: keep_pace,
        mut failures,
    } = keep_pace_rows(input);
    let drops = drop_accounting(input);
    failures.extend(liveness_failures(input));
    failures.extend(completeness_failures(input, &drops));

    Verdict {
        pass: failures.is_empty(),
        failures,
        offered: c.offered,
        accepted: c.accepted,
        receipted: c.receipted,
        missing: input.missing,
        unlanded: input.unlanded,
        bad_status: c.bad_status,
        inferred_ingress_drop: drops.inferred_ingress_drop,
        seq_dropped: drops.seq_dropped,
        seq_evicted: drops.seq_evicted,
        seq_backpressure: drops.seq_backpressure,
        keep_pace,
    }
}

/// A per-ramp-step version of [`evaluate`]'s keep-pace gate. This is a
/// cheap boolean check over two step-boundary snapshots, for a frozen or
/// lagging executor. It does no restart recheck, and is lenient on a
/// missing metric. The full-window verdict, with failure reasons, is
/// still [`evaluate`]'s job.
pub(crate) fn step_gap_ok(s0: &MetricsSnapshot, s1: &MetricsSnapshot, max_gap: u64) -> bool {
    let sealer_adv = match (s0.sealer_block, s1.sealer_block) {
        (Some(a), Some(b)) => b > a,
        _ => false,
    };
    s1.executor_blocks
        .iter()
        .all(|(node, b1)| step_gap_ok_one(s0, node, *b1, s1.sealer_block, sealer_adv, max_gap))
}

/// One executor's [`step_gap_ok`] check. Stays lenient when a metric
/// is missing, since the check cannot run without it.
fn step_gap_ok_one(
    s0: &MetricsSnapshot,
    node: &str,
    b1: Option<u64>,
    sealer_block: Option<u64>,
    sealer_adv: bool,
    max_gap: u64,
) -> bool {
    let Some(b0) = executor_block(s0, node) else {
        return true;
    };
    let Some(b1) = b1 else {
        return true;
    };
    let Some(sealer) = sealer_block else {
        return true;
    };
    if sealer_adv && b1 <= b0 {
        return false; // The executor is frozen.
    }
    sealer.saturating_sub(b1) <= max_gap // The executor is not lagging.
}

/// A per-ramp-step version of [`evaluate`]'s sequencer-drop gate. Checks
/// whether the drop or eviction counters grew over the step.
pub(crate) fn step_seq_clean(s0: &MetricsSnapshot, s1: &MetricsSnapshot) -> bool {
    let grew = |a: Option<u64>, b: Option<u64>| matches!((a, b), (Some(a), Some(b)) if b > a);
    !grew(s0.seq_dropped_past, s1.seq_dropped_past) && !grew(s0.seq_evictions, s1.seq_evictions)
}

/// Render the report and verdict to stdout.
pub(crate) fn print_report(r: &LoadReport) {
    println!(
        "================= KARDAMOM-LOAD ({}) =================",
        r.mode
    );
    println!(
        "target_tps={}  discovered_max={}  soak_rate={}  duration={:.0}s",
        r.target_tps, r.discovered_max_tps, r.soak_rate_tps, r.duration_secs
    );
    print_ramp(&r.ramp);
    print_gas(r);
    print_counts(r);
    print_keep_pace(&r.verdict.keep_pace);
    if r.verdict.pass {
        println!("RESULT: PASS");
    } else {
        println!("RESULT: FAIL");
        r.verdict
            .failures
            .iter()
            .for_each(|f| println!("  FAIL: {f}"));
    }
    println!("=====================================================");
}

/// The per-step ramp table, if the run had a ramp.
#[allow(
    clippy::cast_precision_loss,
    reason = "display-only gas totals stay far under 2^52"
)]
fn print_ramp(ramp: &[RampStep]) {
    if ramp.is_empty() {
        return;
    }
    println!("---- ramp ----");
    for s in ramp {
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
            sustainable_label(s.sustainable)
        );
    }
}

/// The ramp-step verdict word for its wire label.
fn sustainable_label(sustainable: bool) -> &'static str {
    if sustainable {
        "SUSTAINABLE"
    } else {
        "UNSUSTAINABLE"
    }
}

/// The run-total and soak-window gas throughput.
#[allow(
    clippy::cast_precision_loss,
    reason = "display-only gas totals stay far under 2^52"
)]
fn print_gas(r: &LoadReport) {
    if !(r.total_gas > 0 && r.duration_secs > 0.0) {
        return;
    }
    // total_gas spans the ramp and the soak. The soak window's own gas
    // is the total minus what the ramp steps drained into their counters.
    // Saturating: display-only, and a display value should never panic
    // over a report-file quirk (a ramp step recorded twice, say).
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

/// The delivery counts, drop accounting, and receipt latency.
fn print_counts(r: &LoadReport) {
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
        "receipt-latency p50={}ms p95={}ms p99={}ms max={}ms",
        r.lat_p50_us / 1000,
        r.lat_p95_us / 1000,
        r.lat_p99_us / 1000,
        r.lat_max_us / 1000
    );
}

/// One line per node's keep-pace verdict.
fn print_keep_pace(keep_pace: &[KeepPace]) {
    println!("---- keep-pace ----");
    for k in keep_pace {
        println!(
            "  {:<22} base={:?} final={:?} advanced={:?} gap={:?} {}",
            k.node, k.base, k.final_block, k.advanced, k.gap, k.verdict
        );
    }
}

#[cfg(test)]
pub(crate) mod tests;
