//! The mdbx A/B sweep's totals and every printed report: the
//! per-worker-count summary row, the scheduling and allocation
//! breakdown, and the per-block receipt line.

use super::alloc::AllocDelta;
use kardamom_stm::execute::StmOutcome;

/// The average of a per-run total across the flow transactions that
/// produced it. Guards against dividing by zero when a run has no flow
/// blocks.
#[allow(
    clippy::cast_precision_loss,
    reason = "total and flow_txs stay far under 2^52 (the f64 mantissa) for any run this harness measures"
)]
fn avg_per_tx(total: u64, flow_txs: u64) -> f64 {
    if flow_txs == 0 {
        0.0
    } else {
        total as f64 / flow_txs as f64
    }
}

/// The allocator activity accumulated over a sweep, for one side (the
/// sequential engine or the pool).
#[derive(Default)]
pub(crate) struct AllocTotals {
    calls: u64,
    bytes: u64,
    reallocs: u64,
    rebytes: u64,
    buckets: [(u64, u64); 6],
}

impl AllocTotals {
    pub(crate) fn add(&mut self, d: &AllocDelta) {
        self.calls += d.calls;
        self.bytes += d.bytes;
        self.reallocs += d.reallocs;
        self.rebytes += d.rebytes;
        for (t, b) in self.buckets.iter_mut().zip(d.buckets.iter()) {
            t.0 += b.0;
            t.1 += b.1;
        }
    }
}

/// One block's contribution to [`MdbxAbTotals::accumulate`].
pub(crate) struct BlockSample<'a> {
    pub(crate) out: &'a StmOutcome,
    pub(crate) seq_ms: f64,
    pub(crate) stm_ms: f64,
    pub(crate) snap_open_us: u64,
    pub(crate) txs: u64,
    pub(crate) seq_alloc: &'a AllocDelta,
    pub(crate) stm_alloc: &'a AllocDelta,
}

/// The per-worker-count, per-batch totals for one mdbx A/B run. This
/// replaces 30 loose `let mut` accumulators with one struct and one
/// `accumulate` call per block.
#[derive(Default)]
pub(crate) struct MdbxAbTotals {
    seq_ms: f64,
    stm_ms: f64,
    wounds: usize,
    busy: u64,
    span: u64,
    commit: u64,
    feed: u64,
    snap_us: u64,
    rt: u64,
    rmv: u64,
    rbase: u64,
    rback: u64,
    w_own: u64,
    w_foreign: u64,
    fifo_cov: u64,
    fifo_st: u64,
    edges_sum: u64,
    read_us: u64,
    disp_hist: Vec<u64>,
    idle_us: u64,
    bpw: Vec<u64>,
    cold_sum: u64,
    prune_calls_s: u64,
    prune_forced_s: u64,
    prune_us_s: u64,
    steals_s: u64,
    admit_us_s: u64,
    fpre_s: u64,
    fdag_s: u64,
    c_fold: u64,
    c_lane: u64,
    seq: AllocTotals,
    stm: AllocTotals,
    c_hash: u64,
    c_delta: u64,
    evm_us: u64,
    pub_us: u64,
    /// The real flow transaction count. Printed averages divide by
    /// this, never by a fixed constant.
    flow_txs: u64,
}

impl MdbxAbTotals {
    pub(crate) fn new(workers: usize) -> Self {
        Self {
            disp_hist: vec![0u64; workers],
            bpw: vec![0u64; workers],
            ..Self::default()
        }
    }

    pub(crate) fn accumulate(&mut self, sample: &BlockSample<'_>) {
        let BlockSample {
            out,
            seq_ms,
            stm_ms,
            snap_open_us,
            txs,
            seq_alloc,
            stm_alloc,
        } = sample;
        self.seq.add(seq_alloc);
        self.stm.add(stm_alloc);
        self.seq_ms += seq_ms;
        self.stm_ms += stm_ms;
        self.wounds += out.wounds;
        self.busy += out.busy_us;
        self.span += out.parallel_span_us;
        self.commit += out.commit_us;
        self.c_hash += out.commit_hash_us;
        self.c_fold += out.commit_fold_us;
        self.c_lane += out.commit_lane_us;
        self.c_delta += out.commit_delta_us;
        self.feed += out.feed_us;
        self.snap_us += snap_open_us;
        self.idle_us += out.idle_us;
        for (wi, b) in out.busy_per_worker_us.iter().enumerate() {
            self.bpw[wi] += *b / 1000;
        }
        self.prune_calls_s += out.prune_calls;
        self.prune_forced_s += out.prune_forced;
        self.prune_us_s += out.prune_us;
        self.admit_us_s += out.admit_us;
        self.fpre_s += out.feed_pre_us;
        self.fdag_s += out.feed_dag_us;
        self.steals_s += out.steals;
        self.w_own += out.writes_own;
        self.w_foreign += out.writes_foreign;
        self.fifo_cov += out.fifo_covered;
        self.fifo_st += out.fifo_stalls;
        self.edges_sum += out.edges as u64;
        self.cold_sum += out.cold as u64;
        for (wi, c) in out.dispatch.iter().enumerate() {
            self.disp_hist[wi] += u64::from(*c);
        }
        self.rt += out.reads_total;
        self.rmv += out.reads_mv_hit;
        self.rbase += out.reads_base_hit;
        self.rback += out.reads_backend;
        self.evm_us += out.evm_us;
        self.read_us += out.read_us;
        self.pub_us += out.publish_us;
        self.flow_txs += *txs;
    }

    /// Print this worker count's full report: the summary row, and,
    /// when the pool read anything (`self.rt > 0`), the scheduling and
    /// allocation breakdown.
    pub(crate) fn print(&self, wk: usize) {
        self.print_headline(wk);
        if self.rt == 0 {
            return;
        }
        self.print_scheduling_detail(wk);
    }

    /// The one-line-per-worker-count summary row, plus a wound count if any.
    #[allow(
        clippy::cast_precision_loss,
        reason = "millisecond display values: microsecond totals over one sweep stay far under 2^52"
    )]
    fn print_headline(&self, wk: usize) {
        let util = if self.span > 0 {
            self.busy as f64 / (wk as f64 * self.span as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "{:>3} {:>8.0} {:>8.0} {:>7.2}x {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>6.0}%",
            wk,
            self.seq_ms,
            self.stm_ms,
            self.seq_ms / self.stm_ms,
            self.busy as f64 / 1000.0,
            self.span as f64 / 1000.0,
            self.commit as f64 / 1000.0,
            self.feed as f64 / 1000.0,
            self.snap_us as f64 / 1000.0,
            util
        );
        if self.wounds > 0 {
            println!("   (wounds: {})", self.wounds);
        }
    }

    /// The scheduling and allocation breakdown, printed only when the pool
    /// ran at least one read (`self.rt > 0`).
    #[allow(
        clippy::cast_precision_loss,
        reason = "millisecond and percentage display values: microsecond and byte totals over one sweep stay far under 2^52"
    )]
    fn print_scheduling_detail(&self, wk: usize) {
        const LBL: [&str; 6] = ["<64", "<512", "<4K", "<32K", "<256K", "big"];
        let t = self;
        println!(
            "     commit scope {:.1}ms = fold-body {:.1}ms vs lanes-body {:.1}ms (gap = spawn/join) | delta {:.1}ms | rest {:.1}ms",
            t.c_hash as f64 / 1000.0,
            t.c_fold as f64 / 1000.0,
            t.c_lane as f64 / 1000.0,
            t.c_delta as f64 / 1000.0,
            (t.commit as f64 - t.c_hash as f64 - t.c_delta as f64) / 1000.0,
        );
        println!(
            "     evm {:.1}ms (read-path {:.1}ms) | publish {:.1}ms | other {:.1}ms",
            t.evm_us as f64 / 1000.0,
            t.read_us as f64 / 1000.0,
            t.pub_us as f64 / 1000.0,
            (t.busy as f64 - t.evm_us as f64 - t.pub_us as f64) / 1000.0,
        );
        println!("     dispatch per worker: {:?}", t.disp_hist);
        println!("     busy per worker (ms): {:?}", t.bpw);
        println!(
            "     cold {} of {} txs | edges {} | fifo-covered {}",
            t.cold_sum, t.flow_txs, t.edges_sum, t.fifo_cov,
        );
        println!(
            "     feed split: pre(assign+slot+clone) {:.1}ms | dag(last-toucher) {:.1}ms | admit(graph) {:.1}ms | prune(graph) {:.1}ms | feed total {:.1}ms",
            t.fpre_s as f64 / 1000.0,
            t.fdag_s as f64 / 1000.0,
            t.admit_us_s as f64 / 1000.0,
            t.prune_us_s as f64 / 1000.0,
            t.feed as f64 / 1000.0,
        );
        println!(
            "     idle {:.1}ms across {wk} workers (span cap {:.1}ms) | prunes {} (forced {}) {:.1}ms | steals {}",
            t.idle_us as f64 / 1000.0,
            (t.span * wk as u64) as f64 / 1000.0,
            t.prune_calls_s,
            t.prune_forced_s,
            t.prune_us_s as f64 / 1000.0,
            t.steals_s,
        );
        println!(
            "     allocs/tx: seq {:.1} ({:.0} B) | stm {:.1} ({:.0} B)",
            avg_per_tx(t.seq.calls, t.flow_txs),
            avg_per_tx(t.seq.bytes, t.flow_txs),
            avg_per_tx(t.stm.calls, t.flow_txs),
            avg_per_tx(t.stm.bytes, t.flow_txs),
        );
        println!(
            "     reallocs/tx: seq {:.1} ({:.0} B) | stm {:.1} ({:.0} B)",
            avg_per_tx(t.seq.reallocs, t.flow_txs),
            avg_per_tx(t.seq.rebytes, t.flow_txs),
            avg_per_tx(t.stm.reallocs, t.flow_txs),
            avg_per_tx(t.stm.rebytes, t.flow_txs),
        );
        LBL.iter()
            .enumerate()
            .filter(|(i, _)| t.seq.buckets[*i].0 + t.stm.buckets[*i].0 > 0)
            .for_each(|(i, lbl)| {
                println!(
                    "       [{:>5}] seq {:>7.2}/tx {:>8.0}B | stm {:>7.2}/tx {:>8.0}B",
                    lbl,
                    avg_per_tx(t.seq.buckets[i].0, t.flow_txs),
                    avg_per_tx(t.seq.buckets[i].1, t.flow_txs),
                    avg_per_tx(t.stm.buckets[i].0, t.flow_txs),
                    avg_per_tx(t.stm.buckets[i].1, t.flow_txs),
                );
            });
        println!(
            "     chain: edges {} | fifo-covered {} | fifo-stalls {}",
            t.edges_sum, t.fifo_cov, t.fifo_st,
        );
        let w_all = t.w_own + t.w_foreign;
        if w_all == 0 {
            println!(
                "     account writes: none observed (a run with real flow blocks should see some)"
            );
        } else {
            println!(
                "     account writes {w_all} = own-domain {:.1}% | FOREIGN {:.1}%                      (foreign = written by >1 worker)",
                t.w_own as f64 / w_all as f64 * 100.0,
                t.w_foreign as f64 / w_all as f64 * 100.0,
            );
        }
        if t.rt == 0 {
            println!("     reads: none observed (a run with real flow blocks should see some)");
        } else {
            println!(
                "     reads {} = mv-version {:.1}% | base-cache {:.1}% | backend {:.1}%",
                t.rt,
                t.rmv as f64 / t.rt as f64 * 100.0,
                t.rbase as f64 / t.rt as f64 * 100.0,
                t.rback as f64 / t.rt as f64 * 100.0,
            );
        }
    }
}

/// Print one block's receipt summary and the current peak core clock.
/// The busy worker is the boosted core, so the maximum reads close to
/// the frequency the block just ran at.
#[allow(
    clippy::cast_precision_loss,
    reason = "millisecond and MHz display values: microsecond counters from one block stay far under 2^52"
)]
pub(crate) fn print_block_detail(
    block_number: u64,
    seq_receipts: &[kardamom_types::Receipt],
    out: &StmOutcome,
) {
    let total_gas: u64 = seq_receipts.iter().map(|r| r.gas_used).sum();
    let avg_gas = total_gas
        .checked_div(seq_receipts.len() as u64)
        .unwrap_or(0);
    let ok = seq_receipts.iter().filter(|r| r.status).count();
    eprintln!(
        "  block {block_number} receipts: avg gas {avg_gas} ok {ok}/{}",
        seq_receipts.len()
    );
    let mhz: u64 = (0..12)
        .filter_map(|c| {
            std::fs::read_to_string(format!(
                "/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_cur_freq"
            ))
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
        })
        .max()
        .unwrap_or(0)
        / 1000;
    eprintln!(
        "pool block {block_number}: evm {:.1}ms (read {:.1}ms) busy {:.1}ms cpu {mhz}MHz",
        out.evm_us as f64 / 1000.0,
        out.read_us as f64 / 1000.0,
        out.busy_us as f64 / 1000.0,
    );
}

#[cfg(test)]
mod tests {
    use super::avg_per_tx;

    /// The per-transaction average divides by the real flow transaction
    /// count, and reads `0.0` instead of dividing by zero.
    #[test]
    fn avg_per_tx_uses_the_real_count() {
        assert!((avg_per_tx(16_000, 4_000) - 4.0).abs() < f64::EPSILON);
        assert!((avg_per_tx(100, 0) - 0.0).abs() < f64::EPSILON);
    }
}
