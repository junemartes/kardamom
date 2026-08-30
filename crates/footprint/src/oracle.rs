//! Oracle dependency analysis: the true conflict graph from actual
//! read and write sets, its gas-weighted critical path, and grading of
//! the classifier's predicted graph against it.

use std::collections::{BTreeSet, HashMap, HashSet};

use crate::classifier::Stats;
use crate::{Cell, TxObs};

/// Per-block oracle numbers.
#[derive(Debug, Clone)]
pub struct BlockOracle {
    pub block: u64,
    pub txs: usize,
    pub gas: u64,
    /// Gas of the longest true-dependency chain.
    pub critical_path_gas: u64,
    /// Direct conflicting pairs (share at least one cell, with a write on
    /// either side).
    pub conflict_pairs: usize,
}

/// Cross-block aggregates for the report.
#[derive(Debug, Default)]
pub struct Report {
    pub blocks: Vec<BlockOracle>,
    /// Cells written by more than 95% of txs (fee-sink candidates).
    pub universal_write_cells: Vec<(Cell, f64)>,
    /// Predicted-graph grading (holdout only).
    pub grading: Option<Grading>,
}

#[derive(Debug, Default, Clone)]
pub struct Grading {
    pub holdout_txs: usize,
    pub cold_txs: usize,
    /// True-conflicting direct pairs missed by prediction. This is the
    /// dangerous direction: these would have run concurrently and aborted.
    pub missed_pairs: usize,
    /// Predicted-conflicting pairs with no true conflict. This is an
    /// over-merge: forfeited parallelism.
    pub false_pairs: usize,
    pub true_pairs: usize,
    pub predicted_pairs: usize,
    /// Critical-path gas of the predicted graph (chains follow predicted
    /// conflicts plus Tail: cold txs serialize behind everything).
    pub predicted_cp_gas: u64,
    pub oracle_cp_gas: u64,
    pub gas: u64,
}

/// Build the direct-conflict pair set per block, through a cell-to-touchers
/// index. Two txs conflict when they touch the same cell and at least one
/// writes it.
pub(crate) fn conflict_pairs(
    txs: &[&TxObs],
    cells_of: impl Fn(&TxObs) -> (BTreeSet<Cell>, BTreeSet<Cell>),
    exclude: &HashSet<Cell>,
) -> HashSet<(u64, u64)> {
    // Map each cell to its readers and writers, as local tx positions.
    let mut readers: HashMap<Cell, Vec<usize>> = HashMap::new();
    let mut writers: HashMap<Cell, Vec<usize>> = HashMap::new();
    for (i, o) in txs.iter().enumerate() {
        let (reads, writes) = cells_of(o);
        for c in reads {
            if !exclude.contains(&c) && !writes.contains(&c) {
                readers.entry(c).or_default().push(i);
            }
        }
        for c in writes {
            if !exclude.contains(&c) {
                writers.entry(c).or_default().push(i);
            }
        }
    }
    let mut pairs = HashSet::new();
    for (cell, ws) in &writers {
        // writer-writer
        for a in 0..ws.len() {
            for b in a + 1..ws.len() {
                pairs.insert((
                    txs[ws[a]].index.min(txs[ws[b]].index),
                    txs[ws[a]].index.max(txs[ws[b]].index),
                ));
            }
        }
        // writer-reader
        if let Some(rs) = readers.get(cell) {
            for w in ws {
                for r in rs {
                    if w != r {
                        pairs.insert((
                            txs[*w].index.min(txs[*r].index),
                            txs[*w].index.max(txs[*r].index),
                        ));
                    }
                }
            }
        }
    }
    pairs
}

/// Gas-weighted critical path over a direct-conflict pair set (canonical
/// order is the topological order: edges only run from a low index to a
/// high index).
pub(crate) fn critical_path(txs: &[&TxObs], pairs: &HashSet<(u64, u64)>) -> u64 {
    let pos: HashMap<u64, usize> = txs.iter().enumerate().map(|(i, o)| (o.index, i)).collect();
    let mut preds: Vec<Vec<usize>> = vec![Vec::new(); txs.len()];
    for (a, b) in pairs {
        if let (Some(&ia), Some(&ib)) = (pos.get(a), pos.get(b)) {
            preds[ib].push(ia);
        }
    }
    let mut cp = vec![0u64; txs.len()];
    for i in 0..txs.len() {
        let best = preds[i].iter().map(|p| cp[*p]).max().unwrap_or(0);
        cp[i] = best + txs[i].gas;
    }
    cp.into_iter().max().unwrap_or(0)
}

pub(crate) fn actual_cells(o: &TxObs) -> (BTreeSet<Cell>, BTreeSet<Cell>) {
    (
        o.reads.iter().copied().collect(),
        o.writes.iter().copied().collect(),
    )
}

/// Identify cells written by more than `threshold` of all txs.
pub fn universal_writes(obs: &[TxObs], threshold: f64) -> Vec<(Cell, f64)> {
    let mut count: HashMap<Cell, u64> = HashMap::new();
    for o in obs {
        for c in &o.writes {
            *count.entry(*c).or_default() += 1;
        }
    }
    let n = obs.len().max(1) as f64;
    let mut v: Vec<(Cell, f64)> = count
        .into_iter()
        .filter_map(|(c, k)| {
            let share = k as f64 / n;
            (share > threshold).then_some((c, share))
        })
        .collect();
    v.sort_by(|a, b| b.1.total_cmp(&a.1));
    v
}

/// Full analysis: oracle per block over ALL blocks; classifier trained on
/// the first `train_frac` of blocks and graded on the rest.
pub fn analyze(obs: &[TxObs], train_frac: f64, exclude: &HashSet<Cell>) -> Report {
    let mut report = Report {
        universal_write_cells: universal_writes(obs, 0.95),
        ..Default::default()
    };

    let max_block = obs.iter().map(|o| o.block).max().unwrap_or(0);
    for b in 1..=max_block {
        let txs: Vec<&TxObs> = obs.iter().filter(|o| o.block == b).collect();
        if txs.is_empty() {
            continue;
        }
        let pairs = conflict_pairs(&txs, actual_cells, exclude);
        let gas: u64 = txs.iter().map(|o| o.gas).sum();
        report.blocks.push(BlockOracle {
            block: b,
            txs: txs.len(),
            gas,
            critical_path_gas: critical_path(&txs, &pairs),
            conflict_pairs: pairs.len(),
        });
    }

    // Train and holdout grading.
    let split = ((max_block as f64) * train_frac).ceil() as u64;
    let train: Vec<TxObs> = obs.iter().filter(|o| o.block <= split).cloned().collect();
    let holdout: Vec<&TxObs> = obs.iter().filter(|o| o.block > split).collect();
    if !train.is_empty() && !holdout.is_empty() {
        let mut stats = Stats::default();
        for o in &train {
            stats.learn_obs(o);
        }
        let mut g = Grading {
            holdout_txs: holdout.len(),
            ..Default::default()
        };
        // Per holdout block: true pairs against predicted pairs.
        for b in (split + 1)..=max_block {
            let txs: Vec<&TxObs> = holdout.iter().filter(|o| o.block == b).copied().collect();
            if txs.is_empty() {
                continue;
            }
            let true_pairs = conflict_pairs(&txs, actual_cells, exclude);

            // Predicted cells per tx. A cold tx is a wildcard: it conflicts
            // with all.
            let mut predicted: Vec<Option<BTreeSet<Cell>>> = Vec::with_capacity(txs.len());
            for o in &txs {
                let p = stats.predict(o);
                if p.is_none() {
                    g.cold_txs += 1;
                }
                predicted.push(p);
            }
            // Predicted pair set: a wildcard tx conflicts with everything.
            // Otherwise, any shared predicted cell counts as a conflict
            // (predictions do not yet split read and write, which is
            // conservative).
            let mut pred_pairs: HashSet<(u64, u64)> = HashSet::new();
            for i in 0..txs.len() {
                for j in i + 1..txs.len() {
                    let key = (txs[i].index, txs[j].index);
                    let conflict = match (&predicted[i], &predicted[j]) {
                        (Some(a), Some(b)) => {
                            let mut inter = a.intersection(b).filter(|c| !exclude.contains(c));
                            inter.next().is_some()
                        }
                        _ => true, // wildcard
                    };
                    if conflict {
                        pred_pairs.insert(key);
                    }
                }
            }
            g.true_pairs += true_pairs.len();
            g.predicted_pairs += pred_pairs.len();
            g.missed_pairs += true_pairs.difference(&pred_pairs).count();
            g.false_pairs += pred_pairs.difference(&true_pairs).count();
            g.gas += txs.iter().map(|o| o.gas).sum::<u64>();
            g.oracle_cp_gas += critical_path(&txs, &true_pairs);
            // The predicted graph must be safe for scheduling. A missed
            // pair would abort at runtime, but for the achievable-speedup
            // bound we schedule by predictions alone.
            g.predicted_cp_gas += critical_path(&txs, &pred_pairs);
        }
        report.grading = Some(g);
    }
    report
}

impl Report {
    pub fn summary(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        let (mut gas, mut cp, mut pairs, mut txs) = (0u64, 0u64, 0usize, 0usize);
        let mut ratios: Vec<f64> = Vec::new();
        for b in &self.blocks {
            gas += b.gas;
            cp += b.critical_path_gas;
            pairs += b.conflict_pairs;
            txs += b.txs;
            if b.critical_path_gas > 0 {
                ratios.push(b.gas as f64 / b.critical_path_gas as f64);
            }
        }
        ratios.sort_by(|a, b| a.total_cmp(b));
        let pct = |p: f64| -> f64 {
            if ratios.is_empty() {
                return 0.0;
            }
            ratios[((ratios.len() - 1) as f64 * p) as usize]
        };
        let _ = writeln!(
            s,
            "blocks={} txs={} gas={:.3}Ggas conflict_pairs={}",
            self.blocks.len(),
            txs,
            gas as f64 / 1e9,
            pairs
        );
        let _ = writeln!(
            s,
            "ORACLE critical-path ratio: agg={:.2}x p10={:.2}x p50={:.2}x p90={:.2}x",
            if cp > 0 { gas as f64 / cp as f64 } else { 0.0 },
            pct(0.10),
            pct(0.50),
            pct(0.90),
        );
        for (c, share) in self.universal_write_cells.iter().take(5) {
            let _ = writeln!(
                s,
                "UNIVERSAL write cell ({:.0}% of txs): {:?}",
                share * 100.0,
                c
            );
        }
        if let Some(g) = &self.grading {
            let _ = writeln!(
                s,
                "PREDICTED (holdout {} txs, {} cold): cp-ratio={:.2}x (oracle {:.2}x) miss-pairs={} ({:.4}/tx) over-merge={} ({:.2}% of predicted)",
                g.holdout_txs,
                g.cold_txs,
                if g.predicted_cp_gas > 0 {
                    g.gas as f64 / g.predicted_cp_gas as f64
                } else {
                    0.0
                },
                if g.oracle_cp_gas > 0 {
                    g.gas as f64 / g.oracle_cp_gas as f64
                } else {
                    0.0
                },
                g.missed_pairs,
                g.missed_pairs as f64 / g.holdout_txs.max(1) as f64,
                g.false_pairs,
                g.false_pairs as f64 / g.predicted_pairs.max(1) as f64 * 100.0,
            );
        }
        s
    }
}
