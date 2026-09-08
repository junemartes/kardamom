//! Oracle dependency analysis: the true conflict graph from actual
//! read and write sets, its gas-weighted critical path, and grading of
//! the classifier's predicted graph against it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::BuildHasher;

use crate::classifier::Stats;
use crate::{Cell, TxObs};

/// One tx's predicted cell set, per [`predicted_pairs`]. `None` is a cold
/// wildcard.
pub(crate) type PredictedCells = Vec<Option<BTreeSet<Cell>>>;

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

/// The canonical `(min, max)` pair key: two tx indices, ordered so the
/// same pair always hashes to the same key regardless of discovery order.
fn pair(a: u64, b: u64) -> (u64, u64) {
    (a.min(b), a.max(b))
}

/// Insert one local tx position's conflicts against the rest of `ws`
/// (or all of `rs`) into `pairs`.
fn insert_pairs_with(txs: &[&TxObs], w: usize, rest: &[usize], pairs: &mut HashSet<(u64, u64)>) {
    for &other in rest {
        if other != w {
            pairs.insert(pair(txs[w].index, txs[other].index));
        }
    }
}

/// A cell-to-touchers index: which local tx positions read (without
/// also writing) or write each cell. Two txs conflict when they touch
/// the same cell and at least one writes it.
#[derive(Default)]
struct CellIndex {
    readers: HashMap<Cell, Vec<usize>>,
    writers: HashMap<Cell, Vec<usize>>,
}

impl CellIndex {
    /// Index tx `i`'s reads and writes, skipping excluded cells (and,
    /// for reads, any cell `i` also writes — a write already implies
    /// the stronger relation).
    fn index_tx<S: BuildHasher>(
        &mut self,
        i: usize,
        reads: BTreeSet<Cell>,
        written: BTreeSet<Cell>,
        exclude: &HashSet<Cell, S>,
    ) {
        for c in reads {
            if !exclude.contains(&c) && !written.contains(&c) {
                self.readers.entry(c).or_default().push(i);
            }
        }
        for c in written {
            if !exclude.contains(&c) {
                self.writers.entry(c).or_default().push(i);
            }
        }
    }

    /// Build the direct-conflict pair set from this index.
    fn pairs(&self, txs: &[&TxObs]) -> HashSet<(u64, u64)> {
        let mut pairs = HashSet::new();
        for (cell, ws) in &self.writers {
            self.insert_cell_pairs(cell, ws, txs, &mut pairs);
        }
        pairs
    }

    /// Insert every conflict pair `cell`'s writers `ws` produce: writer-
    /// writer (against later writers only, so a pair is never inserted
    /// twice) and writer-reader (against every reader of `cell`). The
    /// single loop in [`Self::pairs`] calls this once per written cell,
    /// so that loop stays at one level.
    fn insert_cell_pairs(
        &self,
        cell: &Cell,
        ws: &[usize],
        txs: &[&TxObs],
        pairs: &mut HashSet<(u64, u64)>,
    ) {
        let rs = self.readers.get(cell).map_or([].as_slice(), Vec::as_slice);
        for (a, &w) in ws.iter().enumerate() {
            insert_pairs_with(txs, w, &ws[a + 1..], pairs);
            insert_pairs_with(txs, w, rs, pairs);
        }
    }
}

/// Build the direct-conflict pair set per block, through a cell-to-touchers
/// index. Two txs conflict when they touch the same cell and at least one
/// writes it.
pub(crate) fn conflict_pairs<S: BuildHasher>(
    txs: &[&TxObs],
    cells_of: impl Fn(&TxObs) -> (BTreeSet<Cell>, BTreeSet<Cell>),
    exclude: &HashSet<Cell, S>,
) -> HashSet<(u64, u64)> {
    let mut index = CellIndex::default();
    for (i, o) in txs.iter().enumerate() {
        let (reads, written) = cells_of(o);
        index.index_tx(i, reads, written, exclude);
    }
    index.pairs(txs)
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

/// Per-tx predictions from `stats` (`None` is a cold wildcard), plus how
/// many were cold. [`crate::grade::grade_block`] and [`Holdout::grade`]
/// must compute this identically, to keep the live numbers on the same
/// yardstick as the measured GO verdict — so both build one of these.
pub(crate) struct Predictions {
    pub(crate) cells: PredictedCells,
    pub(crate) cold: usize,
}

impl Predictions {
    /// Predict each tx's cell set with `stats`.
    pub(crate) fn of(stats: &Stats, txs: &[&TxObs]) -> Self {
        let mut cells: PredictedCells = Vec::with_capacity(txs.len());
        let mut cold = 0usize;
        for o in txs {
            let p = stats.predict(o);
            if p.is_none() {
                cold += 1;
            }
            cells.push(p);
        }
        Self { cells, cold }
    }

    /// Build the predicted conflict-pair set: a wildcard conflicts with
    /// everything, otherwise any shared, non-excluded predicted cell is
    /// a conflict (predictions do not split read and write, which is
    /// conservative).
    pub(crate) fn pairs<S: BuildHasher>(
        &self,
        txs: &[&TxObs],
        exclude: &HashSet<Cell, S>,
    ) -> HashSet<(u64, u64)> {
        let mut pairs = HashSet::new();
        for i in 0..txs.len() {
            self.insert_pairs_from(txs, i, exclude, &mut pairs);
        }
        pairs
    }

    /// Insert tx `i`'s predicted conflicts against every later tx into
    /// `pairs`. The single loop in [`Self::pairs`] calls this once per
    /// tx, so that loop stays at one level.
    fn insert_pairs_from<S: BuildHasher>(
        &self,
        txs: &[&TxObs],
        i: usize,
        exclude: &HashSet<Cell, S>,
        pairs: &mut HashSet<(u64, u64)>,
    ) {
        for j in (i + 1)..txs.len() {
            let conflict = match (&self.cells[i], &self.cells[j]) {
                (Some(a), Some(b)) => {
                    let mut inter = a.intersection(b).filter(|c| !exclude.contains(c));
                    inter.next().is_some()
                }
                _ => true, // wildcard
            };
            if conflict {
                pairs.insert(pair(txs[i].index, txs[j].index));
            }
        }
    }
}

/// Identify cells written by more than `threshold` of all txs.
#[must_use]
#[allow(
    clippy::cast_precision_loss,
    reason = "a display share, not a consensus value"
)]
pub fn universal_writes(obs: &[TxObs], threshold: f64) -> Vec<(Cell, f64)> {
    let mut count: HashMap<Cell, u64> = HashMap::new();
    for c in obs.iter().flat_map(|o| &o.writes) {
        *count.entry(*c).or_default() += 1;
    }
    // No `.max(1)` guard needed: when `obs` is empty, `count` is too (the
    // loop above never ran), so the closure below — the only reader of
    // `n` — never executes.
    let n = obs.len() as f64;
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
#[must_use]
pub fn analyze<S: BuildHasher>(
    obs: &[TxObs],
    train_frac: f64,
    exclude: &HashSet<Cell, S>,
) -> Report {
    let mut report = Report {
        universal_write_cells: universal_writes(obs, 0.95),
        ..Default::default()
    };

    let max_block = obs.iter().map(|o| o.block).max().unwrap_or(0);
    report.blocks = Report::per_block_oracle(obs, max_block, exclude);

    let TrainSplit { train, holdout } = Holdout::split(obs, max_block, train_frac);
    if !train.is_empty() && !holdout.is_empty() {
        let mut stats = Stats::default();
        for o in &train {
            stats.learn_obs(o);
        }
        report.grading = Some(holdout.grade(&stats, exclude));
    }
    report
}

impl Report {
    /// Oracle numbers for each block from 1 to `max_block`, skipping any
    /// block with no observations.
    fn per_block_oracle<S: BuildHasher>(
        obs: &[TxObs],
        max_block: u64,
        exclude: &HashSet<Cell, S>,
    ) -> Vec<BlockOracle> {
        let mut blocks = Vec::new();
        for b in 1..=max_block {
            let txs: Vec<&TxObs> = obs.iter().filter(|o| o.block == b).collect();
            if txs.is_empty() {
                continue;
            }
            let pairs = conflict_pairs(&txs, actual_cells, exclude);
            let gas: u64 = txs.iter().map(|o| o.gas).sum();
            blocks.push(BlockOracle {
                block: b,
                txs: txs.len(),
                gas,
                critical_path_gas: critical_path(&txs, &pairs),
                conflict_pairs: pairs.len(),
            });
        }
        blocks
    }
}

/// The result of [`Holdout::split`]: an owned `train` set, and the
/// borrowed [`Holdout`] to grade against once `train` has been learned.
struct TrainSplit<'a> {
    train: Vec<TxObs>,
    holdout: Holdout<'a>,
}

/// The blocks held out from training, with the split point that
/// separates them from `train`: the classifier trains on blocks up to
/// `split` and is graded on these, so a block never grades against
/// stats that already saw it.
struct Holdout<'a> {
    txs: Vec<&'a TxObs>,
    split: u64,
    max_block: u64,
}

impl<'a> Holdout<'a> {
    /// Split `obs` at `train_frac` of `max_block` into an owned `train`
    /// set and a borrowed [`Holdout`].
    fn split(obs: &'a [TxObs], max_block: u64, train_frac: f64) -> TrainSplit<'a> {
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "train_frac is validated by the caller: a NaN, negative, or >1.0 value reads the same as a valid boundary, so no local clamp changes what split computes. max_block is a block count far below f64's exact-integer range, so the ceiling stays non-negative and in 0..=max_block"
        )]
        let split = ((max_block as f64) * train_frac).ceil() as u64;
        let train: Vec<TxObs> = obs.iter().filter(|o| o.block <= split).cloned().collect();
        let txs: Vec<&TxObs> = obs.iter().filter(|o| o.block > split).collect();
        TrainSplit {
            train,
            holdout: Self {
                txs,
                split,
                max_block,
            },
        }
    }

    fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    /// Grade the classifier, trained on `stats`, against each holdout
    /// block: true pairs (from actual cells) against predicted pairs.
    fn grade<S: BuildHasher>(&self, stats: &Stats, exclude: &HashSet<Cell, S>) -> Grading {
        let mut g = Grading {
            holdout_txs: self.txs.len(),
            ..Default::default()
        };
        for b in (self.split + 1)..=self.max_block {
            let txs: Vec<&TxObs> = self.txs.iter().filter(|o| o.block == b).copied().collect();
            if txs.is_empty() {
                continue;
            }
            let true_pairs = conflict_pairs(&txs, actual_cells, exclude);
            let predictions = Predictions::of(stats, &txs);
            let pred_pairs = predictions.pairs(&txs, exclude);

            g.cold_txs += predictions.cold;
            g.true_pairs += true_pairs.len();
            g.predicted_pairs += pred_pairs.len();
            g.missed_pairs += true_pairs.difference(&pred_pairs).count();
            g.false_pairs += pred_pairs.difference(&true_pairs).count();
            g.gas += txs.iter().map(|o| o.gas).sum::<u64>();
            g.oracle_cp_gas += critical_path(&txs, &true_pairs);
            // The predicted graph must be safe for scheduling. A missed
            // pair would abort at runtime, but for the achievable-
            // speedup bound we schedule by predictions alone.
            g.predicted_cp_gas += critical_path(&txs, &pred_pairs);
        }
        g
    }
}

/// Cross-block totals, plus the sorted per-block critical-path ratios
/// [`BlockAgg::percentile`] reads from.
struct BlockAgg {
    gas: u64,
    cp: u64,
    pairs: usize,
    txs: usize,
    ratios: Vec<f64>,
}

impl BlockAgg {
    /// Fold the report's per-block numbers into cross-block totals, and
    /// collect each block's critical-path ratio for the percentile
    /// summary.
    fn from_blocks(blocks: &[BlockOracle]) -> Self {
        let mut agg = blocks.iter().fold(
            Self {
                gas: 0,
                cp: 0,
                pairs: 0,
                txs: 0,
                ratios: Vec::new(),
            },
            |mut acc, b| {
                acc.gas += b.gas;
                acc.cp += b.critical_path_gas;
                acc.pairs += b.conflict_pairs;
                acc.txs += b.txs;
                if b.critical_path_gas > 0 {
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "a display ratio, not a consensus value"
                    )]
                    acc.ratios.push(b.gas as f64 / b.critical_path_gas as f64);
                }
                acc
            },
        );
        agg.ratios.sort_by(f64::total_cmp);
        agg
    }

    /// The `p`-th percentile (`p` in `0.0..=1.0`) of the pre-sorted
    /// ratios.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "p is always caller-supplied and in range, so the index stays in bounds; the cast is a display computation, not a consensus value"
    )]
    fn percentile(&self, p: f64) -> f64 {
        if self.ratios.is_empty() {
            return 0.0;
        }
        self.ratios[((self.ratios.len() - 1) as f64 * p) as usize]
    }
}

impl core::fmt::Display for Grading {
    #[allow(
        clippy::cast_precision_loss,
        reason = "a display ratio, not a consensus value"
    )]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "PREDICTED (holdout {} txs, {} cold): cp-ratio={:.2}x (oracle {:.2}x) miss-pairs={} ({:.4}/tx) over-merge={} ({:.2}% of predicted)",
            self.holdout_txs,
            self.cold_txs,
            if self.predicted_cp_gas > 0 {
                self.gas as f64 / self.predicted_cp_gas as f64
            } else {
                0.0
            },
            if self.oracle_cp_gas > 0 {
                self.gas as f64 / self.oracle_cp_gas as f64
            } else {
                0.0
            },
            self.missed_pairs,
            if self.holdout_txs > 0 {
                self.missed_pairs as f64 / self.holdout_txs as f64
            } else {
                0.0
            },
            self.false_pairs,
            // `predicted_pairs` can be a real 0; the numerator is then 0
            // too, so the guard (not a `.max(1)` clamp) gives the right
            // 0.0%.
            if self.predicted_pairs > 0 {
                self.false_pairs as f64 / self.predicted_pairs as f64 * 100.0
            } else {
                0.0
            },
        )
    }
}

impl Report {
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        reason = "display ratios, not consensus values"
    )]
    pub fn summary(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        let agg = BlockAgg::from_blocks(&self.blocks);
        let _ = writeln!(
            s,
            "blocks={} txs={} gas={:.3}Ggas conflict_pairs={}",
            self.blocks.len(),
            agg.txs,
            agg.gas as f64 / 1e9,
            agg.pairs
        );
        let _ = writeln!(
            s,
            "ORACLE critical-path ratio: agg={:.2}x p10={:.2}x p50={:.2}x p90={:.2}x",
            if agg.cp > 0 {
                agg.gas as f64 / agg.cp as f64
            } else {
                0.0
            },
            agg.percentile(0.10),
            agg.percentile(0.50),
            agg.percentile(0.90),
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
            let _ = writeln!(s, "{g}");
        }
        s
    }
}
