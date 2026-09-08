//! Per-block shadow grading: predict with stats trained on prior
//! blocks, build the predicted conflict structure, and grade it against the
//! block's actual read and write cells. This is pure computation, called by
//! the executor's shadow thread once per boundary (it can train afterwards,
//! so a block never grades against stats that already saw it, which keeps
//! the cold-start curve honest).
//!
//! Semantics match the offline oracle's holdout grading
//! ([`crate::oracle::analyze`]) exactly: same conflict definition, same
//! wildcard treatment of cold txs, same exclusion boundary (the fee-sink
//! Accumulator cell). This keeps the live numbers on the same yardstick as
//! the measured GO verdict.

use std::collections::{BTreeSet, HashSet};

use crate::classifier::Stats;
use crate::oracle::{actual_cells, conflict_pairs, critical_path};
use crate::{Cell, TxObs};

/// One block's shadow verdict.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockGrade {
    /// Txs offered for grading. Excludes serial-lane records the caller
    /// never builds an observation for.
    pub txs: usize,
    /// Txs actually graded (`min(txs, cap)`; the caller logs truncation).
    pub graded: usize,
    /// Graded txs whose selector had no stats (wildcard: conflicts with
    /// everything, the Tail lane).
    pub cold_txs: usize,
    pub gas: u64,
    /// True direct-conflict pairs among graded txs.
    pub true_edges: usize,
    /// Predicted direct-conflict pairs among graded txs.
    pub predicted_edges: usize,
    /// True-conflicting pairs missed by prediction: the dangerous class
    /// (`footprint_false_independent_total`). These would have run
    /// concurrently and aborted.
    pub missed_pairs: usize,
    /// Predicted-conflicting pairs with no true conflict. This is an
    /// over-merge: forfeited parallelism, the silent pessimism error.
    pub false_pairs: usize,
    /// Wave structure of the predicted DAG: the number of levels (a tx's
    /// level is one plus the max of its predicted predecessors') and the
    /// widest level.
    pub predicted_waves: usize,
    pub predicted_width: usize,
    /// Gas-weighted critical paths: the predicted schedule's bound and the
    /// oracle's bound (the number no predictor beats).
    pub predicted_cp_gas: u64,
    pub oracle_cp_gas: u64,
    /// Cell-coverage hit rate inputs, over non-cold graded txs: how many of
    /// the actual (non-excluded) cells the prediction contained.
    pub cells_actual: usize,
    pub cells_hit: usize,
}

impl BlockGrade {
    /// Cell-coverage hit rate. A ratio of small counts: precision loss
    /// from the `f64` cast is below any rate this reports.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        reason = "a ratio of small counts; precision loss from the f64 cast is below any rate this reports"
    )]
    pub fn hit_rate(&self) -> f64 {
        if self.cells_actual == 0 {
            return 1.0;
        }
        self.cells_hit as f64 / self.cells_actual as f64
    }

    /// Gas-weighted critical-path ratio against the predicted schedule.
    /// A ratio of gas counts: precision loss from the `f64` cast is below
    /// any rate this reports.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        reason = "a ratio of gas counts; precision loss from the f64 cast is below any rate this reports"
    )]
    pub fn predicted_cp_ratio(&self) -> f64 {
        if self.predicted_cp_gas == 0 {
            return 1.0;
        }
        self.gas as f64 / self.predicted_cp_gas as f64
    }

    /// Gas-weighted critical-path ratio against the oracle schedule. A
    /// ratio of gas counts: precision loss from the `f64` cast is below
    /// any rate this reports.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        reason = "a ratio of gas counts; precision loss from the f64 cast is below any rate this reports"
    )]
    pub fn oracle_cp_ratio(&self) -> f64 {
        if self.oracle_cp_gas == 0 {
            return 1.0;
        }
        self.gas as f64 / self.oracle_cp_gas as f64
    }

    /// Score each non-cold prediction against its tx's actual cells, into
    /// `self.cells_actual` / `self.cells_hit`. A cold (`None`) prediction
    /// is already counted in `self.cold_txs`.
    fn score_coverage<S: std::hash::BuildHasher>(
        &mut self,
        graded: &[&TxObs],
        predicted: &[Option<BTreeSet<Cell>>],
        exclude: &HashSet<Cell, S>,
    ) {
        for (o, p) in graded.iter().zip(predicted) {
            let Some(set) = p else { continue };
            self.score_tx(o, set, exclude);
        }
    }

    /// Score one non-cold prediction's cell coverage against its tx's
    /// actual cells. [`Self::score_coverage`] calls this once per graded
    /// tx, so that loop stays at one level; this loop is the other.
    fn score_tx<S: std::hash::BuildHasher>(
        &mut self,
        o: &TxObs,
        set: &BTreeSet<Cell>,
        exclude: &HashSet<Cell, S>,
    ) {
        let (reads, writes) = actual_cells(o);
        for c in reads.union(&writes) {
            if exclude.contains(c) {
                continue;
            }
            self.cells_actual += 1;
            if set.contains(c) {
                self.cells_hit += 1;
            }
        }
    }

    /// Wave structure of the predicted DAG (canonical order is the
    /// topological order: edges only run from a low index to a high
    /// index): the number of levels, and the widest level. Sets
    /// `self.predicted_waves` / `self.predicted_width`.
    fn score_waves(&mut self, graded: &[&TxObs], pred_pairs: &HashSet<(u64, u64)>) {
        // One pass over edges sorted by source is enough: every edge into
        // a node has a smaller source, so a node's level is settled
        // before any edge leaves it.
        let pos: std::collections::HashMap<u64, usize> = graded
            .iter()
            .enumerate()
            .map(|(i, o)| (o.index, i))
            .collect();
        let mut edges: Vec<(usize, usize)> = pred_pairs
            .iter()
            .map(|(a, b)| {
                let (ia, ib) = (pos[a], pos[b]);
                if ia < ib { (ia, ib) } else { (ib, ia) }
            })
            .collect();
        edges.sort_unstable();
        let mut level = vec![0usize; graded.len()];
        for (lo, hi) in &edges {
            if level[*lo] + 1 > level[*hi] {
                level[*hi] = level[*lo] + 1;
            }
        }
        self.predicted_waves = level.iter().max().map_or(0, |m| m + 1);
        // A plain histogram: count how many nodes land at each level.
        self.predicted_width = level
            .iter()
            .fold(vec![0usize; self.predicted_waves], |mut w, l| {
                w[*l] += 1;
                w
            })
            .into_iter()
            .max()
            .unwrap_or(0);
    }
}

/// Grade one block. `stats` must not have been trained on this block yet.
/// `exclude` is the Accumulator boundary: cells
/// serviced by deferred commutative folding never form edges. Without
/// excluding the fee sink, every block grades a flat 1.00x (measured
/// offline). `cap` bounds the O(n²) pair grading on burst blocks. Graded txs are
/// the first `cap` in canonical order, and `graded < txs` reports the cut.
#[must_use]
pub fn grade_block<S: std::hash::BuildHasher>(
    stats: &Stats,
    txs: &[TxObs],
    exclude: &HashSet<Cell, S>,
    cap: usize,
) -> BlockGrade {
    let graded: Vec<&TxObs> = txs.iter().take(cap).collect();
    let mut g = BlockGrade {
        txs: txs.len(),
        graded: graded.len(),
        gas: graded.iter().map(|o| o.gas).sum(),
        ..Default::default()
    };
    if graded.is_empty() {
        return g;
    }

    let true_pairs = conflict_pairs(&graded, actual_cells, exclude);
    // Byte-for-byte the same prediction and pair-building oracle::analyze's
    // holdout grading uses, so the live numbers stay on the same yardstick
    // as the measured GO verdict.
    let predictions = crate::oracle::Predictions::of(stats, &graded);
    let pred_pairs = predictions.pairs(&graded, exclude);
    g.cold_txs = predictions.cold;
    g.score_coverage(&graded, &predictions.cells, exclude);

    g.true_edges = true_pairs.len();
    g.predicted_edges = pred_pairs.len();
    g.missed_pairs = true_pairs.difference(&pred_pairs).count();
    g.false_pairs = pred_pairs.difference(&true_pairs).count();
    g.oracle_cp_gas = critical_path(&graded, &true_pairs);
    g.predicted_cp_gas = critical_path(&graded, &pred_pairs);

    g.score_waves(&graded, &pred_pairs);
    g
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::addr;
    use alloy_primitives::{Address, B256, U256, address};

    const POOL: Address = address!("00000000000000000000000000000000000000F0");
    const SEL: [u8; 4] = [0x12, 0x34, 0x56, 0x78];

    fn obs(index: u64, sender: Address, cells: Vec<Cell>, reads: Vec<Cell>) -> TxObs {
        crate::testkit::obs(index, sender, POOL, SEL, cells, reads)
    }

    #[test]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::float_cmp,
        reason = "small loop/range bounds fit u8; ratios here compare exact values from integer inputs, bit-for-bit reproducible"
    )]
    fn cold_block_serializes_and_misses_nothing() {
        // Every tx is cold, so every tx is a wildcard, so every pair is a
        // predicted conflict: zero missed pairs (safe), and the wave count
        // equals the tx count (fully serial).
        let stats = Stats::default();
        let fixed = Cell::Slot(POOL, B256::ZERO);
        let txs: Vec<TxObs> = (0..4)
            .map(|i| obs(i, addr(i as u8 + 1), vec![fixed], vec![]))
            .collect();
        let g = grade_block(&stats, &txs, &HashSet::new(), 2048);
        assert_eq!(g.cold_txs, 4);
        assert_eq!(g.missed_pairs, 0);
        assert_eq!(g.predicted_waves, 4);
        assert_eq!(g.predicted_width, 1);
        assert_eq!(g.predicted_edges, 6); // complete graph on 4 nodes
        assert_eq!(g.true_edges, 6); // all txs write the same fixed slot
        // 4 chained 100k-gas txs: the critical path equals the total gas.
        assert_eq!(g.predicted_cp_ratio(), 1.0);
    }

    #[test]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::float_cmp,
        reason = "small loop/range bounds fit u8; ratios here compare exact values from integer inputs, bit-for-bit reproducible"
    )]
    fn trained_stats_split_independent_senders_into_one_wave() {
        // Each tx writes a slot unique to its sender. Since inversion was
        // removed, these slots are not modelled (each appears once, far
        // below the fixed threshold), so cell coverage is partial. But the
        // schedule is unaffected — this is the key distinction:
        // unmodelled cells that never collide cost nothing.
        use alloy_primitives::keccak256;
        let slot_of = |a: Address| -> B256 {
            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(&U256::from_be_slice(a.as_slice()).to_be_bytes::<32>());
            buf[32..].copy_from_slice(&U256::from(3u8).to_be_bytes::<32>());
            keccak256(buf)
        };
        let mk = |i: u64, a: Address| {
            obs(
                i,
                a,
                vec![Cell::Account(a), Cell::Slot(POOL, slot_of(a))],
                vec![],
            )
        };
        let mut stats = Stats::default();
        for i in 0..4 {
            stats.learn_obs(&mk(i, addr(i as u8 + 1)));
        }
        // Grade a fresh block of three distinct-sender txs.
        let txs: Vec<TxObs> = (10..13).map(|i| mk(i, addr(i as u8 + 20))).collect();
        let g = grade_block(&stats, &txs, &HashSet::new(), 2048);
        assert_eq!(g.cold_txs, 0);
        assert_eq!(g.true_edges, 0);
        assert_eq!(g.predicted_edges, 0);
        assert_eq!(g.missed_pairs, 0);
        assert_eq!(g.false_pairs, 0);
        assert_eq!(g.predicted_waves, 1, "independent txs share one wave");
        assert_eq!(g.predicted_width, 3);
        // Half the actual cells (the per-sender slots) are unmodelled.
        assert_eq!(g.hit_rate(), 0.5);
        // This costs the schedule nothing, because those slots never
        // collide: no true edge exists for the predictor to miss.
        assert_eq!(g.missed_pairs, 0);
    }

    #[test]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::float_cmp,
        reason = "small loop/range bounds fit u8; ratios here compare exact values from integer inputs, bit-for-bit reproducible"
    )]
    fn false_independence_is_counted() {
        // Training saw only sender-slot behavior. The block also shares a
        // hot fixed slot that training never showed at 60%, so the
        // prediction says independent while the truth says conflict. This
        // must count as a missed pair.
        use alloy_primitives::keccak256;
        let slot_of = |a: Address| -> B256 {
            let mut buf = [0u8; 64];
            buf[..32].copy_from_slice(&U256::from_be_slice(a.as_slice()).to_be_bytes::<32>());
            buf[32..].copy_from_slice(&U256::from(3u8).to_be_bytes::<32>());
            keccak256(buf)
        };
        let mut stats = Stats::default();
        for i in 0..4 {
            let a = addr(i as u8 + 1);
            stats.learn_obs(&obs(i, a, vec![Cell::Slot(POOL, slot_of(a))], vec![]));
        }
        let hot = Cell::Slot(POOL, B256::with_last_byte(0xFF));
        let txs: Vec<TxObs> = (10..12)
            .map(|i| {
                let a = addr(i as u8 + 20);
                obs(i, a, vec![Cell::Slot(POOL, slot_of(a)), hot], vec![])
            })
            .collect();
        let g = grade_block(&stats, &txs, &HashSet::new(), 2048);
        assert_eq!(g.true_edges, 1);
        assert_eq!(g.predicted_edges, 0);
        assert_eq!(g.missed_pairs, 1, "the dangerous class must be counted");
        assert!(
            g.hit_rate() < 1.0,
            "the unpredicted hot slot dents coverage"
        );
    }

    #[test]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::float_cmp,
        reason = "small loop/range bounds fit u8; ratios here compare exact values from integer inputs, bit-for-bit reproducible"
    )]
    fn excluded_fee_sink_forms_no_edges() {
        let sink = Cell::Account(Address::ZERO);
        let txs: Vec<TxObs> = (0..3)
            .map(|i| obs(i, addr(i as u8 + 1), vec![sink], vec![]))
            .collect();
        let mut exclude = HashSet::new();
        exclude.insert(sink);
        // Cold (wildcard) predictions still serialize, but true edges must
        // vanish with the exclusion — the Accumulator boundary.
        let g = grade_block(&Stats::default(), &txs, &exclude, 2048);
        assert_eq!(g.true_edges, 0);
        let g_no_excl = grade_block(&Stats::default(), &txs, &HashSet::new(), 2048);
        assert_eq!(g_no_excl.true_edges, 3);
    }

    #[test]
    #[allow(
        clippy::cast_possible_truncation,
        clippy::float_cmp,
        reason = "small loop/range bounds fit u8; ratios here compare exact values from integer inputs, bit-for-bit reproducible"
    )]
    fn cap_truncates_and_reports() {
        let fixed = Cell::Slot(POOL, B256::ZERO);
        let txs: Vec<TxObs> = (0..10)
            .map(|i| obs(i, addr(i as u8 + 1), vec![fixed], vec![]))
            .collect();
        let g = grade_block(&Stats::default(), &txs, &HashSet::new(), 4);
        assert_eq!(g.txs, 10);
        assert_eq!(g.graded, 4);
        assert_eq!(g.true_edges, 6); // complete graph on the graded 4
    }
}
