//! Per-block shadow grading (spec §P1): predict with stats trained on PRIOR
//! blocks, build the predicted conflict structure, grade it against the
//! block's actual read/write cells — all pure computation, called by the
//! executor's shadow thread once per boundary (and trainable afterwards,
//! keeping the cold-start curve honest: a block never grades against stats
//! that already saw it).
//!
//! Semantics are IDENTICAL to the P0 oracle's holdout grading
//! ([`crate::oracle::analyze`]) — same conflict definition, same wildcard
//! treatment of cold txs, same exclusion boundary (the fee-sink Accumulator
//! cell) — so the live numbers land on the same yardstick as the measured
//! GO verdict.

use std::collections::{BTreeSet, HashSet};

use crate::classifier::Stats;
use crate::oracle::{actual_cells, conflict_pairs, critical_path};
use crate::{Cell, TxObs};

/// One block's shadow verdict.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct BlockGrade {
    /// Txs offered for grading (excludes serial-lane records the caller
    /// never builds obs for).
    pub txs: usize,
    /// Txs actually graded (`min(txs, cap)`; the caller logs truncation).
    pub graded: usize,
    /// Graded txs whose selector had no stats (wildcard: conflicts with
    /// everything — the Tail lane).
    pub cold_txs: usize,
    pub gas: u64,
    /// True direct-conflict pairs among graded txs.
    pub true_edges: usize,
    /// Predicted direct-conflict pairs among graded txs.
    pub predicted_edges: usize,
    /// True-conflicting pairs missed by prediction — the DANGEROUS class
    /// (`footprint_false_independent_total`): these would have run
    /// concurrently and aborted.
    pub missed_pairs: usize,
    /// Predicted-conflicting pairs with no true conflict (over-merge:
    /// forfeited parallelism, the silent pessimism error).
    pub false_pairs: usize,
    /// Wave structure of the PREDICTED DAG: number of levels (a tx's level
    /// is 1 + max of its predicted predecessors') and the widest level.
    pub predicted_waves: usize,
    pub predicted_width: usize,
    /// Gas-weighted critical paths: the predicted schedule's bound and the
    /// oracle's (the number no predictor beats).
    pub predicted_cp_gas: u64,
    pub oracle_cp_gas: u64,
    /// Cell-coverage hit rate inputs, over non-cold graded txs: how many of
    /// the actual (non-excluded) cells the prediction contained.
    pub cells_actual: usize,
    pub cells_hit: usize,
}

impl BlockGrade {
    pub fn hit_rate(&self) -> f64 {
        if self.cells_actual == 0 {
            return 1.0;
        }
        self.cells_hit as f64 / self.cells_actual as f64
    }

    pub fn predicted_cp_ratio(&self) -> f64 {
        if self.predicted_cp_gas == 0 {
            return 1.0;
        }
        self.gas as f64 / self.predicted_cp_gas as f64
    }

    pub fn oracle_cp_ratio(&self) -> f64 {
        if self.oracle_cp_gas == 0 {
            return 1.0;
        }
        self.gas as f64 / self.oracle_cp_gas as f64
    }
}

/// Grade one block. `stats` must NOT have been trained on this block yet.
/// `exclude` is the Accumulator boundary (spec "The graph index" #4): cells
/// serviced by deferred commutative folding never form edges — without
/// excluding the fee sink every block grades 1.00x flat (P0, measured).
/// `cap` bounds the O(n²) pair grading on burst blocks; graded txs are the
/// first `cap` in canonical order and `graded < txs` reports the cut.
pub fn grade_block(
    stats: &Stats,
    txs: &[TxObs],
    exclude: &HashSet<Cell>,
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

    // Predicted cells per tx; cold => wildcard (conflicts with all).
    let mut predicted: Vec<Option<BTreeSet<Cell>>> = Vec::with_capacity(graded.len());
    for o in &graded {
        let p = stats.predict(o);
        if p.is_none() {
            g.cold_txs += 1;
        } else {
            let (reads, writes) = actual_cells(o);
            for c in reads.union(&writes) {
                if exclude.contains(c) {
                    continue;
                }
                g.cells_actual += 1;
                if p.as_ref().is_some_and(|set| set.contains(c)) {
                    g.cells_hit += 1;
                }
            }
        }
        predicted.push(p);
    }

    // Predicted pair set: wildcard txs conflict with everything; otherwise
    // any shared predicted cell conflicts (predictions don't split R/W —
    // conservative, matching the P0 grading).
    let mut pred_pairs: HashSet<(u64, u64)> = HashSet::new();
    for i in 0..graded.len() {
        for j in i + 1..graded.len() {
            let conflict = match (&predicted[i], &predicted[j]) {
                (Some(a), Some(b)) => {
                    let mut inter = a.intersection(b).filter(|c| !exclude.contains(c));
                    inter.next().is_some()
                }
                _ => true, // wildcard
            };
            if conflict {
                pred_pairs.insert((graded[i].index, graded[j].index));
            }
        }
    }

    g.true_edges = true_pairs.len();
    g.predicted_edges = pred_pairs.len();
    g.missed_pairs = true_pairs.difference(&pred_pairs).count();
    g.false_pairs = pred_pairs.difference(&true_pairs).count();
    g.oracle_cp_gas = critical_path(&graded, &true_pairs);
    g.predicted_cp_gas = critical_path(&graded, &pred_pairs);

    // Wave structure of the predicted DAG (canonical order is the
    // topological order: edges only run low index -> high). One pass over
    // edges sorted by source suffices: every edge INTO a node has a smaller
    // source, so a node's level is settled before any edge leaves it.
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
    let waves = level.iter().max().map(|m| m + 1).unwrap_or(0);
    let mut width = vec![0usize; waves];
    for l in &level {
        width[*l] += 1;
    }
    g.predicted_waves = waves;
    g.predicted_width = width.into_iter().max().unwrap_or(0);
    g
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, U256, address};

    const POOL: Address = address!("00000000000000000000000000000000000000F0");
    const SEL: [u8; 4] = [0x12, 0x34, 0x56, 0x78];

    fn obs(index: u64, sender: Address, cells: Vec<Cell>, reads: Vec<Cell>) -> TxObs {
        TxObs {
            index,
            block: 1,
            sender,
            to: Some(POOL),
            selector: Some(SEL),
            args: vec![U256::from(1u64)],
            gas: 100_000,
            has_value: false,
            reads,
            writes: cells,
        }
    }

    fn addr(i: u8) -> Address {
        let mut b = [0u8; 20];
        b[19] = i;
        Address::from(b)
    }

    #[test]
    fn cold_block_serializes_and_misses_nothing() {
        // Everything cold => wildcard => all pairs predicted-conflicting:
        // zero missed pairs (safe), waves == txs (fully serial).
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
        assert_eq!(g.predicted_edges, 6); // complete graph on 4
        assert_eq!(g.true_edges, 6); // all write the same fixed slot
        // 4 chained 100k-gas txs: cp == total.
        assert_eq!(g.predicted_cp_ratio(), 1.0);
    }

    #[test]
    fn trained_stats_split_independent_senders_into_one_wave() {
        // Each tx writes a slot unique to its sender. Those slots are NOT
        // modelled since inversion was removed (they appear once each, far
        // below the fixed threshold), so cell COVERAGE is partial — but
        // the SCHEDULE is unaffected, which is the distinction that
        // matters: unmodelled cells that never collide cost nothing.
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
        // Grade a fresh block of 3 distinct-sender txs.
        let txs: Vec<TxObs> = (10..13).map(|i| mk(i, addr(i as u8 + 20))).collect();
        let g = grade_block(&stats, &txs, &HashSet::new(), 2048);
        assert_eq!(g.cold_txs, 0);
        assert_eq!(g.true_edges, 0);
        assert_eq!(g.predicted_edges, 0);
        assert_eq!(g.missed_pairs, 0);
        assert_eq!(g.false_pairs, 0);
        assert_eq!(g.predicted_waves, 1, "independent txs share one wave");
        assert_eq!(g.predicted_width, 3);
        // Half the actual cells (the per-sender slots) are unmodelled...
        assert_eq!(g.hit_rate(), 0.5);
        // ...and it costs the schedule nothing, because they never
        // collide: no true edge exists for the predictor to miss.
        assert_eq!(g.missed_pairs, 0);
    }

    #[test]
    fn false_independence_is_counted() {
        // Trained on sender-slot-only behavior, then the block ALSO shares
        // a hot fixed slot the training never showed at 60%: prediction
        // says independent, truth says conflict => missed pairs.
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
    fn excluded_fee_sink_forms_no_edges() {
        let sink = Cell::Account(Address::ZERO);
        let txs: Vec<TxObs> = (0..3)
            .map(|i| obs(i, addr(i as u8 + 1), vec![sink], vec![]))
            .collect();
        let mut exclude = HashSet::new();
        exclude.insert(sink);
        // Cold (wildcard) predictions still serialize, but TRUE edges must
        // vanish with the exclusion — the Accumulator boundary.
        let g = grade_block(&Stats::default(), &txs, &exclude, 2048);
        assert_eq!(g.true_edges, 0);
        let g_no_excl = grade_block(&Stats::default(), &txs, &HashSet::new(), 2048);
        assert_eq!(g_no_excl.true_edges, 3);
    }

    #[test]
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
