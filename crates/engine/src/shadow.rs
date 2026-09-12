//! Footprint shadow scheduler.
//!
//! At each boundary, the exec thread sends this thread the block's per-tx
//! captures: envelope clone, gas, `TouchSet` reads, and `WriteSet`-derived
//! write cells. It uses a bounded `try_send` channel, the same as the BAL
//! publisher handoff. The shadow must never back-pressure execution. A
//! dropped block costs one block of measurement, not the chain. All heavy
//! work (envelope decode, prediction, O(pairs) grading, training) happens
//! here, off the hot path.
//!
//! Per block, in order:
//!   1. Predict, using stats trained only on prior blocks.
//!   2. Grade the prediction against the block's actual cells
//!      ([`kardamom_footprint::grade::grade_block`], with offline-identical
//!      semantics).
//!   3. Emit metrics and a summary log line.
//!   4. Train on the block.
//!
//! A block never grades against stats that already saw it. So the
//! cold-start curve in the emitted series is the real no-persistence cost
//! the spec wants to price.
//!
//! Execution stays sequential. Nothing here feeds back into scheduling.
//! The executor role enables this with `KARDAMOM_FOOTPRINT_SHADOW=1`.

use std::collections::HashSet;

use alloy_primitives::Address;
use crossbeam_channel::{Receiver, Sender, bounded};
use kardamom_exec_core::delta::WriteSet;
use kardamom_exec_core::executor::TouchSet;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::grade::grade_block;
use kardamom_footprint::{Cell, TxObs, envelope_view};
use kardamom_types::TxEnvelope;

/// The fee sink. Every tx credits it (a universal write cell). The
/// `Accumulator` strategy services it with deferred commutative folding.
/// So conflict analysis excludes it. This
/// mirrors `kardamom_exec_core::block_env`: beneficiary = address(0),
/// basefee = 0, the documented V0 burn.
pub(crate) const FEE_SINK: Address = Address::ZERO;

/// Pair-grading cap per block. Grading does O(n²) set intersections. CI-scale
/// blocks have at most ~600 txs; saturated dev-host blocks have ~2,700. This
/// cap stops a burst block from wedging the shadow thread for seconds.
/// Truncation is always reported (`graded < txs` in the summary line).
const GRADE_CAP: usize = 2_048;

/// One executed tx's capture. The exec thread builds this at near-zero
/// cost: the envelope's byte payload is refcounted, and cell extraction is
/// one pass over the small `WriteSet`.
pub struct ShadowTxCapture {
    pub(crate) envelope: TxEnvelope,
    pub(crate) gas_used: u64,
    pub(crate) touches: TouchSet,
    pub(crate) write_cells: Vec<Cell>,
}

/// One block's handoff.
pub struct ShadowBlock {
    pub(crate) block_number: u64,
    pub(crate) captures: Vec<ShadowTxCapture>,
    /// Count of serial-lane records (deposits) in the block. The predictor
    /// does not model these (they use the serial barrier lane). This
    /// count lets block totals match in the summary line.
    pub(crate) serial_records: u32,
}

impl ShadowBlock {
    /// Turn this block's captures into graded observations, one per tx, in
    /// capture order. Read and write cells are sorted and deduped, for
    /// parity with the offline yardstick.
    fn into_observations(self) -> Vec<TxObs> {
        self.captures
            .into_iter()
            .enumerate()
            .map(|(i, c)| {
                let kardamom_footprint::EnvelopeView {
                    to,
                    selector,
                    args,
                    has_value,
                } = envelope_view(&c.envelope.raw_tx);
                // Account reads (BALANCE/EXTCODE* subjects) stay out of the
                // conflict cells, for parity with the offline yardstick. See
                // the note on `kardamom_footprint::Cell`.
                let mut reads: Vec<Cell> = c
                    .touches
                    .slot_reads
                    .iter()
                    .map(|(a, k)| Cell::Slot(*a, *k))
                    .collect();
                reads.sort_unstable();
                reads.dedup();
                let mut writes = c.write_cells;
                writes.sort_unstable();
                writes.dedup();
                TxObs {
                    index: i as u64,
                    block: self.block_number,
                    sender: c.envelope.sender,
                    to,
                    selector,
                    args,
                    gas: c.gas_used,
                    has_value,
                    reads,
                    writes,
                }
            })
            .collect()
    }
}

/// Extract the write cells of one tx from its `WriteSet`. This is the same
/// cell model the offline capture used: one `Account` cell per written account,
/// one `Slot` cell per storage write. Reads come in through [`TouchSet`].
pub(crate) fn write_cells(ws: &WriteSet) -> Vec<Cell> {
    ws.accounts
        .iter()
        .map(|(addr, _)| Cell::Account(*addr))
        .chain(
            ws.storage
                .iter()
                .map(|((addr, key), _)| Cell::Slot(*addr, *key)),
        )
        .collect()
}

/// One graded block's outcome: everything the metrics emission and the
/// summary log line need, gathered so neither takes it as loose
/// parameters.
struct GradedBlock {
    block_number: u64,
    g: kardamom_footprint::grade::BlockGrade,
    serial_records: u32,
    accumulator_reads: usize,
}

impl GradedBlock {
    /// Emit the grade's metrics: prediction hit rate, false-independent
    /// and false-edge counts, cold txs, the Accumulator-guard read count,
    /// and the predicted-vs-oracle wave shape.
    #[allow(
        clippy::cast_precision_loss,
        reason = "per-block DAG counts stay far below 2^52"
    )]
    fn emit_metrics(&self) {
        let g = &self.g;
        metrics::counter!(crate::metrics::FOOTPRINT_BLOCKS_TOTAL, "outcome" => "graded")
            .increment(1);
        metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTION_HIT_RATE).set(g.hit_rate());
        metrics::counter!(crate::metrics::FOOTPRINT_FALSE_INDEPENDENT_TOTAL)
            .increment(g.missed_pairs as u64);
        metrics::counter!(crate::metrics::FOOTPRINT_FALSE_EDGE_TOTAL)
            .increment(g.false_pairs as u64);
        metrics::counter!(crate::metrics::FOOTPRINT_COLD_TX_TOTAL).increment(g.cold_txs as u64);
        metrics::counter!(crate::metrics::FOOTPRINT_ACCUMULATOR_READ_TOTAL)
            .increment(self.accumulator_reads as u64);
        metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_WAVES).set(g.predicted_waves as f64);
        metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_WIDTH).set(g.predicted_width as f64);
        metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_EDGES).set(g.predicted_edges as f64);
        metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_CP_RATIO).set(g.predicted_cp_ratio());
        metrics::gauge!(crate::metrics::FOOTPRINT_ORACLE_CP_RATIO).set(g.oracle_cp_ratio());
    }

    /// Log one summary line for the graded block.
    fn log_summary(&self) {
        let g = &self.g;
        tracing::info!(
            target: "kardamom_executor::shadow",
            block = self.block_number,
            txs = g.txs,
            graded = g.graded,
            serial = self.serial_records,
            cold = g.cold_txs,
            hit_rate = format!("{:.4}", g.hit_rate()),
            waves = g.predicted_waves,
            width = g.predicted_width,
            pred_edges = g.predicted_edges,
            true_edges = g.true_edges,
            false_independent = g.missed_pairs,
            over_merge = g.false_pairs,
            cp_pred = format!("{:.2}", g.predicted_cp_ratio()),
            cp_oracle = format!("{:.2}", g.oracle_cp_ratio()),
            accumulator_reads = self.accumulator_reads,
            "footprint shadow block graded"
        );
    }
}

/// The shadow thread's state: the block channel it drains, the classifier
/// stats trained across blocks, and the grading exclusion set (the
/// Accumulator boundary). One instance serves the shadow thread's whole
/// life; test code builds one instance to grade a block without a thread.
pub struct Shadow {
    rx: Receiver<ShadowBlock>,
    stats: Stats,
    exclude: HashSet<Cell>,
}

impl Shadow {
    pub(crate) fn new(rx: Receiver<ShadowBlock>) -> Self {
        Self {
            rx,
            stats: Stats::default(),
            exclude: HashSet::from([Cell::Account(FEE_SINK)]),
        }
    }

    /// Read `KARDAMOM_FOOTPRINT_SHADOW`. If it is `1`, spawn the shadow
    /// thread and return the exec side's sender. The thread exits when the
    /// executor drops the sender. No join handle is needed, because the
    /// thread owns no state that anything waits for.
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the thread.
    pub fn spawn_from_env() -> Option<Sender<ShadowBlock>> {
        if std::env::var("KARDAMOM_FOOTPRINT_SHADOW").ok().as_deref() != Some("1") {
            return None;
        }
        let (tx, rx) = bounded::<ShadowBlock>(8);
        std::thread::Builder::new()
            .name("footprint-shadow".into())
            .spawn(move || Self::new(rx).run())
            .expect("spawn footprint-shadow");
        tracing::info!(target: "kardamom_executor::shadow", "footprint shadow ENABLED (measurement only; execution stays sequential)");
        Some(tx)
    }

    /// Grade every block until the exec side drops its sender.
    fn run(mut self) {
        while let Ok(block) = self.rx.recv() {
            self.process_block(block);
        }
    }

    /// Grade one block, emit its metrics and summary line, then train.
    pub(crate) fn process_block(&mut self, block: ShadowBlock) {
        let block_number = block.block_number;
        let serial_records = block.serial_records;
        // This is the Accumulator-guard signal. A BALANCE-opcode read
        // against the accumulator-marked fee sink would force
        // materialization at runtime. This should almost never happen. It
        // is measured here to track the price of the guard.
        let accumulator_reads = block
            .captures
            .iter()
            .filter(|c| c.touches.account_reads.contains(&FEE_SINK))
            .count();

        let obs = block.into_observations();
        let g = grade_block(&self.stats, &obs, &self.exclude, GRADE_CAP);

        let graded = GradedBlock {
            block_number,
            g,
            serial_records,
            accumulator_reads,
        };
        graded.emit_metrics();
        graded.log_summary();

        // Train after grading. The next block's prediction includes this one.
        self.train(&obs);
    }

    fn train(&mut self, obs: &[TxObs]) {
        for o in obs {
            self.stats.learn_obs(o);
        }
    }
}
