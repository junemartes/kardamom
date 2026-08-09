//! P1 footprint SHADOW scheduler (spec: block-stm-executor §P1).
//!
//! At each boundary the exec thread hands this thread the block's per-tx
//! captures (envelope clone, gas, `TouchSet` reads, `WriteSet`-derived
//! write cells) over a bounded `try_send` channel — the same discipline as
//! the BAL publisher handoff: the shadow must NEVER back-pressure
//! execution, and a dropped block costs one block of measurement, not the
//! chain. Everything heavier than a few `Vec` pushes (envelope decode,
//! prediction, O(pairs) grading, training) happens here, off the hot path.
//!
//! Per block, in order: (1) predict with stats trained on PRIOR blocks
//! only, (2) grade the prediction against the block's actual cells
//! ([`kardamom_footprint::grade::grade_block`] — P0-identical semantics),
//! (3) emit metrics + a summary log line, (4) train on the block. A block
//! never grades against stats that already saw it, so the cold-start curve
//! in the emitted series is the real no-persistence cost the spec wants
//! priced.
//!
//! Execution stays sequential; nothing here feeds back into scheduling.
//! Enabled by `KARDAMOM_FOOTPRINT_SHADOW=1` on the EXECUTOR role only.

use std::collections::HashSet;

use alloy_primitives::Address;
use crossbeam_channel::{Receiver, Sender, bounded};
use kardamom_exec_core::delta::WriteSet;
use kardamom_exec_core::executor::TouchSet;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::grade::grade_block;
use kardamom_footprint::{Cell, TxObs, envelope_view};
use kardamom_types::TxEnvelope;

/// The fee sink — every tx credits it (P0: universal write cell), and the
/// `Accumulator` strategy services it by deferred commutative folding, so
/// it is excluded from conflict analysis (spec "The graph index" #4).
/// Mirrors `kardamom_exec_core::block_env`: beneficiary = address(0),
/// basefee = 0 — the V0 documented burn.
pub const FEE_SINK: Address = Address::ZERO;

/// Pair-grading cap per block. Grading is O(n²) set intersections; CI-scale
/// blocks are ≤~600 txs and saturated dev-host blocks ~2,700 — the cap
/// keeps a pathological burst block from wedging the shadow thread for
/// seconds. Truncation is REPORTED (`graded < txs` in the summary line),
/// never silent.
const GRADE_CAP: usize = 2_048;

/// One executed tx's capture, assembled on the exec thread at near-zero
/// cost: the envelope's byte payload is refcounted, the cell extraction is
/// one pass over the (small) `WriteSet`.
pub struct ShadowTxCapture {
    pub envelope: TxEnvelope,
    pub gas_used: u64,
    pub touches: TouchSet,
    pub write_cells: Vec<Cell>,
}

/// One block's handoff.
pub struct ShadowBlock {
    pub block_number: u64,
    pub captures: Vec<ShadowTxCapture>,
    /// Serial-lane records (deposits) in the block: not modeled by the
    /// predictor (spec strategy #1 — they take the serial barrier lane),
    /// counted so block totals reconcile in the summary line.
    pub serial_records: u32,
}

/// Extract the write cells of one tx from its `WriteSet` — the exact cell
/// model the P0 capture used: an `Account` cell per written account tuple,
/// a `Slot` cell per storage write. (Reads ride in via [`TouchSet`].)
pub fn write_cells(ws: &WriteSet) -> Vec<Cell> {
    let mut cells = Vec::with_capacity(ws.accounts.len() + ws.storage.len());
    for (addr, _) in ws.accounts.iter() {
        cells.push(Cell::Account(*addr));
    }
    for ((addr, key), _) in ws.storage.iter() {
        cells.push(Cell::Slot(*addr, *key));
    }
    cells
}

/// Read `KARDAMOM_FOOTPRINT_SHADOW`; when `1`, spawn the shadow thread and
/// return the exec side's sender. The thread exits when the executor drops
/// the sender (channel disconnect) — no join handle needed: it owns no
/// state anyone waits for.
pub fn spawn_from_env() -> Option<Sender<ShadowBlock>> {
    if std::env::var("KARDAMOM_FOOTPRINT_SHADOW").ok().as_deref() != Some("1") {
        return None;
    }
    let (tx, rx) = bounded::<ShadowBlock>(8);
    std::thread::Builder::new()
        .name("footprint-shadow".into())
        .spawn(move || run_shadow(rx))
        .expect("spawn footprint-shadow");
    tracing::info!(target: "kardamom_executor::shadow", "footprint shadow ENABLED (measurement only; execution stays sequential)");
    Some(tx)
}

fn run_shadow(rx: Receiver<ShadowBlock>) {
    let mut stats = Stats::default();
    let mut exclude = HashSet::new();
    exclude.insert(Cell::Account(FEE_SINK));
    while let Ok(block) = rx.recv() {
        process_block(block, &mut stats, &exclude);
    }
}

/// Grade one block, emit its metrics + summary line, then train. Public
/// (crate) so the actor tests can drive it without a thread.
pub(crate) fn process_block(block: ShadowBlock, stats: &mut Stats, exclude: &HashSet<Cell>) {
    let block_number = block.block_number;
    // The P2 Accumulator-guard signal: a BALANCE-opcode read against the
    // accumulator-marked fee sink would force materialization at runtime —
    // expected ~never; measured here so P2 knows the price of the guard.
    let accumulator_reads = block
        .captures
        .iter()
        .filter(|c| c.touches.account_reads.contains(&FEE_SINK))
        .count();

    let obs: Vec<TxObs> = block
        .captures
        .into_iter()
        .enumerate()
        .map(|(i, c)| {
            let (to, selector, args, has_value) = envelope_view(&c.envelope.raw_tx);
            let mut reads: Vec<Cell> = c
                .touches
                .slot_reads
                .iter()
                .map(|(a, k)| Cell::Slot(*a, *k))
                .collect();
            // Account reads (BALANCE/EXTCODE* subjects) stay OUT of the
            // conflict cells for parity with the P0 yardstick — see the
            // note on `kardamom_footprint::Cell`.
            reads.sort_unstable();
            reads.dedup();
            let mut writes = c.write_cells;
            writes.sort_unstable();
            writes.dedup();
            TxObs {
                index: i as u64,
                block: block_number,
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
        .collect();

    let g = grade_block(stats, &obs, exclude, GRADE_CAP);

    metrics::counter!(crate::metrics::FOOTPRINT_BLOCKS_TOTAL, "outcome" => "graded").increment(1);
    metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTION_HIT_RATE).set(g.hit_rate());
    metrics::counter!(crate::metrics::FOOTPRINT_FALSE_INDEPENDENT_TOTAL)
        .increment(g.missed_pairs as u64);
    metrics::counter!(crate::metrics::FOOTPRINT_FALSE_EDGE_TOTAL).increment(g.false_pairs as u64);
    metrics::counter!(crate::metrics::FOOTPRINT_COLD_TX_TOTAL).increment(g.cold_txs as u64);
    metrics::counter!(crate::metrics::FOOTPRINT_ACCUMULATOR_READ_TOTAL)
        .increment(accumulator_reads as u64);
    metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_WAVES).set(g.predicted_waves as f64);
    metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_WIDTH).set(g.predicted_width as f64);
    metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_EDGES).set(g.predicted_edges as f64);
    metrics::gauge!(crate::metrics::FOOTPRINT_PREDICTED_CP_RATIO).set(g.predicted_cp_ratio());
    metrics::gauge!(crate::metrics::FOOTPRINT_ORACLE_CP_RATIO).set(g.oracle_cp_ratio());

    tracing::info!(
        target: "kardamom_executor::shadow",
        block = block_number,
        txs = g.txs,
        graded = g.graded,
        serial = block.serial_records,
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
        accumulator_reads,
        "footprint shadow block graded"
    );

    // Train AFTER grading: the next block predicts with this one folded in.
    for o in &obs {
        stats.learn_obs(o);
    }
}
