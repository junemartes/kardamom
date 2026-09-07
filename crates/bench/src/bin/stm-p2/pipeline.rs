//! The pipelined A/B measurement: two independent databases. Pass A
//! runs sequentially, one block at a time, timed. Pass B streams blocks
//! through submit-ahead, at depth 2, with lag-1 byte-identical checks
//! and production-shaped settlement. Reports aggregate throughput.

use std::sync::mpsc;
use std::time::Instant;

use kardamom_footprint::classifier::Stats;
use kardamom_state::{StateSnapshot, WriteBatch, WriterHandle};
use kardamom_stm::execute::{
    BlockTicket, DeltaRelease, MvRelease, StmOutcome, execute_block_sequential,
};
use kardamom_types::{AccountChange, TxEnvelope};

use kardamom_bench::stm::BlockAt;

use super::common::{
    BlockOutputs, EngineOpts, FeedPayload, FeedRec, FlowRecs, StatsExt, Workload, assert_identical,
    open_mdbx_env, records,
};
use super::drive::{DriveParams, DriveState, MvChannels};

/// The timed sequential baseline pass, and the canonical outputs each
/// pipelined block is checked against.
struct PassA {
    seq_ms: f64,
    baseline: Vec<BlockOutputs>,
    flow_recs: Vec<FlowRecs>,
}

/// The mutable state [`PassA::run`]'s loop carries from one block to
/// the next.
struct PassARun<'a> {
    writer_a: &'a WriterHandle,
    snap_a: StateSnapshot,
    stats: Stats,
    global_idx: u64,
    seq_ms: f64,
    baseline: Vec<BlockOutputs>,
    flow_recs: Vec<FlowRecs>,
    warm: usize,
    timing: bool,
    chain_id: u64,
}

impl PassA {
    /// Run pass A: sequential, on its own database, timed for each flow
    /// block. The writer thread this opens closes when this function
    /// returns.
    fn run(w: &Workload<'_>, genesis: &[AccountChange]) -> anyhow::Result<Self> {
        let handles = open_mdbx_env(genesis)?;
        let writer_a = handles.writer;
        let snap_a = super::common::initial_snapshot(&writer_a);
        let mut state = PassARun {
            writer_a: &writer_a,
            snap_a,
            stats: Stats::default(),
            global_idx: 0,
            seq_ms: 0.0,
            baseline: Vec::new(),
            flow_recs: Vec::new(),
            warm: w.warm(),
            timing: std::env::var_os("KARDAMOM_STM_PHASE_TIMING").is_some(),
            chain_id: w.chain_id,
        };
        for (bi, blk) in w.all_blocks.iter().enumerate() {
            state.run_block(bi, blk)?;
        }
        Ok(Self {
            seq_ms: state.seq_ms,
            baseline: state.baseline,
            flow_recs: state.flow_recs,
        })
    }
}

impl PassARun<'_> {
    /// Run one block: sequential execution, capture (untimed), and
    /// commit to database A. Records the flow-window sample when `bi`
    /// is past warmup.
    fn run_block(&mut self, bi: usize, blk: &[TxEnvelope]) -> anyhow::Result<()> {
        let e = BlockAt(bi).env(self.chain_id);
        let recs = records(self.global_idx, blk);
        self.global_idx += blk.len() as u64;
        let t0 = Instant::now();
        let (receipts, delta) = execute_block_sequential(&self.snap_a, None, e, &recs)?;
        let el = t0.elapsed().as_secs_f64() * 1e3;
        let is_flow = bi >= self.warm;
        if is_flow {
            self.seq_ms += el;
            self.baseline.push(BlockOutputs {
                receipts: receipts.clone(),
                delta: delta.clone(),
            });
            self.flow_recs.push(recs.clone());
        }
        self.stats.train_block(&self.snap_a, &recs, e, None)?;
        let bd = delta.finalize(e.block_number, receipts);
        let t_w = Instant::now();
        self.writer_a
            .delta_tx
            .send(WriteBatch::new(BlockAt(bi).boundary(self.global_idx), bd))?;
        self.snap_a = super::common::wait_for_block(self.writer_a, e.block_number);
        if self.timing && is_flow {
            eprintln!("writer block {bi}: {:?}", t_w.elapsed());
        }
        Ok(())
    }
}

impl Workload<'_> {
    /// Settle setup and warmup on database B, sequentially and
    /// untimed, training along the way, so the pipeline starts warm,
    /// as production does. Returns the post-warmup snapshot.
    fn settle_warmup(
        &self,
        writer_b: &WriterHandle,
        warm_recs: &[(usize, FlowRecs)],
        warm: usize,
        stats_b: &mut Stats,
    ) -> anyhow::Result<StateSnapshot> {
        let mut snap_b = super::common::initial_snapshot(writer_b);
        for (bi, recs) in warm_recs.iter().take(warm) {
            let e = self.env_for(*bi);
            let (receipts, delta) = execute_block_sequential(&snap_b, None, e, recs)?;
            stats_b.train_block(&snap_b, recs, e, None)?;
            let end = recs.last().map_or(0, |r| r.0.0 + 1);
            let bd = delta.finalize(e.block_number, receipts);
            writer_b
                .delta_tx
                .send(WriteBatch::new(BlockAt(*bi).boundary(end), bd))?;
            snap_b = super::common::wait_for_block(writer_b, e.block_number);
        }
        Ok(snap_b)
    }
}

/// Every block's records, indexed by block number. Built once and
/// shared by the warmup pass and the upstream prepare pass below.
fn all_recs(all_blocks: &[Vec<TxEnvelope>]) -> Vec<(usize, FlowRecs)> {
    let mut gi = 0u64;
    let mut recs = Vec::new();
    for (bi, blk) in all_blocks.iter().enumerate() {
        recs.push((bi, records(gi, blk)));
        gi += blk.len() as u64;
    }
    recs
}

/// Upstream prepare (decode and predict), untimed. The live executor
/// pays this cost on the `tx_data` readers.
fn prepare_feed_payloads(
    warm_recs: &[(usize, FlowRecs)],
    warm: usize,
    stats_b: &Stats,
) -> Vec<FeedPayload> {
    warm_recs
        .iter()
        .skip(warm)
        .map(|(_, recs)| {
            recs.iter()
                .map(|(t, p, en)| {
                    let prepared = kardamom_stm::execute::prepare(en, *t, stats_b);
                    FeedRec {
                        idx: *t,
                        position: *p,
                        envelope: en.clone(),
                        prepared,
                    }
                })
                .collect()
        })
        .collect()
}

/// One retained STM outcome from the settler thread: `pfi` is its
/// index into the pipeline's flow blocks.
struct RetainedOutcome {
    pfi: usize,
    out: StmOutcome,
}

/// The settler thread's join handle: it resolves tickets in order,
/// finalizes each block, hands the writer its batch, and returns the
/// outcomes it retained (empty in pure-timing mode).
type SettlerHandle = std::thread::JoinHandle<anyhow::Result<Vec<RetainedOutcome>>>;

/// Everything the settler thread needs, owned up front so
/// [`Settler::spawn`] can move it into the thread closure.
struct Settler<'a> {
    writer_b: &'a WriterHandle,
    settle_rx: mpsc::Receiver<(usize, BlockTicket)>,
    flow_recs: &'a [FlowRecs],
    chain_id: u64,
    warm: usize,
    timing: bool,
    retain_outcomes: bool,
}

/// The settler thread's own state, moved into the thread closure: it
/// holds nothing borrowed from [`Settler`], so it can outlive the
/// submission loop that spawns it.
struct SettleCtx {
    delta_tx: crossbeam_channel::Sender<WriteBatch>,
    /// Each flow block's end record index, indexed by `pfi`.
    ends: Vec<u64>,
    chain_id: u64,
    warm: usize,
    timing: bool,
    retain_outcomes: bool,
}

impl SettleCtx {
    /// Wait for one ticket's tail, finalize its block, hand the writer
    /// its batch, and return the outcome when the post-hoc byte checks
    /// need it (`retain_outcomes`).
    ///
    /// The per-worker busy time and read time, in microseconds, stay
    /// far under 2^52 for any block this benchmark runs.
    #[allow(
        clippy::cast_precision_loss,
        reason = "per-worker busy and read time, in microseconds, stay far under 2^52 for any block this benchmark runs"
    )]
    fn settle_one(&self, pfi: usize, t: BlockTicket) -> anyhow::Result<Option<RetainedOutcome>> {
        let out = t.wait()?;
        if self.timing {
            eprintln!(
                "settle block {pfi}: busy {:.1}ms read {:.1}ms wounds {}",
                out.busy_per_worker_us.iter().sum::<u64>() as f64 / 1000.0,
                out.read_us as f64 / 1000.0,
                out.wounds
            );
        }
        let bi = self.warm + pfi;
        let e = BlockAt(bi).env(self.chain_id);
        // The production shape consumes the outcome by value, with no
        // clone: `retain_outcomes` is false. The clone and retention
        // below run only for the post-hoc byte checks
        // (KARDAMOM_PIPE_ASSERT=1, the correctness pass), off the
        // executing span's shared cache traffic.
        if self.retain_outcomes {
            let bd = out
                .delta
                .clone()
                .finalize(e.block_number, out.receipts.clone());
            self.delta_tx
                .send(WriteBatch::new(BlockAt(bi).boundary(self.ends[pfi]), bd))?;
            Ok(Some(RetainedOutcome { pfi, out }))
        } else {
            let bd = out.delta.finalize(e.block_number, out.receipts);
            self.delta_tx
                .send(WriteBatch::new(BlockAt(bi).boundary(self.ends[pfi]), bd))?;
            Ok(None)
        }
    }
}

impl Settler<'_> {
    /// Spawn the settler thread. It resolves tickets in order: it waits
    /// for the tail, records the outcome, finalizes, hands the writer
    /// its batch, and advances the pool's base cache, all off the
    /// submission loop. The loop's only bookkeeping is layer assembly,
    /// through Arc clones, and admission.
    ///
    /// The thread owns its outcomes and returns them from this handle
    /// instead of sharing a mutex only it ever writes.
    fn spawn(self) -> SettlerHandle {
        let delta_tx = self.writer_b.delta_tx.clone();
        let ends: Vec<u64> = self
            .flow_recs
            .iter()
            .map(|r| r.last().map_or(0, |x| x.0.0 + 1))
            .collect();
        let Self {
            settle_rx,
            chain_id,
            warm,
            timing,
            retain_outcomes,
            ..
        } = self;
        let ctx = SettleCtx {
            delta_tx,
            ends,
            chain_id,
            warm,
            timing,
            retain_outcomes,
        };
        std::thread::spawn(move || -> anyhow::Result<Vec<RetainedOutcome>> {
            let mut outcomes = Vec::new();
            while let Ok((pfi, t)) = settle_rx.recv() {
                outcomes.extend(ctx.settle_one(pfi, t)?);
            }
            Ok(outcomes)
        })
    }
}

/// [`run_pipelined`]'s fixed inputs, shared across every worker count.
struct PipelineRun<'a> {
    w: &'a Workload<'a>,
    eng: &'a EngineOpts,
    speculative: bool,
    genesis: Vec<AccountChange>,
    chain_id: u64,
    warm: usize,
}

impl PipelineRun<'_> {
    /// Run pass A (the sequential baseline) and the pipelined pool at
    /// `wk` workers, and print the comparison line.
    fn run_worker_count(&self, wk: usize) -> anyhow::Result<()> {
        let pass_a = PassA::run(self.w, &self.genesis)?;
        let (seq_ms, baseline, flow_recs) = (pass_a.seq_ms, pass_a.baseline, pass_a.flow_recs);

        // Pass B: the pipelined pool, a fresh database, submit-ahead at depth 2.
        let handles_b = open_mdbx_env(&self.genesis)?;
        let env_b = handles_b.env_for_reads;
        let writer_b = handles_b.writer;
        let cfg = self.eng.pool_config(wk, 4, false);
        // Prepare (decode and predict) upstream and untimed. The live
        // executor pays this cost on the tx_data readers.
        let mut stats_b = Stats::default();
        let warm_recs = all_recs(self.w.all_blocks);
        let _snap_b = self
            .w
            .settle_warmup(&writer_b, &warm_recs, self.warm, &mut stats_b)?;
        // These are owned per-block feed payloads. The loop consumes them
        // without clones, since production tx_data readers hand over owned
        // values.
        let mut feed_payloads = prepare_feed_payloads(&warm_recs, self.warm, &stats_b);

        let n_flow = baseline.len();
        // Setting KARDAMOM_PIPE_ASSERT=0 gives pure-timing mode: the
        // settler consumes outcomes, the production shape, with no clone
        // on the clock, and the code skips the post-hoc byte checks. The
        // default, 1, checks every block. This is the correctness pass,
        // with slightly pessimistic timing.
        let retain_outcomes = std::env::var("KARDAMOM_PIPE_ASSERT").map_or(true, |v| v != "0");
        let mut outcomes: Vec<RetainedOutcome> = Vec::new();
        let t_pipe = Instant::now();
        let timing = std::env::var_os("KARDAMOM_STM_PHASE_TIMING").is_some();
        let warm = self.warm;
        let chain_id = self.chain_id;
        let speculative = self.speculative;
        let w = self.w;
        kardamom_stm::execute::with_pool(cfg, |pool| -> anyhow::Result<()> {
            // Writer settlement is asynchronous: the loop sends deltas and
            // never waits on them. `advanced_to` tracks which flow deltas
            // are mirrored into the pool's base cache after the writer
            // confirms them. Everything after that layers as the pending
            // base; depth grows only if the writer lags a full execution span.
            let (settle_tx, settle_rx) = mpsc::channel::<(usize, BlockTicket)>();
            let settler = Settler {
                writer_b: &writer_b,
                settle_rx,
                flow_recs: &flow_recs,
                chain_id,
                warm,
                timing,
                retain_outcomes,
            }
            .spawn();

            let (rel_tx, rel_rx) = mpsc::channel::<DeltaRelease>();
            let (mv_tx, mv_rx) = mpsc::channel::<MvRelease>();
            let mut state = DriveState::new(
                std::mem::take(&mut feed_payloads),
                speculative,
                &baseline,
                n_flow,
            );
            let params = DriveParams {
                w,
                env_b: &env_b,
                writer_b: &writer_b,
                stats_b: &stats_b,
                warm,
                wk,
                n_flow,
                timing,
            };
            // The submission loop owns `settle_tx`; it closes when this
            // function returns, so the settler's `recv()` loop sees EOF.
            if speculative {
                let ch = MvChannels {
                    mv_tx,
                    rel_tx,
                    rel_rx,
                    mv_rx,
                };
                state.drive_speculative(pool, &params, settle_tx, &ch)?;
            } else {
                state.drive_baseline(pool, &params, settle_tx)?;
            }
            outcomes.extend(settler.join().expect("settler join")?);
            Ok(())
        })?;
        let stm_ms = t_pipe.elapsed().as_secs_f64() * 1e3;
        if !retain_outcomes {
            println!(
                "  (pure-timing mode: byte-asserts skipped — run KARDAMOM_PIPE_ASSERT=1 for the correctness pass)"
            );
        }
        // Verify after the clock stops: every block must be byte-identical.
        let asserted = verify_outcomes(&outcomes, &baseline, self.warm);
        println!(
            "  w={wk} seq {seq_ms:.0}ms | pipelined {stm_ms:.0}ms | speedup {:.2}x | blocks {n_flow} asserted {asserted}",
            seq_ms / stm_ms
        );
        Ok(())
    }
}

/// Run the pipelined measurement over every worker count in `eng`.
///
/// `speculative` selects the block-N+1 layering source: the engine's
/// own deltas, released at each block's fold (the production shape),
/// or the pass-A baseline deltas (measures pipeline mechanics only, the
/// default). A scenario must be wound-free: a corrected release fails
/// the run loudly.
pub(crate) fn run_pipelined(
    w: &Workload<'_>,
    eng: &EngineOpts,
    speculative: bool,
) -> anyhow::Result<()> {
    println!("\n===== STM-P3a PIPELINED [two DBs, depth 2, lag-1 asserts] =====");
    let run = PipelineRun {
        w,
        eng,
        speculative,
        genesis: w.genesis(),
        chain_id: w.chain_id,
        warm: w.warm(),
    };
    for &wk in &eng.worker_counts {
        run.run_worker_count(wk)?;
    }
    Ok(())
}

/// Check every retained outcome against pass A's baseline, byte for
/// byte. Returns the count checked.
fn verify_outcomes(outcomes: &[RetainedOutcome], baseline: &[BlockOutputs], warm: usize) -> usize {
    for r in outcomes {
        assert_identical(&baseline[r.pfi], &r.out, (warm + r.pfi) as u64 + 1, 4);
    }
    outcomes.len()
}
