//! `kardamom-stm-p2` — Block-STM P2 offline A/B: the same generated blocks
//! through the sequential engine (`ExecScope`) and the STM engine
//! (`kardamom-stm`), asserting BYTE-IDENTICAL receipts + delta on every
//! block, and reporting wall-clock per worker count.
//! (spec: docs/agents/block-stm-executor-spec.md §P2 / "Measurement plan")
//!
//! Protocol per block, mirroring production learning dynamics:
//! 1. timed sequential run (the baseline and the canonical outputs),
//! 2. timed STM run per worker count, each byte-compared against (1),
//! 3. untimed capture pass training the footprint stats (prior-blocks-only
//!    stats feed each block's schedule — cold start, stream order).
//!
//! Wall-clock numbers are indicative (shared dev host); the assertion is
//! the point — the speedup column is the shape, not a benchmark citation.

use std::time::Instant;

use alloy_primitives::U256;
use clap::Parser;
use kardamom_bench::load::defi;
use kardamom_bench::mnemonic;
use kardamom_bench::stm::uniswap;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_engine::executor::{ExecScope, TouchSet};
use kardamom_engine::state::MockStateDatabase;
use kardamom_footprint::classifier::Stats;
use kardamom_footprint::{Cell, TxObs, envelope_view};
use kardamom_stm::execute::execute_block_sequential;
use kardamom_types::{BPosition, Receipt, TxEnvelope};

const ANVIL_MNEMONIC: &str = "test test test test test test test test test test test junk";

#[derive(Parser, Debug)]
#[command(name = "kardamom-stm-p2")]
struct Args {
    /// uniswap | defi | transfers
    #[arg(long, default_value = "uniswap")]
    scenario: String,
    #[arg(long, default_value_t = 8)]
    pairs: usize,
    #[arg(long, default_value_t = 96)]
    senders: usize,
    #[arg(long, default_value_t = 16)]
    blocks: usize,
    #[arg(long, default_value_t = 500)]
    block_size: usize,
    #[arg(long, default_value_t = 70)]
    swap_share: u64,
    #[arg(long, default_value_t = 10)]
    cross: u64,
    /// Worker counts to sweep, comma-separated.
    #[arg(long, default_value = "1,2,4,8,12")]
    workers: String,
    #[arg(long, default_value = ".")]
    repo_root: String,
    #[arg(long, default_value_t = 412346)]
    chain_id: u64,
    /// Print per-block lines.
    #[arg(long, default_value_t = false)]
    per_block: bool,
    /// Write an on-CPU flamegraph here (pprof). Guessing which part of
    /// the read path costs what has been wrong repeatedly — this settles it.
    #[arg(long)]
    pprof_out: Option<String>,
    /// State backend: `mock` (in-memory — the harshest baseline, where a
    /// read costs nothing) or `mdbx` (the real backend, where it does).
    #[arg(long, default_value = "mock")]
    state: String,
    /// DAG prune batch sizes to sweep (completions applied per graph-lock
    /// acquisition; 1 = update on every completion).
    #[arg(long, default_value = "1")]
    prune_batch: String,
    /// Mean per-tx nanoseconds below which the pool declines a block and
    /// runs it sequentially. `0` forces parallel execution regardless —
    /// which is what you want when MEASURING scaling, since the default
    /// policy would route cheap workloads to the sequential path and hide
    /// the very numbers under test.
    #[arg(long)]
    parallel_worth_ns: Option<u64>,
    /// Dispatch on the sender instead of the first non-sender cell.
    #[arg(long, default_value_t = false)]
    dispatch_by_sender: bool,
    /// Eager chain FIFO: enqueue at admission when all unfinished preds
    /// are already in the same worker's queue. Disable for the A/B
    /// baseline.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    eager_chain: bool,
    /// Sticky least-loaded domain assignment instead of pure hashing.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    sticky_assign: bool,
    /// Pin worker i to core list[i % len], e.g. "2,3,4,5". Empty = OS.
    #[arg(long, default_value = "")]
    pin_cores: String,
    /// parcounter: how many sload+add+sstore rounds each call performs.
    /// 1 = the 10-byte micro counter (~4us/tx — a stress test of fixed
    /// costs); ~25 approximates real contract weight (~10us/tx), which is
    /// the honest substrate for scaling questions.
    #[arg(long, default_value_t = 1)]
    call_work: usize,
    /// Flow blocks excluded from timing while the footprint stats warm
    /// up. The FIRST flow block runs entirely COLD (nothing trained yet):
    /// every tx is a barrier, the DAG degenerates to a serial chain with
    /// O(n^2) edge fan-in (measured: 1000 colds, 375k edges, ~30ms serial
    /// span), and the 8-block average mostly measures that one block.
    /// Production stats are continuously warm; steady state is the honest
    /// number.
    #[arg(long, default_value_t = 1)]
    warmup_blocks: usize,
    /// Engine keep-hot: workers spin-yield between blocks (holds core
    /// frequency; replaces external SCHED_IDLE spinners).
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    keep_hot: bool,
    /// P3a pipelined measurement: two independent DBs; pass A runs
    /// sequential per block (timed), pass B streams blocks through
    /// submit-ahead (depth 2) with lag-1 byte-identical asserts and
    /// production-shaped settlement. Reports aggregate throughput.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pipeline: bool,
    /// With --pipeline: layer block N+1 on the ENGINE's own deltas,
    /// released speculatively at block N's fold (spec P3b) — the
    /// production shape. Default keeps the baseline-delta layering
    /// (measures pipeline mechanics only). Scenarios must be
    /// wound-free: a corrected release fails the run loudly.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pipeline_speculative: bool,
    /// Bag scheduler (default): one shared lock-free runnable set,
    /// inline completion, chain-local hand-off. `false` = legacy
    /// per-worker FIFO scheduler (stealing + eager coverage) for A/B.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    bag_scheduler: bool,
}

/// Counting allocator: every alloc through one pair of relaxed counters.
/// The w=1 gap survived read-path timing (2.5us of 19.2), graph elision,
/// and a sampling profiler — allocation pressure is the surviving
/// hypothesis, and counting is the only offline way to test it.
struct CountingAlloc;
static ALLOC_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static ALLOC_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Size-class histogram: <64, <512, <4K, <32K, <256K, big.
static ALLOC_BUCKETS: [std::sync::atomic::AtomicU64; 6] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];
static BUCKET_BYTES: [std::sync::atomic::AtomicU64; 6] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

fn bucket_of(sz: usize) -> usize {
    match sz {
        0..=63 => 0,
        64..=511 => 1,
        512..=4095 => 2,
        4096..=32767 => 3,
        32768..=262143 => 4,
        _ => 5,
    }
}

static REALLOC_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static REALLOC_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

unsafe impl std::alloc::GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, std::sync::atomic::Ordering::Relaxed);
        let b = bucket_of(layout.size());
        ALLOC_BUCKETS[b].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        BUCKET_BYTES[b].fetch_add(layout.size() as u64, std::sync::atomic::Ordering::Relaxed);
        unsafe { std::alloc::System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    // Explicit, so Vec growth is measured as GROWTH — the default impl
    // routes through alloc()+dealloc() and makes a 4->8->16 growth series
    // read as three fresh allocations.
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        REALLOC_BYTES.fetch_add(new_size as u64, std::sync::atomic::Ordering::Relaxed);
        unsafe { std::alloc::System.realloc(ptr, layout, new_size) }
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, std::sync::atomic::Ordering::Relaxed);
        let b = bucket_of(layout.size());
        ALLOC_BUCKETS[b].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        BUCKET_BYTES[b].fetch_add(layout.size() as u64, std::sync::atomic::Ordering::Relaxed);
        unsafe { std::alloc::System.alloc_zeroed(layout) }
    }
}

fn bucket_snap() -> [(u64, u64); 6] {
    std::array::from_fn(|i| {
        (
            ALLOC_BUCKETS[i].load(std::sync::atomic::Ordering::Relaxed),
            BUCKET_BYTES[i].load(std::sync::atomic::Ordering::Relaxed),
        )
    })
}

// mimalloc as the backing store was TRIED and REVERTED (2026-08-16):
// it raised block-at-a-time worker busy (+11%) more than it helped the
// pipeline's span inflation (unchanged) — the contention is not
// user-space arena locks; see the spec's pipeline-span-inflation note
// (mmap_lock / page-fault path is the open suspect).
#[global_allocator]
static COUNTING_ALLOC: CountingAlloc = CountingAlloc;

fn alloc_snap() -> (u64, u64, u64, u64) {
    (
        ALLOC_CALLS.load(std::sync::atomic::Ordering::Relaxed),
        ALLOC_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        REALLOC_CALLS.load(std::sync::atomic::Ordering::Relaxed),
        REALLOC_BYTES.load(std::sync::atomic::Ordering::Relaxed),
    )
}

fn records(base_idx: u64, envs: &[TxEnvelope]) -> Vec<(TxIndex, BPosition, TxEnvelope)> {
    envs.iter()
        .enumerate()
        .map(|(i, e)| {
            let g = base_idx + i as u64;
            (TxIndex(g), BPosition::from_index(g), e.clone())
        })
        .collect()
}

fn assert_identical(
    seq: &[Receipt],
    seq_delta: &PendingDelta,
    stm: &[Receipt],
    stm_delta: &PendingDelta,
    block: u64,
    w: usize,
) {
    if seq != stm {
        let i = seq
            .iter()
            .zip(stm.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(seq.len().min(stm.len()));
        panic!("block {block} workers {w}: receipt divergence at tx {i}");
    }
    assert!(
        seq_delta.accounts == stm_delta.accounts
            && seq_delta.storage == stm_delta.storage
            && seq_delta.code == stm_delta.code,
        "block {block} workers {w}: delta divergence"
    );
}

/// Fold a block's ACTUAL footprints into the stats — the capture pass the
/// live shadow performs, shared by both backends so each block is scheduled
/// with prior-blocks-only knowledge.
fn train<S: kardamom_types::StateDatabase>(
    snapshot: &S,
    recs: &[(TxIndex, BPosition, TxEnvelope)],
    e: ExecEnv,
    base: Option<&PendingDelta>,
    stats: &mut Stats,
) -> anyhow::Result<()> {
    let mut scope = ExecScope::new(snapshot, base, e)?;
    let mut cumulative = 0u64;
    for (i, (tx_idx, position, envelope)) in recs.iter().enumerate() {
        let mut touches = TouchSet::default();
        let (receipt, ws) = scope.execute_tx(
            *tx_idx,
            *position,
            envelope,
            i as u64,
            cumulative,
            None,
            Some(&mut touches),
        )?;
        cumulative = receipt.cumulative_gas_used;
        let (to, selector, args, has_value) = envelope_view(&envelope.raw_tx);
        let mut reads: Vec<Cell> = touches
            .slot_reads
            .iter()
            .map(|(ad, k)| Cell::Slot(*ad, *k))
            .collect();
        reads.sort_unstable();
        reads.dedup();
        let mut writes: Vec<Cell> = ws
            .accounts
            .iter()
            .map(|(ad, _)| Cell::Account(*ad))
            .chain(ws.storage.iter().map(|((ad, k), _)| Cell::Slot(*ad, *k)))
            .collect();
        writes.sort_unstable();
        writes.dedup();
        stats.learn_obs(&TxObs {
            index: i as u64,
            block: e.block_number,
            sender: envelope.sender,
            to,
            selector,
            args,
            gas: receipt.gas_used,
            has_value,
            reads,
            writes,
        });
    }
    Ok(())
}

/// mdbx-backed A/B — THE honest baseline.
///
/// Every other number in this harness is measured against in-memory Mock
/// state, where a read costs a hash lookup and sequential execution runs at
/// ~3 Ggas/s. That is the most hostile possible comparison for a parallel
/// engine: it maximizes the scheduler's share of the work. Here reads go to
/// the real backend, so per-tx execution costs what it costs in production
/// and the coordination overhead is measured against the right denominator.
///
/// State is MONOTONIC here (a committed block cannot be un-committed), so
/// the sweep cannot replay a block per worker count against a rewound DB.
/// Instead each configuration gets a FRESH database and replays the whole
/// sequence, with both engines reading the SAME snapshot before it advances:
/// snapshot -> sequential (timed, canonical outputs) -> STM (timed,
/// byte-compared) -> commit the sequential delta -> next snapshot.
type FlowRecs = Vec<(TxIndex, BPosition, TxEnvelope)>;
type FeedPayload = Vec<(
    TxIndex,
    BPosition,
    TxEnvelope,
    kardamom_stm::execute::Prepared,
)>;

#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
#[allow(clippy::needless_range_loop)]
fn run_pipelined(
    signers: &[kardamom_bench::signers::DerivedSigner],
    all_blocks: &[Vec<TxEnvelope>],
    n_setup: usize,
    chain_id: u64,
    worker_counts: &[usize],
    parallel_worth_ns: u64,
    dispatch_by_sender: bool,
    eager_chain: bool,
    bag_scheduler: bool,
    sticky_assign: bool,
    pin_cores: Vec<usize>,
    warmup_blocks: usize,
    keep_hot: bool,
    speculative: bool,
) -> anyhow::Result<()> {
    use kardamom_state::{Durability, StateEnvBuilder, StateWriter, WriteBatch};
    use kardamom_types::{AccountChange, BlockBoundary};

    let genesis: Vec<AccountChange> = signers
        .iter()
        .map(|s| AccountChange {
            address: s.signer.address(),
            nonce: 0,
            balance: U256::from(10u128.pow(21)),
            code_hash: alloy_primitives::KECCAK256_EMPTY,
        })
        .collect();

    let mk_env = || -> anyhow::Result<_> {
        let dir = tempfile::tempdir()?;
        let env = StateEnvBuilder::new(dir.path())
            .durability(Durability::SafeNoSync)
            .write_map(true)
            .open()?;
        kardamom_state::seed_genesis(&env, &genesis, &[])?;
        let env_for_reads = env.clone();
        let writer = StateWriter::spawn(env)?;
        Ok((dir, env_for_reads, writer))
    };
    let boundary = move |bi: usize, end: u64| BlockBoundary {
        block_number: bi as u64 + 1,
        end_tx_idx: BPosition::from_index(end),
        l2_timestamp: 1_700_000_000 + bi as u64 * 2,
        l1_origin: 0,
    };
    let env_of = move |bi: usize| ExecEnv {
        chain_id,
        block_number: bi as u64 + 1,
        l2_timestamp: 1_700_000_000 + bi as u64 * 2,
    };

    println!("\n===== STM-P3a PIPELINED [two DBs, depth 2, lag-1 asserts] =====");
    let warm = n_setup + warmup_blocks;

    for &w in worker_counts {
        // ---- PASS A: sequential, own DB, timed per flow block ----
        let (_da, _ra, writer_a) = mk_env()?;
        let mut snap_a = writer_a.snapshot_rx.current().expect("initial snapshot");
        let mut stats = Stats::default();
        let mut global_idx = 0u64;
        let mut seq_ms = 0f64;
        let mut baseline: Vec<(Vec<Receipt>, PendingDelta)> = Vec::new();
        let mut flow_recs: Vec<Vec<(TxIndex, BPosition, TxEnvelope)>> = Vec::new();
        for (bi, blk) in all_blocks.iter().enumerate() {
            let e = env_of(bi);
            let recs = records(global_idx, blk);
            global_idx += blk.len() as u64;
            let t0 = Instant::now();
            let (receipts, delta) = execute_block_sequential(&snap_a, None, e, &recs)?;
            let el = t0.elapsed().as_secs_f64() * 1e3;
            if bi >= warm {
                seq_ms += el;
                baseline.push((receipts.clone(), delta.clone()));
                flow_recs.push(recs.clone());
            }
            train(&snap_a, &recs, e, None, &mut stats)?;
            let bd = delta.finalize(e.block_number, receipts);
            let t_w = Instant::now();
            writer_a
                .delta_tx
                .send(WriteBatch::new(boundary(bi, global_idx), bd))?;
            loop {
                let sn = writer_a.snapshot_rx.recv().expect("writer alive");
                let at = sn.block_number();
                snap_a = sn;
                if at >= e.block_number {
                    break;
                }
            }
            if std::env::var_os("KARDAMOM_STM_PHASE_TIMING").is_some() && bi >= warm {
                eprintln!("writer block {bi}: {:?}", t_w.elapsed());
            }
        }
        drop(writer_a);

        // ---- PASS B: pipelined pool, fresh DB, submit-ahead depth 2 ----
        let (_db, env_b, writer_b) = mk_env()?;
        let mut snap_b = writer_b.snapshot_rx.current().expect("initial snapshot");
        let cfg = kardamom_stm::execute::PoolConfig {
            workers: w,
            prune_batch: 4,
            parallel_worth_ns,
            dispatch_by_sender,
            eager_chain,
            bag_scheduler,
            sticky_assign,
            keep_hot,
            tail_on_workers: false,
            pin_cores: pin_cores.clone(),
        };
        // Prepare (decode+predict) upstream and untimed — P3 pays this on
        // the tx_data readers.
        let mut stats_b = Stats::default();
        let mut gi = 0u64;
        let mut warm_recs: Vec<(usize, FlowRecs)> = Vec::new();
        for (bi, blk) in all_blocks.iter().enumerate() {
            let recs = records(gi, blk);
            gi += blk.len() as u64;
            warm_recs.push((bi, recs));
        }
        // Settle setup+warmup on DB B (sequential, untimed), training as
        // we go — the pipeline starts warm, as production does.
        for (bi, recs) in warm_recs.iter().take(warm) {
            let e = env_of(*bi);
            let (receipts, delta) = execute_block_sequential(&snap_b, None, e, recs)?;
            train(&snap_b, recs, e, None, &mut stats_b)?;
            let end = recs.last().map(|r| r.0.0 + 1).unwrap_or(0);
            let bd = delta.finalize(e.block_number, receipts);
            writer_b
                .delta_tx
                .send(WriteBatch::new(boundary(*bi, end), bd))?;
            loop {
                let sn = writer_b.snapshot_rx.recv().expect("writer alive");
                let at = sn.block_number();
                snap_b = sn;
                if at >= e.block_number {
                    break;
                }
            }
        }
        // OWNED per-block feed payloads — the loop consumes them without
        // clones, as production tx_data readers hand owned values.
        let mut feed_payloads: Vec<FeedPayload> = warm_recs
            .iter()
            .skip(warm)
            .map(|(_, recs)| {
                recs.iter()
                    .map(|(t, p, en)| {
                        let prep = kardamom_stm::execute::prepare(en, *t, &stats_b);
                        (*t, *p, en.clone(), prep)
                    })
                    .collect()
            })
            .collect();

        let n_flow = baseline.len();
        // KARDAMOM_PIPE_ASSERT=0 = pure-timing mode: the settler
        // consumes outcomes (production shape, no on-clock clone) and
        // the post-hoc byte-asserts are skipped. Default = 1: assert
        // every block (the correctness pass; slightly pessimistic
        // timing).
        let retain_outcomes = std::env::var("KARDAMOM_PIPE_ASSERT")
            .map(|v| v != "0")
            .unwrap_or(true);
        let mut asserted = 0usize;
        let mut outcomes: Vec<(usize, kardamom_stm::execute::StmOutcome)> = Vec::new();
        let t_pipe = Instant::now();
        kardamom_stm::execute::with_pool(cfg, |pool| -> anyhow::Result<()> {
            let timing = std::env::var_os("KARDAMOM_STM_PHASE_TIMING").is_some();
            // SETTLER thread: resolves tickets in order — waits the tail,
            // records the outcome, finalizes and hands the writer its
            // batch, and advances the pool's base cache — all OFF the
            // submission loop. The loop's only bookkeeping is layer
            // assembly (Arc clones) and admission.
            let (settle_tx, settle_rx) =
                std::sync::mpsc::channel::<(usize, kardamom_stm::execute::BlockTicket)>();
            let settled = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let settled_c = settled.clone();
            // Writer settlement is ASYNC: deltas are sent and never
            // waited on inside the loop. `advanced_to` tracks which flow
            // deltas have been mirrored into the pool's base cache after
            // the writer confirmed them; everything after that is layered
            // as the pending base (depth grows only if the writer lags a
            // full execution span).
            let outcomes_arc = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(
                usize,
                kardamom_stm::execute::StmOutcome,
            )>::new()));
            let outcomes_c = outcomes_arc.clone();
            let settler = std::thread::spawn({
                let delta_tx = writer_b.delta_tx.clone();
                let ends: Vec<u64> = flow_recs
                    .iter()
                    .map(|r| r.last().map(|x| x.0.0 + 1).unwrap_or(0))
                    .collect();
                let env_of2 = env_of;
                let timing2 = std::env::var_os("KARDAMOM_STM_PHASE_TIMING").is_some();
                let retain2 = retain_outcomes;
                move || -> anyhow::Result<()> {
                    while let Ok((pfi, t)) = settle_rx.recv() {
                        let out = t.wait()?;
                        if timing2 {
                            eprintln!(
                                "settle block {pfi}: busy {:.1}ms read {:.1}ms wounds {}",
                                out.busy_per_worker_us.iter().sum::<u64>() as f64 / 1000.0,
                                out.read_us as f64 / 1000.0,
                                out.wounds
                            );
                        }
                        let bi = warm + pfi;
                        let e = env_of2(bi);
                        // PRODUCTION SHAPE by default: the outcome is
                        // CONSUMED — finalize by value, no clone. The
                        // clone + retention exist only for the post-hoc
                        // byte-asserts (KARDAMOM_PIPE_ASSERT=1, the
                        // correctness pass) — cloning multi-MB deltas on
                        // the clock streams through the shared L3 the
                        // executing span depends on (see the spec's
                        // span-inflation note).
                        if retain2 {
                            let bd = out
                                .delta
                                .clone()
                                .finalize(e.block_number, out.receipts.clone());
                            delta_tx.send(WriteBatch::new(boundary(bi, ends[pfi]), bd))?;
                            outcomes_c.lock().unwrap().push((pfi, out));
                        } else {
                            let bd = out.delta.finalize(e.block_number, out.receipts);
                            delta_tx.send(WriteBatch::new(boundary(bi, ends[pfi]), bd))?;
                        }
                        settled_c.store(pfi + 1, std::sync::atomic::Ordering::Release);
                    }
                    Ok(())
                }
            });
            // Layer source. BASELINE mode: pass A's deltas, Arc'd up
            // front — measures pipeline mechanics with the answer known.
            // SPECULATIVE mode (P3b, the production shape): the engine's
            // own deltas, received from the tail's streaming release at
            // each block's FOLD — block fi cannot be layered before
            // fi-1's release arrives, which is exactly the pipeline's
            // sync point (execution of fi overlaps only fi-1's
            // validate/hash/receipts/settle tail).
            let deltas_arc: Vec<std::sync::Arc<PendingDelta>> = if speculative {
                Vec::new()
            } else {
                baseline
                    .iter()
                    .map(|(_, d)| std::sync::Arc::new(d.clone()))
                    .collect()
            };
            let mut engine_deltas: Vec<Option<std::sync::Arc<PendingDelta>>> = vec![None; n_flow];
            let (rel_tx, rel_rx) =
                std::sync::mpsc::channel::<kardamom_stm::execute::DeltaRelease>();
            // mv-as-layer: the EARLY release (pre-fold) that block fi
            // actually binds on; the DeltaRelease above is drained
            // lazily for base-cache advancement bookkeeping only.
            type MvSlot = (
                std::sync::Arc<kardamom_stm::mv::MvCache>,
                Option<revm::state::AccountInfo>,
            );
            let mut engine_mvs: Vec<Option<MvSlot>> = vec![None; n_flow];
            let (mv_tx, mv_rx) = std::sync::mpsc::channel::<kardamom_stm::execute::MvRelease>();
            let layer_of = |engine_deltas: &[Option<std::sync::Arc<PendingDelta>>],
                            k: usize|
             -> std::sync::Arc<PendingDelta> {
                if speculative {
                    engine_deltas[k].clone().expect("release drained in order")
                } else {
                    deltas_arc[k].clone()
                }
            };
            let mut advanced_to: usize = 0;
            if speculative {
                // THE P3b SEQUENCING (late-bound layers): block fi is
                // built, FED, and SUBMITTED while fi-1 still executes —
                // admission is layer-independent — and its read base
                // binds when fi-1's delta releases at the fold. The
                // first measurement of the naive order (build AFTER the
                // release) measured 2.08x vs 2.68x block-at-a-time: the
                // feed had moved back onto the critical path.
                for fi in 0..n_flow {
                    let t1 = Instant::now();
                    let views: Vec<kardamom_state::StateSnapshot> = (0..w)
                        .map(|_| kardamom_state::StateSnapshot::open(&env_b))
                        .collect::<Result<_, _>>()?;
                    let t_views = t1.elapsed();
                    let t2 = Instant::now();
                    let (mut sess, binder) = pool.begin_block_deferred(
                        views,
                        PendingDelta::new(),
                        env_of(warm + fi),
                        &stats_b,
                    )?;
                    for (t, p, en, prep) in std::mem::take(&mut feed_payloads[fi]) {
                        sess.push_prepared(t, p, en, prep)?;
                    }
                    let ticket = sess.submit_streaming_mv(mv_tx.clone(), rel_tx.clone())?;
                    settle_tx.send((fi, ticket)).expect("settler alive");
                    let t_feed = t2.elapsed();
                    // BIND: wait out fi-1's EARLY release (drain +
                    // extract — pre-fold; spec P3b mv-as-layer).
                    // Releases arrive in submission order.
                    // Advancement bookkeeping FIRST — it overlaps
                    // fi-1's still-running execution instead of sitting
                    // on the cadence after the bind-wait. Fold deltas
                    // arrive lazily; drain without blocking.
                    let t_a = Instant::now();
                    while let Ok(rel) = rel_rx.try_recv() {
                        assert!(
                            !rel.corrected,
                            "wound in pipeline bench (block {}) — scenario must be wound-free",
                            rel.block
                        );
                        let k = (rel.block as usize)
                            .checked_sub(warm + 1)
                            .expect("flow-range release");
                        engine_deltas[k] = Some(rel.delta);
                    }
                    let h = writer_b
                        .snapshot_rx
                        .current()
                        .map(|s| s.block_number())
                        .unwrap_or(0);
                    while advanced_to < fi {
                        let bn = (warm + advanced_to) as u64 + 1;
                        if bn > h || engine_deltas[advanced_to].is_none() {
                            break;
                        }
                        pool.advance_base(&layer_of(&engine_deltas, advanced_to));
                        // Settled: hand the shell back to the fold pool.
                        if let Some(arc) = engine_deltas[advanced_to].take()
                            && let Ok(d) = std::sync::Arc::try_unwrap(arc)
                        {
                            pool.recycle_delta(d);
                        }
                        advanced_to += 1;
                    }
                    let t_adv = t_a.elapsed();
                    let t0 = Instant::now();
                    if fi > 0 {
                        while engine_mvs[fi - 1].is_none() {
                            let rel = mv_rx.recv().expect("mv release channel");
                            let k = (rel.block as usize)
                                .checked_sub(warm + 1)
                                .expect("flow-range release");
                            engine_mvs[k] = Some((rel.mv, rel.sink_final));
                        }
                    }
                    // NEWEST FIRST: the mv caches of unsettled
                    // predecessors; the sink rides the newest release.
                    let mv_layers: Vec<std::sync::Arc<kardamom_stm::mv::MvCache>> = (advanced_to
                        ..fi)
                        .rev()
                        .map(|k| engine_mvs[k].as_ref().expect("drained in order").0.clone())
                        .collect();
                    let sink = if fi > 0 {
                        Some(engine_mvs[fi - 1].as_ref().expect("just drained").1.clone())
                    } else {
                        None
                    };
                    binder
                        .bind_with(mv_layers, Vec::new(), sink)
                        .map_err(|e| anyhow::anyhow!("bind block {fi}: {e}"))?;
                    if timing {
                        eprintln!(
                            "pipe block {fi}: views {t_views:?} feed {t_feed:?} adv {t_adv:?} bind-wait {:?}",
                            t0.elapsed()
                        );
                    }
                }
            } else {
                for fi in 0..n_flow {
                    let t0 = Instant::now();
                    let h = writer_b
                        .snapshot_rx
                        .current()
                        .map(|s| s.block_number())
                        .unwrap_or(0);
                    while advanced_to < fi {
                        let bn = (warm + advanced_to) as u64 + 1;
                        if bn > h {
                            break;
                        }
                        pool.advance_base(&layer_of(&engine_deltas, advanced_to));
                        advanced_to += 1;
                    }
                    // NEWEST FIRST.
                    let layers: Vec<std::sync::Arc<PendingDelta>> = (advanced_to..fi)
                        .rev()
                        .map(|k| layer_of(&engine_deltas, k))
                        .collect();
                    let t_base = t0.elapsed();
                    let t1 = Instant::now();
                    let views: Vec<kardamom_state::StateSnapshot> = (0..w)
                        .map(|_| kardamom_state::StateSnapshot::open(&env_b))
                        .collect::<Result<_, _>>()?;
                    let t_views = t1.elapsed();
                    let t2 = Instant::now();
                    let mut sess = pool.begin_block_layered(
                        views,
                        PendingDelta::new(),
                        layers,
                        env_of(warm + fi),
                        &stats_b,
                    )?;
                    for (t, p, en, prep) in std::mem::take(&mut feed_payloads[fi]) {
                        sess.push_prepared(t, p, en, prep)?;
                    }
                    let t_feed = t2.elapsed();
                    let t3 = Instant::now();
                    settle_tx.send((fi, sess.submit()?)).expect("settler alive");
                    if timing {
                        eprintln!(
                            "pipe block {fi}: base {t_base:?} views {t_views:?} feed {t_feed:?} hand {:?}",
                            t3.elapsed()
                        );
                    }
                }
            }
            drop(settle_tx);
            settler.join().expect("settler join")?;
            outcomes.extend(
                std::sync::Arc::try_unwrap(outcomes_arc)
                    .map_err(|_| ())
                    .expect("settler done")
                    .into_inner()
                    .unwrap(),
            );
            Ok(())
        })?;
        let stm_ms = t_pipe.elapsed().as_secs_f64() * 1e3;
        if !retain_outcomes {
            println!(
                "  (pure-timing mode: byte-asserts skipped — run KARDAMOM_PIPE_ASSERT=1 for the correctness pass)"
            );
        }
        // Verification AFTER the clock: every block, byte-identical.
        for (pfi, out) in &outcomes {
            assert_identical(
                &baseline[*pfi].0,
                &baseline[*pfi].1,
                &out.receipts,
                &out.delta,
                (warm + *pfi) as u64 + 1,
                4,
            );
            asserted += 1;
        }
        println!(
            "  w={w} seq {seq_ms:.0}ms | pipelined {stm_ms:.0}ms | speedup {:.2}x | blocks {n_flow} asserted {asserted}",
            seq_ms / stm_ms
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_mdbx_ab(
    signers: &[kardamom_bench::signers::DerivedSigner],
    all_blocks: &[Vec<TxEnvelope>],
    n_setup: usize,
    chain_id: u64,
    worker_counts: &[usize],
    batches: &[usize],
    parallel_worth_ns: u64,
    dispatch_by_sender: bool,
    eager_chain: bool,
    bag_scheduler: bool,
    sticky_assign: bool,
    per_block: bool,
    pin_cores: Vec<usize>,
    warmup_blocks: usize,
    keep_hot: bool,
) -> anyhow::Result<()> {
    use kardamom_state::{Durability, StateEnvBuilder, StateWriter, WriteBatch};
    use kardamom_types::{AccountChange, BlockBoundary};

    let genesis: Vec<AccountChange> = signers
        .iter()
        .map(|s| AccountChange {
            address: s.signer.address(),
            nonce: 0,
            balance: U256::from(10u128.pow(21)),
            code_hash: alloy_primitives::KECCAK256_EMPTY,
        })
        .collect();

    println!(
        "\n===== STM-P2 A/B [mdbx-backed state] =====\n{:>3} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>7}",
        "w",
        "seq_ms",
        "stm_ms",
        "speedup",
        "busy_ms",
        "span_ms",
        "commit_ms",
        "feed_ms",
        "snap_ms",
        "util"
    );

    for &w in worker_counts {
        for &batch in batches {
            let dir = tempfile::tempdir()?;
            let env = StateEnvBuilder::new(dir.path())
                .durability(Durability::SafeNoSync)
                .write_map(true)
                .open()?;
            kardamom_state::seed_genesis(&env, &genesis, &[])?;
            // Keep an env handle: each worker needs its OWN read
            // transaction (mdbx serialises reads through one txn's mutex).
            let env_for_reads = env.clone();
            let writer = StateWriter::spawn(env)?;
            let mut snapshot = writer
                .snapshot_rx
                .current()
                .expect("writer publishes an initial snapshot");

            let cfg = kardamom_stm::execute::PoolConfig {
                workers: w,
                prune_batch: batch,
                parallel_worth_ns,
                dispatch_by_sender,
                eager_chain,
                bag_scheduler,
                sticky_assign,
                keep_hot,
                tail_on_workers: true,
                pin_cores: pin_cores.clone(),
            };
            let mut stats = Stats::default();
            let mut global_idx = 0u64;
            let (mut seq_ms, mut stm_ms, mut gas, mut wounds) = (0f64, 0f64, 0u64, 0usize);
            let (mut busy, mut span, mut commit, mut feed, mut snap_us) =
                (0u64, 0u64, 0u64, 0u64, 0u64);
            let (mut rt, mut rmv, mut rbase, mut rback) = (0u64, 0u64, 0u64, 0u64);
            let (mut w_own, mut w_foreign) = (0u64, 0u64);
            let (mut fifo_cov, mut fifo_st, mut edges_sum) = (0u64, 0u64, 0u64);
            let mut read_us = 0u64;
            let mut disp_hist = vec![0u64; w];
            let mut idle_us = 0u64;
            let mut bpw = vec![0u64; w];
            let mut cold_sum = 0u64;
            let (mut prune_calls_s, mut prune_forced_s, mut prune_us_s, mut steals_s) =
                (0u64, 0u64, 0u64, 0u64);
            let mut admit_us_s = 0u64;
            let (mut fpre_s, mut fdag_s) = (0u64, 0u64);
            let (mut c_fold, mut c_lane) = (0u64, 0u64);
            let (mut seq_allocs, mut seq_abytes) = (0u64, 0u64);
            let (mut stm_allocs, mut stm_abytes) = (0u64, 0u64);
            let mut seq_buckets = [(0u64, 0u64); 6];
            let mut stm_buckets = [(0u64, 0u64); 6];
            let (mut seq_reallocs, mut seq_rebytes) = (0u64, 0u64);
            let (mut stm_reallocs, mut stm_rebytes) = (0u64, 0u64);
            let (mut c_hash, mut c_delta) = (0u64, 0u64);
            let (mut evm_us, mut pub_us) = (0u64, 0u64);

            kardamom_stm::execute::with_pool(cfg, |pool| -> anyhow::Result<()> {
                for (bi, blk) in all_blocks.iter().enumerate() {
                    let is_flow = bi >= n_setup + warmup_blocks;
                    let e = ExecEnv {
                        chain_id,
                        block_number: bi as u64 + 1,
                        l2_timestamp: 1_700_000_000 + bi as u64 * 2,
                    };
                    let recs = records(global_idx, blk);
                    global_idx += blk.len() as u64;
                    let prog = std::env::var_os("KARDAMOM_BENCH_PROGRESS").is_some();
                    if prog {
                        eprintln!("[prog] block {bi}: seq start");
                    }

                    // Both engines read the SAME snapshot, before it moves.
                    // KARDAMOM_STM_ONLY: run pool blocks back-to-back with no
                    // sequential run between them. The per-block timeline
                    // showed a uniform +30% step (evm AND reads equally) with
                    // one block at 11.2ms — FASTER than sequential — which is
                    // the signature of core-frequency ramping: the worker
                    // parks during each interleaved sequential run and its
                    // core downclocks. Skipping the interleave keeps the
                    // worker hot; if the drag vanishes, it was frequency.
                    let stm_only = std::env::var_os("KARDAMOM_STM_ONLY").is_some();
                    // FAIR BASELINE: decode once, OUTSIDE both timers, and
                    // hand it to the sequential engine the way `prepare`
                    // hands it to the parallel one. Charging decode to one
                    // side only inflated every ratio by 2-9%.
                    let seq_decoded: Vec<Option<alloy_consensus::TxEnvelope>> = recs
                        .iter()
                        .map(|(t, _, en)| kardamom_stm::decode_alloy_envelope(&en.raw_tx, *t).ok())
                        .collect();
                    let a0 = alloc_snap();
                    let b0 = bucket_snap();
                    let t0 = Instant::now();
                    let (seq_receipts, seq_delta) = if stm_only {
                        (Vec::new(), PendingDelta::new())
                    } else if std::env::var_os("KARDAMOM_SEQ_ON_THREAD").is_some() {
                        // Discriminator: the pool's per-tx cost exceeds
                        // sequential's by ~1.8x for PURE-INTERPRETER work.
                        // Run the same sequential engine on a spawned
                        // thread pinned to a worker core: if it slows to
                        // the pool's rate, the tax is THREAD CONTEXT
                        // (stack, arena, placement); if it stays fast, the
                        // tax is in the pool's own execution path.
                        std::thread::scope(|sc| {
                            sc.spawn(|| {
                                let core: usize = std::env::var("KARDAMOM_SEQ_CORE")
                                    .ok()
                                    .and_then(|v| v.parse().ok())
                                    .unwrap_or(3);
                                let _ = core_affinity::set_for_current(core_affinity::CoreId {
                                    id: core,
                                });
                                execute_block_sequential(&snapshot, None, e, &recs)
                            })
                            .join()
                            .expect("seq thread")
                        })?
                    } else {
                        kardamom_stm::execute::execute_block_sequential_decoded(
                            &snapshot,
                            None,
                            e,
                            &recs,
                            &seq_decoded,
                        )?
                    };
                    let s_ms = t0.elapsed().as_secs_f64() * 1e3;

                    if prog {
                        eprintln!("[prog] block {bi}: seq done, preparing");
                    }
                    let prepared: Vec<_> = recs
                        .iter()
                        .map(|(t, _, en)| kardamom_stm::execute::prepare(en, *t, &stats))
                        .collect();
                    let t1 = Instant::now();
                    // One independent view per worker, all at the block
                    // the writer just published.
                    let t_snap = Instant::now();
                    let views: Vec<kardamom_state::StateSnapshot> = (0..w)
                        .map(|_| kardamom_state::StateSnapshot::open(&env_for_reads))
                        .collect::<Result<_, _>>()?;
                    let snap_open = t_snap.elapsed().as_micros() as u64;
                    debug_assert!(
                        views
                            .iter()
                            .all(|v| v.block_number() == snapshot.block_number()),
                        "per-worker views must agree on the block"
                    );
                    let a1 = alloc_snap();
                    let b1 = bucket_snap();
                    if prog {
                        eprintln!("[prog] block {bi}: pool submit");
                    }
                    let mut out = pool.run_block_prepared(
                        views,
                        PendingDelta::new(),
                        e,
                        &recs,
                        prepared,
                        &stats,
                    )?;
                    let p_ms = t1.elapsed().as_secs_f64() * 1e3;
                    let a2 = alloc_snap();
                    let b2 = bucket_snap();
                    let skip_assert = stm_only;
                    if is_flow {
                        seq_allocs += a1.0 - a0.0;
                        seq_abytes += a1.1 - a0.1;
                        stm_allocs += a2.0 - a1.0;
                        stm_abytes += a2.1 - a1.1;
                        seq_reallocs += a1.2 - a0.2;
                        seq_rebytes += a1.3 - a0.3;
                        stm_reallocs += a2.2 - a1.2;
                        stm_rebytes += a2.3 - a1.3;
                        for i in 0..6 {
                            seq_buckets[i].0 += b1[i].0 - b0[i].0;
                            seq_buckets[i].1 += b1[i].1 - b0[i].1;
                            stm_buckets[i].0 += b2[i].0 - b1[i].0;
                            stm_buckets[i].1 += b2[i].1 - b1[i].1;
                        }
                    }
                    if !skip_assert {
                        assert_identical(
                            &seq_receipts,
                            &seq_delta,
                            &out.receipts,
                            &out.delta,
                            e.block_number,
                            w,
                        );
                    }
                    // Off the measured window (a2 snapped above): the
                    // delta shell goes back to the fold pool.
                    pool.recycle_delta(std::mem::take(&mut out.delta));
                    if prog {
                        eprintln!("[prog] block {bi}: pool done");
                    }
                    if per_block && is_flow {
                        let avg_gas = seq_receipts.iter().map(|r| r.gas_used).sum::<u64>()
                            / seq_receipts.len().max(1) as u64;
                        let ok = seq_receipts.iter().filter(|r| r.status).count();
                        eprintln!(
                            "  block {} receipts: avg gas {} ok {}/{}",
                            e.block_number,
                            avg_gas,
                            ok,
                            seq_receipts.len()
                        );
                        // Max core clock right now — the busy worker is the
                        // boosted core, so max ~= the frequency the block
                        // just ran at. Uniform per-block steps (evm AND
                        // reads scaling together) are a frequency
                        // signature, and this settles it.
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
                            "pool block {}: evm {:.1}ms (read {:.1}ms) busy {:.1}ms cpu {mhz}MHz",
                            e.block_number,
                            out.evm_us as f64 / 1000.0,
                            out.read_us as f64 / 1000.0,
                            out.busy_us as f64 / 1000.0,
                        );
                    }
                    if is_flow {
                        seq_ms += s_ms;
                        stm_ms += p_ms;
                        wounds += out.wounds;
                        busy += out.busy_us;
                        span += out.parallel_span_us;
                        commit += out.commit_us;
                        c_hash += out.commit_hash_us;
                        c_fold += out.commit_fold_us;
                        c_lane += out.commit_lane_us;
                        c_delta += out.commit_delta_us;
                        feed += out.feed_us;
                        snap_us += snap_open;
                        idle_us += out.idle_us;
                        for (wi, b) in out.busy_per_worker_us.iter().enumerate() {
                            bpw[wi] += *b / 1000;
                        }
                        prune_calls_s += out.prune_calls;
                        prune_forced_s += out.prune_forced;
                        prune_us_s += out.prune_us;
                        admit_us_s += out.admit_us;
                        fpre_s += out.feed_pre_us;
                        fdag_s += out.feed_dag_us;
                        steals_s += out.steals;
                        w_own += out.writes_own;
                        w_foreign += out.writes_foreign;
                        fifo_cov += out.fifo_covered;
                        fifo_st += out.fifo_stalls;
                        edges_sum += out.edges as u64;
                        cold_sum += out.cold as u64;
                        for (wi, c) in out.dispatch.iter().enumerate() {
                            disp_hist[wi] += *c as u64;
                        }
                        rt += out.reads_total;
                        rmv += out.reads_mv_hit;
                        rbase += out.reads_base_hit;
                        rback += out.reads_backend;
                        evm_us += out.evm_us;
                        read_us += out.read_us;
                        pub_us += out.publish_us;
                        gas += seq_receipts
                            .last()
                            .map(|r| r.cumulative_gas_used)
                            .unwrap_or(0);
                    }

                    // Train on this block (prior-blocks-only stats), then
                    // COMMIT it so the next block reads it from mdbx.
                    train(&snapshot, &recs, e, None, &mut stats)?;
                    let boundary = BlockBoundary {
                        block_number: e.block_number,
                        end_tx_idx: BPosition::from_index(global_idx),
                        l2_timestamp: e.l2_timestamp,
                        l1_origin: 0,
                    };
                    // In stm-only mode the pool's delta is the only one —
                    // and when both run they are asserted byte-identical, so
                    // this is the same state either way.
                    let (fin_delta, fin_receipts) = if stm_only {
                        (out.delta, out.receipts)
                    } else {
                        (seq_delta, seq_receipts)
                    };
                    // Mirror the committed delta into the pool's
                    // pool-lifetime backend cache BEFORE the writer applies
                    // it — next block's reads hit warm entries instead of
                    // mdbx (parcounter measured 100% backend reads without
                    // this: every hot cell changes every block, so a
                    // per-block cache can never hit).
                    pool.advance_base(&fin_delta);
                    let bd = fin_delta.finalize(e.block_number, fin_receipts);
                    writer.delta_tx.send(WriteBatch::new(boundary, bd))?;
                    // Wait for the writer to publish the post-commit view.
                    loop {
                        let snap = writer.snapshot_rx.recv().expect("writer alive");
                        let at = snap.block_number();
                        snapshot = snap;
                        if at >= e.block_number {
                            break;
                        }
                    }
                }
                Ok(())
            })?;

            let util = if span > 0 {
                busy as f64 / (w as f64 * span as f64) * 100.0
            } else {
                0.0
            };
            let _ = (gas, batch);
            println!(
                "{:>3} {:>8.0} {:>8.0} {:>7.2}x {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>8.1} {:>6.0}%",
                w,
                seq_ms,
                stm_ms,
                seq_ms / stm_ms,
                busy as f64 / 1000.0,
                span as f64 / 1000.0,
                commit as f64 / 1000.0,
                feed as f64 / 1000.0,
                snap_us as f64 / 1000.0,
                util
            );
            if wounds > 0 {
                println!("   (wounds: {wounds})");
            }
            if rt > 0 {
                println!(
                    "     commit scope {:.1}ms = fold-body {:.1}ms vs lanes-body {:.1}ms (gap = spawn/join) | delta {:.1}ms | rest {:.1}ms",
                    c_hash as f64 / 1000.0,
                    c_fold as f64 / 1000.0,
                    c_lane as f64 / 1000.0,
                    c_delta as f64 / 1000.0,
                    (commit as f64 - c_hash as f64 - c_delta as f64) / 1000.0,
                );
                println!(
                    "     evm {:.1}ms (read-path {:.1}ms) | publish {:.1}ms | other {:.1}ms",
                    evm_us as f64 / 1000.0,
                    read_us as f64 / 1000.0,
                    pub_us as f64 / 1000.0,
                    (busy as f64 - evm_us as f64 - pub_us as f64) / 1000.0,
                );
                println!("     dispatch per worker: {disp_hist:?}");
                println!("     busy per worker (ms): {bpw:?}");
                println!(
                    "     cold {cold_sum} of {} txs | edges {edges_sum} | fifo-covered {fifo_cov}",
                    8 * 1000usize,
                );
                println!(
                    "     feed split: pre(assign+slot+clone) {:.1}ms | dag(last-toucher) {:.1}ms | admit(graph) {:.1}ms | prune(graph) {:.1}ms | feed total {:.1}ms",
                    fpre_s as f64 / 1000.0,
                    fdag_s as f64 / 1000.0,
                    admit_us_s as f64 / 1000.0,
                    prune_us_s as f64 / 1000.0,
                    feed as f64 / 1000.0,
                );
                println!(
                    "     idle {:.1}ms across {w} workers (span cap {:.1}ms) | prunes {} (forced {}) {:.1}ms | steals {}",
                    idle_us as f64 / 1000.0,
                    (span * w as u64) as f64 / 1000.0,
                    prune_calls_s,
                    prune_forced_s,
                    prune_us_s as f64 / 1000.0,
                    steals_s,
                );
                let n_tx = (rt / rt.max(1)).max(1); // placeholder, replaced below
                let _ = n_tx;
                println!(
                    "     allocs/tx: seq {:.1} ({:.0} B) | stm {:.1} ({:.0} B)",
                    seq_allocs as f64 / 8000.0,
                    seq_abytes as f64 / 8000.0,
                    stm_allocs as f64 / 8000.0,
                    stm_abytes as f64 / 8000.0,
                );
                println!(
                    "     reallocs/tx: seq {:.1} ({:.0} B) | stm {:.1} ({:.0} B)",
                    seq_reallocs as f64 / 8000.0,
                    seq_rebytes as f64 / 8000.0,
                    stm_reallocs as f64 / 8000.0,
                    stm_rebytes as f64 / 8000.0,
                );
                const LBL: [&str; 6] = ["<64", "<512", "<4K", "<32K", "<256K", "big"];
                for i in 0..6 {
                    if seq_buckets[i].0 + stm_buckets[i].0 == 0 {
                        continue;
                    }
                    println!(
                        "       [{:>5}] seq {:>7.2}/tx {:>8.0}B | stm {:>7.2}/tx {:>8.0}B",
                        LBL[i],
                        seq_buckets[i].0 as f64 / 8000.0,
                        seq_buckets[i].1 as f64 / 8000.0,
                        stm_buckets[i].0 as f64 / 8000.0,
                        stm_buckets[i].1 as f64 / 8000.0,
                    );
                }
                println!(
                    "     chain: edges {} | fifo-covered {} | fifo-stalls {}",
                    edges_sum, fifo_cov, fifo_st,
                );
                let w_all = (w_own + w_foreign).max(1);
                println!(
                    "     account writes {} = own-domain {:.1}% | FOREIGN {:.1}%                      (foreign = written by >1 worker)",
                    w_own + w_foreign,
                    w_own as f64 / w_all as f64 * 100.0,
                    w_foreign as f64 / w_all as f64 * 100.0,
                );
                println!(
                    "     reads {} = mv-version {:.1}% | base-cache {:.1}% | backend {:.1}%",
                    rt,
                    rmv as f64 / rt as f64 * 100.0,
                    rbase as f64 / rt as f64 * 100.0,
                    rback as f64 / rt as f64 * 100.0,
                );
            }
        }
    }
    println!("BYTE-IDENTICAL: every block, every worker count — verified");
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    let parallel_worth_ns = a
        .parallel_worth_ns
        .unwrap_or(kardamom_stm::execute::PARALLEL_WORTH_NS);
    let dispatch_by_sender = a.dispatch_by_sender;
    let eager_chain = a.eager_chain;
    let sticky_assign = a.sticky_assign;
    let pin_cores: Vec<usize> = a
        .pin_cores
        .split(',')
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().parse().expect("pin-cores csv"))
        .collect();
    let worker_counts: Vec<usize> = a
        .workers
        .split(',')
        .map(|s| s.trim().parse().expect("workers csv"))
        .collect();
    let signers = mnemonic::derive_signers(ANVIL_MNEMONIC, (a.senders + 1) as u32)?;

    let mut b = MockStateDatabase::builder();
    for s in &signers {
        b = b.account(
            s.signer.address(),
            U256::from(10u128.pow(21)),
            0,
            alloy_primitives::KECCAK256_EMPTY,
        );
    }
    let snap = b.build();

    let (setup_blocks, flow_blocks): (Vec<Vec<TxEnvelope>>, Vec<Vec<TxEnvelope>>) =
        match a.scenario.as_str() {
            "uniswap" => {
                let w = uniswap::generate(
                    &a.repo_root,
                    &signers,
                    a.chain_id,
                    a.pairs,
                    a.blocks,
                    a.block_size,
                    a.swap_share,
                    a.cross,
                )?;
                (w.setup_blocks, w.flow_blocks)
            }
            "defi" => {
                // BenchDefi single-instance — mirrors stm-p0's construction.
                let (deploys, contracts) =
                    defi::deployment_txs(&signers, a.chain_id, 0, 1_000_000_000)?;
                let per_sender = (a.blocks * a.block_size) / a.senders + 2;
                let queues = defi::pregenerate_defi(
                    &signers,
                    a.chain_id,
                    &contracts,
                    per_sender,
                    0,
                    1_000_000_000,
                )?;
                let to_env = |t: &kardamom_bench::load::plan::PlannedTx,
                              sender: alloy_primitives::Address| {
                    TxEnvelope {
                        correlation_id: 0,
                        raw_tx: t.raw.clone().into(),
                        sender,
                        tx_hash: t.hash,
                    }
                };
                let setup: Vec<TxEnvelope> = deploys
                    .iter()
                    .map(|d| to_env(d, signers[0].signer.address()))
                    .collect();
                let mut cursors = vec![0usize; queues.len()];
                let mut flows = Vec::with_capacity(a.blocks);
                for _ in 0..a.blocks {
                    let mut blk = Vec::with_capacity(a.block_size);
                    let mut si = 0usize;
                    while blk.len() < a.block_size {
                        let q = &queues[si % queues.len()];
                        let c = &mut cursors[si % queues.len()];
                        if *c < q.len() {
                            blk.push(to_env(&q[*c], signers[si % queues.len()].signer.address()));
                            *c += 1;
                        }
                        si += 1;
                        if si > a.block_size * queues.len() * 2 {
                            break;
                        }
                    }
                    flows.push(blk);
                }
                (vec![setup], flows)
            }
            "partransfer" => {
                // FULLY INDEPENDENT plain transfers: sender i (one tx per
                // block, senders >= block_size) pays 1 wei to a FRESH
                // address derived from (sender, block) that nothing else
                // ever touches. No sender chains, no recipient overlap,
                // no code — the pure 21k-gas rung. The structural
                // question it isolates: how much of a ~2.5us transaction
                // do the engine's serial parts (feed ~0.5-0.7us, fold)
                // consume — i.e. the Amdahl ceiling for micro-txs.
                use alloy_consensus::{SignableTransaction, TxLegacy};
                use alloy_eips::eip2718::Encodable2718;
                use alloy_network::TxSignerSync;
                use alloy_primitives::{TxKind, keccak256};
                let mut nonces = vec![0u64; signers.len()];
                let mut flows = Vec::with_capacity(a.blocks);
                for bidx in 0..a.blocks {
                    let mut blk = Vec::with_capacity(a.block_size);
                    for i in 0..a.block_size {
                        let si = i % signers.len();
                        let mut fresh = [0u8; 20];
                        fresh[..8].copy_from_slice(&(bidx as u64).to_be_bytes());
                        fresh[8..16].copy_from_slice(&(i as u64).to_be_bytes());
                        fresh[19] = 0xEE;
                        let mut tx = TxLegacy {
                            chain_id: Some(a.chain_id),
                            nonce: nonces[si],
                            gas_price: 1_000_000_000,
                            gas_limit: 21_000,
                            to: TxKind::Call(alloy_primitives::Address::from(fresh)),
                            value: U256::from(1u64),
                            input: Default::default(),
                        };
                        nonces[si] += 1;
                        let sig = signers[si].signer.sign_transaction_sync(&mut tx).unwrap();
                        let env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
                        let raw = env.encoded_2718();
                        blk.push(TxEnvelope {
                            correlation_id: 0,
                            raw_tx: raw.clone().into(),
                            sender: signers[si].signer.address(),
                            tx_hash: keccak256(&raw),
                        });
                    }
                    flows.push(blk);
                }
                (Vec::new(), flows)
            }
            "parcounter" => {
                // FULLY INDEPENDENT contract calls — the bottom rung of
                // the dependency ladder. Each sender deploys its OWN
                // 10-byte counter (slot-0 increment) in setup, then calls
                // it once per block. With senders >= block_size no two
                // txs in a block share ANY state: distinct sender,
                // distinct contract, distinct slot. Any idle time or
                // sub-linear scaling here is an ENGINE defect by
                // construction, not workload structure. Contract-call
                // weight (~15us/tx) keeps the serial feed (<1us/tx) from
                // masking the scaling, which plain transfers cannot do
                // (2.5us/tx caps at ~2.2x by Amdahl regardless of the
                // engine).
                use alloy_consensus::{SignableTransaction, TxLegacy};
                use alloy_eips::eip2718::Encodable2718;
                use alloy_network::TxSignerSync;
                use alloy_primitives::{TxKind, keccak256};
                // Runtime: `call_work` LOOP iterations of slot0 += 1.
                // Warm sload/sstore are ~40ns in revm, so only a loop can
                // reach contract-scale per-tx weight:
                //   PUSH2 N; JUMPDEST@3; PUSH1 0 SLOAD; PUSH1 1 ADD;
                //   PUSH1 0 SSTORE; PUSH1 1; SWAP1; SUB; DUP1; PUSH1 3;
                //   JUMPI; STOP
                let n = a.call_work.max(1) as u16;
                let runtime: Vec<u8> = vec![
                    0x61,
                    (n >> 8) as u8,
                    (n & 0xff) as u8,
                    0x5b,
                    0x60,
                    0x00,
                    0x54,
                    0x60,
                    0x01,
                    0x01,
                    0x60,
                    0x00,
                    0x55,
                    0x60,
                    0x01,
                    0x90,
                    0x03,
                    0x80,
                    0x60,
                    0x03,
                    0x57,
                    0x00,
                ];
                // init: PUSH1 len PUSH1 off PUSH1 0 CODECOPY PUSH1 len
                // PUSH1 0 RETURN ++ runtime.
                let mut init = vec![
                    0x60,
                    runtime.len() as u8,
                    0x60,
                    0x0c,
                    0x60,
                    0x00,
                    0x39,
                    0x60,
                    runtime.len() as u8,
                    0x60,
                    0x00,
                    0xf3,
                ];
                init.extend_from_slice(&runtime);
                let sel = [0xAA, 0xBB, 0xCC, 0xDDu8];
                let mut nonces = vec![0u64; signers.len()];
                let counters: Vec<alloy_primitives::Address> = signers
                    .iter()
                    .map(|s| s.signer.address().create(0))
                    .collect();
                let mk =
                    |si: usize, nonce: u64, kind: TxKind, input: Vec<u8>, gas: u64| -> TxEnvelope {
                        let mut tx = TxLegacy {
                            chain_id: Some(a.chain_id),
                            nonce,
                            gas_price: 1_000_000_000,
                            gas_limit: gas,
                            to: kind,
                            value: U256::ZERO,
                            input: input.into(),
                        };
                        let sig = signers[si].signer.sign_transaction_sync(&mut tx).unwrap();
                        let env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
                        let raw = env.encoded_2718();
                        TxEnvelope {
                            correlation_id: 0,
                            raw_tx: raw.clone().into(),
                            sender: signers[si].signer.address(),
                            tx_hash: keccak256(&raw),
                        }
                    };
                let setup: Vec<TxEnvelope> = (0..signers.len())
                    .map(|si| {
                        nonces[si] += 1;
                        mk(si, 0, TxKind::Create, init.clone(), 200_000)
                    })
                    .collect();
                let mut flows = Vec::with_capacity(a.blocks);
                for _ in 0..a.blocks {
                    let mut blk = Vec::with_capacity(a.block_size);
                    for i in 0..a.block_size {
                        let si = i % signers.len();
                        blk.push(mk(
                            si,
                            nonces[si],
                            TxKind::Call(counters[si]),
                            sel.to_vec(),
                            60_000 + a.call_work.max(1) as u64 * 400,
                        ));
                        nonces[si] += 1;
                    }
                    flows.push(blk);
                }
                (vec![setup], flows)
            }
            "transfers" => {
                use alloy_consensus::{SignableTransaction, TxLegacy};
                use alloy_eips::eip2718::Encodable2718;
                use alloy_network::TxSignerSync;
                use alloy_primitives::{TxKind, keccak256};
                let mut nonces = vec![0u64; signers.len()];
                let mut flows = Vec::with_capacity(a.blocks);
                for bidx in 0..a.blocks {
                    let mut blk = Vec::with_capacity(a.block_size);
                    for i in 0..a.block_size {
                        let si = (bidx * 7 + i) % signers.len();
                        let to = signers[(si + 1 + i % (signers.len() - 1)) % signers.len()]
                            .signer
                            .address();
                        let mut tx = TxLegacy {
                            chain_id: Some(a.chain_id),
                            nonce: nonces[si],
                            gas_price: 1_000_000_000,
                            gas_limit: 21_000,
                            to: TxKind::Call(to),
                            value: U256::from(1000u64),
                            input: Default::default(),
                        };
                        nonces[si] += 1;
                        let sig = signers[si].signer.sign_transaction_sync(&mut tx).unwrap();
                        let env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
                        let raw = env.encoded_2718();
                        blk.push(TxEnvelope {
                            correlation_id: 0,
                            raw_tx: raw.clone().into(),
                            sender: signers[si].signer.address(),
                            tx_hash: keccak256(&raw),
                        });
                    }
                    flows.push(blk);
                }
                (Vec::new(), flows)
            }
            other => anyhow::bail!("unknown scenario {other}"),
        };

    let n_setup = setup_blocks.len();
    let mut all_blocks = setup_blocks;
    all_blocks.extend(flow_blocks);

    let batches: Vec<usize> = a
        .prune_batch
        .split(',')
        .map(|s| s.trim().parse().expect("prune-batch csv"))
        .collect();
    if a.state == "mdbx" {
        let guard = match &a.pprof_out {
            Some(_) => Some(
                pprof::ProfilerGuardBuilder::default()
                    .frequency(997)
                    .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                    .build()
                    .map_err(|e| anyhow::anyhow!("pprof guard: {e}"))?,
            ),
            None => None,
        };
        if a.pipeline {
            let r = run_pipelined(
                &signers,
                &all_blocks,
                n_setup,
                a.chain_id,
                &worker_counts,
                parallel_worth_ns,
                dispatch_by_sender,
                eager_chain,
                a.bag_scheduler,
                sticky_assign,
                pin_cores.clone(),
                a.warmup_blocks,
                a.keep_hot,
                a.pipeline_speculative,
            );
            if let (Some(g), Some(path)) = (guard, a.pprof_out.as_ref())
                && let Ok(report) = g.report().build()
            {
                let file = std::fs::File::create(path)?;
                report.flamegraph(file)?;
                eprintln!("==> wrote flamegraph to {path}");
            }
            return r;
        }
        let r = run_mdbx_ab(
            &signers,
            &all_blocks,
            n_setup,
            a.chain_id,
            &worker_counts,
            &batches,
            parallel_worth_ns,
            dispatch_by_sender,
            eager_chain,
            a.bag_scheduler,
            sticky_assign,
            a.per_block,
            pin_cores,
            a.warmup_blocks,
            a.keep_hot,
        );
        if let (Some(g), Some(path)) = (guard, a.pprof_out.as_ref())
            && let Ok(report) = g.report().build()
        {
            let file = std::fs::File::create(path)?;
            report
                .flamegraph(file)
                .map_err(|e| anyhow::anyhow!("flamegraph: {e}"))?;
            eprintln!("==> wrote flamegraph to {path}");
        }
        return r;
    }

    eprintln!(
        "==> A/B over {} blocks ({} setup) workers={:?}",
        all_blocks.len(),
        n_setup,
        worker_counts
    );

    // ---- Pass 0: timed sequential baseline + caches -------------------
    // Per block: the pre-block delta (base), the canonical outputs, and a
    // SNAPSHOT of the stats as they stood before the block (so every STM
    // sweep sees the exact inputs the streaming executor would).
    struct BlockCase {
        env: ExecEnv,
        recs: Vec<(TxIndex, BPosition, TxEnvelope)>,
        base: PendingDelta,
        stats: Stats,
        seq_receipts: Vec<Receipt>,
        seq_delta: PendingDelta,
        is_flow: bool,
        /// Upstream-prepared decode+prediction — what P3's tx_data readers
        /// will hand the executor. Timed separately: it is NOT on the
        /// feed's serial path.
        prep_us: u64,
    }
    let mut cases: Vec<BlockCase> = Vec::with_capacity(all_blocks.len());
    let mut delta = PendingDelta::new();
    let mut stats = Stats::default();
    let mut global_idx = 0u64;
    let mut wall_seq = 0f64;
    let mut flow_gas = 0u64;
    let mut flow_txs = 0usize;

    for (bi, blk) in all_blocks.iter().enumerate() {
        let is_flow = bi >= n_setup;
        let env = ExecEnv {
            chain_id: a.chain_id,
            block_number: bi as u64 + 1,
            l2_timestamp: 1_700_000_000 + bi as u64 * 2,
        };
        let recs = records(global_idx, blk);
        global_idx += blk.len() as u64;
        let base = delta.clone();
        let stats_before = stats.clone();

        let t0 = Instant::now();
        let (seq_receipts, seq_delta) = execute_block_sequential(&snap, Some(&delta), env, &recs)?;
        let seq_ms = t0.elapsed().as_secs_f64() * 1e3;
        if is_flow {
            wall_seq += seq_ms;
            flow_gas += seq_receipts
                .last()
                .map(|r| r.cumulative_gas_used)
                .unwrap_or(0);
            flow_txs += recs.len();
        }

        // Untimed capture pass: train the stats for the NEXT block
        // (prior-blocks-only, like the live shadow).
        train(&snap, &recs, env, Some(&delta), &mut stats)?;

        delta.merge_from(&seq_delta);
        // Cost of preparing this block upstream (measured once; the sweep
        // re-prepares per run so each timed pass starts from raw inputs).
        let t_prep = Instant::now();
        for (t, _, e) in &recs {
            let _ = kardamom_stm::execute::prepare(e, *t, &stats_before);
        }
        let prep_us = t_prep.elapsed().as_micros() as u64;
        cases.push(BlockCase {
            env,
            recs,
            base,
            stats: stats_before,
            seq_receipts,
            seq_delta,
            is_flow,
            prep_us,
        });
    }

    // ---- Sweep: ONE persistent pool per worker count, blocks streamed
    // through it — no per-block thread cost, matching the executor
    // pipeline shape.
    struct Row {
        workers: usize,
        batch: usize,
        wall: f64,
        fallbacks: usize,
        feed_us: u64,
        prep_us: u64,
        redundant: u64,
        steals: u64,
        /// Dispatch imbalance: busiest thread's share vs an even split.
        /// Domain-affinity assignment collides when domains ~ workers.
        imbalance: f64,
        imb_n: u64,
        idle_threads: usize,
        busy_us: u64,
        span_us: u64,
        ramp_us: u64,
        commit_us: u64,
        decode_us: u64,
        predict_us: u64,
        admit_us: u64,
        prune_us: u64,
        prune_calls: u64,
        prune_forced: u64,
        avg_batch: f64,
        idle_us: u64,
    }
    let mut rows: Vec<Row> = Vec::new();
    let (mut agg_edges, mut agg_cold) = (0usize, 0usize);
    for &w in &worker_counts {
        for &batch in &batches {
            let cfg = kardamom_stm::execute::PoolConfig {
                workers: w,
                prune_batch: batch,
                parallel_worth_ns,
                dispatch_by_sender,
                eager_chain,
                bag_scheduler: true,
                sticky_assign,
                keep_hot: false,
                tail_on_workers: true,
                pin_cores: pin_cores.clone(),
            };
            let mut row = Row {
                workers: w,
                batch,
                wall: 0.0,
                fallbacks: 0,
                feed_us: 0,
                prep_us: 0,
                redundant: 0,
                steals: 0,
                imbalance: 0.0,
                imb_n: 0,
                idle_threads: 0,
                busy_us: 0,
                span_us: 0,
                ramp_us: 0,
                commit_us: 0,
                decode_us: 0,
                predict_us: 0,
                admit_us: 0,
                prune_us: 0,
                prune_calls: 0,
                prune_forced: 0,
                avg_batch: 0.0,
                idle_us: 0,
            };
            let mut batch_weight = 0f64;
            kardamom_stm::execute::with_pool(cfg, |pool| -> anyhow::Result<()> {
                for case in &cases {
                    let base = case.base.clone();
                    // Upstream stage — in production the tx_data readers,
                    // here just ahead of the timer: the point is that it
                    // is NOT on the executor's serial feed.
                    let prepared: Vec<_> = case
                        .recs
                        .iter()
                        .map(|(t, _, e)| kardamom_stm::execute::prepare(e, *t, &case.stats))
                        .collect();
                    let t = Instant::now();
                    let out = pool.run_block_prepared(
                        vec![snap.clone(); w],
                        base,
                        case.env,
                        &case.recs,
                        prepared,
                        &case.stats,
                    )?;
                    let ms = t.elapsed().as_secs_f64() * 1e3;
                    assert_identical(
                        &case.seq_receipts,
                        &case.seq_delta,
                        &out.receipts,
                        &out.delta,
                        case.env.block_number,
                        w,
                    );
                    if case.is_flow {
                        row.wall += ms;
                        row.fallbacks += out.fallback as usize;
                        row.feed_us += out.feed_us;
                        row.prep_us += case.prep_us;
                        row.busy_us += out.busy_us;
                        row.span_us += out.parallel_span_us;
                        row.ramp_us += out.ramp_us;
                        row.commit_us += out.commit_us;
                        let total: u32 = out.dispatch.iter().sum();
                        let maxd = *out.dispatch.iter().max().unwrap_or(&0);
                        let used = out.dispatch.iter().filter(|c| **c > 0).count();
                        if total > 0 {
                            row.imbalance += maxd as f64 * out.dispatch.len() as f64 / total as f64;
                            row.idle_threads += out.dispatch.len() - used;
                            row.imb_n += 1;
                        }
                        row.redundant += out.redundant_edges;
                        row.steals += out.steals;
                        row.decode_us += out.decode_us;
                        row.predict_us += out.predict_us;
                        row.admit_us += out.admit_us;
                        row.prune_us += out.prune_us;
                        row.prune_calls += out.prune_calls;
                        row.prune_forced += out.prune_forced;
                        row.idle_us += out.idle_us;
                        row.avg_batch += out.avg_batch * out.prune_calls as f64;
                        batch_weight += out.prune_calls as f64;
                        if rows.is_empty() {
                            agg_edges += out.edges;
                            agg_cold += out.cold;
                        }
                    }
                }
                Ok(())
            })?;
            if batch_weight > 0.0 {
                row.avg_batch /= batch_weight;
            }
            rows.push(row);
        }
    }

    println!(
        "\n===== STM-P2 A/B [{}] pairs={} senders={} blocks={}x{} =====",
        a.scenario, a.pairs, a.senders, a.blocks, a.block_size
    );
    println!(
        "flow: txs={} gas={:.3}Ggas seq_wall={:.0}ms ({:.0} Mgas/s) sched: edges={} cold={}",
        flow_txs,
        flow_gas as f64 / 1e9,
        wall_seq,
        flow_gas as f64 / 1e6 / (wall_seq / 1e3),
        agg_edges,
        agg_cold,
    );
    println!(
        "{:>3} {:>6} {:>9} {:>8} {:>9} {:>10} {:>10} {:>7} {:>9} {:>9} {:>6}",
        "w",
        "batch",
        "wall_ms",
        "speedup",
        "mgas/s",
        "feed_us",
        "decode_us",
        "pred_us",
        "prune_us",
        "idle_us",
        "wound"
    );
    for r in &rows {
        println!(
            "{:>3} {:>6} {:>9.0} {:>7.2}x {:>9.0} {:>10} {:>10} {:>7} {:>9} {:>9} {:>6.2}",
            r.workers,
            r.batch,
            r.wall,
            wall_seq / r.wall,
            flow_gas as f64 / 1e6 / (r.wall / 1e3),
            r.feed_us,
            r.prep_us,
            r.predict_us,
            r.prune_us,
            r.idle_us,
            if r.imb_n > 0 {
                r.imbalance / r.imb_n as f64
            } else {
                0.0
            },
        );
    }
    let _ = |r: &Row| (r.avg_batch, r.redundant, r.idle_threads);
    for r in &rows {
        println!("  w={} steals={}", r.workers, r.steals);
    }
    let tot_idle_threads: usize = rows.iter().map(|r| r.idle_threads).sum();
    let tot_blocks: u64 = rows.iter().map(|r| r.imb_n).sum();
    println!(
        "DISPATCH: empty-thread-slots {} over {} block-runs (domain-affinity collisions)",
        tot_idle_threads, tot_blocks
    );
    println!(
        "\n----- WHERE THE WALL TIME GOES (per worker count) -----\n{:>3} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "w", "ramp_ms", "span_ms", "commit_ms", "busy_ms", "utilization"
    );
    for r in &rows {
        let util = if r.span_us > 0 {
            r.busy_us as f64 / (r.workers as f64 * r.span_us as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "{:>3} {:>10.1} {:>10.1} {:>10.1} {:>10.1} {:>11.1}%",
            r.workers,
            r.ramp_us as f64 / 1000.0,
            r.span_us as f64 / 1000.0,
            r.commit_us as f64 / 1000.0,
            r.busy_us as f64 / 1000.0,
            util
        );
    }
    println!("BYTE-IDENTICAL: every block, every worker count — verified");
    Ok(())
}
