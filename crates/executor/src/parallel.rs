//! Block-at-a-time Block-STM execution for the executor role
//! (`--parallel-execution`).
//!
//! The engine's exec thread buffers a block's records and hands them to a
//! [`BlockExec`] strategy at the boundary. This is the same seam the
//! validator's parallel verification uses. This module's strategy sends
//! each block to a pool-server thread that owns a persistent
//! [`kardamom_stm`] worker pool. (`with_pool` is scoped and its handle
//! borrows, so it cannot be captured in the `'static` strategy closure.
//! The server thread is the bridge, and the STM engine's internals stay
//! untouched.)
//!
//! ## Deposits: segmentation
//!
//! The STM engine executes transactions only. A block's records split
//! into maximal Tx-runs and Deposit singletons at their canonical
//! positions. Tx-runs execute on the pool (the base layer is the actor's
//! parent layer, with prior segments layered above it). Deposits execute
//! serially through the same [`execute_record_in_scope`] deposit arm the
//! streaming path uses. Every segment's delta becomes a read layer for
//! the next. Indices and cumulative gas stay block-global throughout: the
//! strategy re-derives both across segments in one final pass, exactly
//! like the validator's batch fold.
//!
//! ## BAL parity (merge gate)
//!
//! With BAL publication on, the strategy's output must be wire-identical
//! to the streaming capture, because the validator cross-checks the
//! published artifact. Tx fragments are captured inside the STM engine
//! at block-global indices (the fee sink is materialized in its commit
//! pass, and wound repairs are re-captured canonically). Deposits
//! capture through the shared deposit path, and [`merge_bal_fragments`]
//! folds everything in canonical order.
//!
//! ## Decline gate
//!
//! Small and cheap blocks skip the pool entirely (`parallel_worth_it`)
//! and run the sequential capture driver. This still teaches the gate
//! (`learn_sequential`), so a pool that saw cheap transfers re-enters
//! parallel execution when heavier blocks arrive.

use std::num::NonZeroUsize;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender, bounded};

use kardamom_engine::actor::{BlockExec, BlockExecOutput, BufferedRecord};
use kardamom_engine::bal_ladder::merge_bal_fragments;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;
use kardamom_engine::executor::Executor as ExecCore;
use kardamom_engine::stateless::{execute_block_capture, execute_record_in_scope};
use kardamom_footprint::classifier::Stats;
use kardamom_stm::execute::{PoolConfig, PoolHandle, with_pool};
use kardamom_types::{Receipt, StateDatabase};

/// A state snapshot the STM pool can fork and share across worker
/// threads. This groups the bound this module repeats on
/// [`stm_block_exec`], `pool_server`, and `run_one`, so it is written
/// once.
pub trait SharedState: StateDatabase + Clone + Sync + 'static {}
impl<T: StateDatabase + Clone + Sync + 'static> SharedState for T {}

/// One block's work order for the pool server.
struct BlockRequest<S> {
    snapshot: S,
    parent: Option<PendingDelta>,
    records: Vec<BufferedRecord>,
    env: ExecEnv,
    block: u64,
    reply: Sender<Result<BlockExecOutput, ExecutorError>>,
}

/// Executor-facing STM configuration (from the CLI to the pool).
#[derive(Debug, Clone)]
pub struct StmExecConfig {
    /// Non-zero: a pool with 0 workers would never make progress.
    /// `wiring::build_block_exec` computes this as a `NonZeroUsize`.
    pub workers: NonZeroUsize,
    pub pin_cores: Vec<usize>,
    pub keep_hot: bool,
}

/// Spawn the pool-server thread, and return the [`BlockExec`] strategy
/// that feeds it. The pool (workers, tail lanes, reaper) lives for the
/// server thread's lifetime. Dropping the strategy closes the request
/// channel and shuts the server down.
///
/// # Panics
///
/// Panics if the pool-server thread cannot be spawned (an OS resource
/// failure).
#[must_use]
pub fn stm_block_exec<S>(cfg: StmExecConfig) -> BlockExec<S>
where
    S: SharedState,
{
    let (req_tx, req_rx) = bounded::<BlockRequest<S>>(1);
    std::thread::Builder::new()
        .name("stm-pool-server".into())
        .spawn(move || pool_server(&cfg, &req_rx))
        .expect("spawn stm pool server");

    Box::new(
        move |snapshot: &S,
              parent: Option<&PendingDelta>,
              records: &[BufferedRecord],
              env: ExecEnv,
              block: u64| {
            let (reply_tx, reply_rx) = bounded(1);
            req_tx
                .send(BlockRequest {
                    snapshot: snapshot.clone(),
                    parent: parent.cloned(),
                    records: records.to_vec(),
                    env,
                    block,
                    reply: reply_tx,
                })
                .map_err(|_| ExecutorError::State("stm pool server gone".into()))?;
            reply_rx
                .recv()
                .map_err(|_| ExecutorError::State("stm pool server dropped a block".into()))?
        },
    )
}

fn pool_server<S: SharedState>(cfg: &StmExecConfig, rx: &Receiver<BlockRequest<S>>) {
    let pool_cfg = PoolConfig {
        workers: cfg.workers.get(),
        pin_cores: cfg.pin_cores.clone(),
        keep_hot: cfg.keep_hot,
        ..PoolConfig::default()
    };
    // Footprint stats persist across blocks. Each block's observed write
    // sets train the next block's predictions (cold start is
    // serial-heavy; the decline gate covers it).
    let stats = Stats::default();
    let workers = pool_cfg.workers;
    with_pool::<S, _>(pool_cfg, |pool| {
        while let Ok(req) = rx.recv() {
            let out = run_one(pool, workers, &stats, &req);
            // If the strategy is gone mid-shutdown, drop the reply: keep
            // draining until the request channel closes.
            let _ = req.reply.send(out);
        }
    });
    tracing::info!("stm pool server stopped");
}

/// A block's records split at deposit and cross-chain-delivery positions.
enum Segment {
    /// Consecutive transactions, starting at block-global record index
    /// `start` (0-based).
    Txs {
        start: u64,
        txs: Vec<(
            kardamom_engine::exec_types::TxIndex,
            kardamom_types::BPosition,
            kardamom_types::TxEnvelope,
        )>,
    },
    /// One deposit or cross-chain delivery at block-global record index
    /// `at`. Both are non-Tx records the STM engine does not execute;
    /// they run serially through the same shared scope arm.
    Singleton { at: u64, rec: BufferedRecord },
}

fn segment(records: &[BufferedRecord]) -> Vec<Segment> {
    let mut segs = Vec::new();
    for (i, rec) in records.iter().enumerate() {
        push_segment(&mut segs, i as u64, rec);
    }
    segs
}

/// Fold one record into `segs`. It extends the last run of
/// consecutive transactions, starts a new run, or pushes a singleton
/// for a deposit or a cross-chain delivery.
fn push_segment(segs: &mut Vec<Segment>, i: u64, rec: &BufferedRecord) {
    match rec {
        BufferedRecord::Tx {
            tx_idx,
            envelope,
            position,
        } => match segs.last_mut() {
            Some(Segment::Txs { txs, .. }) => txs.push((*tx_idx, *position, envelope.clone())),
            _ => segs.push(Segment::Txs {
                start: i,
                txs: vec![(*tx_idx, *position, envelope.clone())],
            }),
        },
        BufferedRecord::Deposit { .. } | BufferedRecord::XChain { .. } => {
            segs.push(Segment::Singleton {
                at: i,
                rec: rec.clone(),
            });
        }
    }
}

fn run_one<S: SharedState>(
    pool: &PoolHandle<'_, S>,
    workers: usize,
    stats: &Stats,
    req: &BlockRequest<S>,
) -> Result<BlockExecOutput, ExecutorError> {
    if req.records.is_empty() {
        return Ok(BlockExecOutput {
            receipts: Vec::new(),
            delta: PendingDelta::new(),
            // The boundary handoff publishes every block, empty included
            // (streaming hands off an empty per-block Bal the same way).
            bal: Some(revm::state::bal::Bal::new()),
        });
    }

    // Decline gate: same statistic, same threshold, and same learning
    // discipline as the pool's own gate. The sequential arm here is the
    // shared capture driver (deposits included), so it needs no
    // segmentation and no fixups.
    if !pool.parallel_worth_it() {
        let started = Instant::now();
        let out = execute_block_capture(&req.snapshot, req.parent.as_ref(), &req.records, req.env);
        pool.learn_sequential(started.elapsed(), req.records.len());
        return out;
    }

    let mut accum = SegAccum::new(req.records.len());
    for seg in segment(&req.records) {
        process_segment(pool, workers, stats, req, seg, &mut accum)?;
    }
    let SegAccum {
        seg_layers,
        receipts,
        frags,
        ..
    } = accum;

    // Block delta: the segments' writes folded in canonical order. The
    // parent layer stays out, because the exec thread owns cross-block
    // accounting; double-counting it would be a silent divergence.
    let mut delta = PendingDelta::new();
    for l in &seg_layers {
        delta.merge_from(l);
    }
    Ok(BlockExecOutput {
        receipts,
        delta,
        bal: Some(merge_bal_fragments(frags)),
    })
}

/// The state `run_one` threads across a block's segments, in canonical
/// order: each segment's read layer, its receipts so far, its BAL
/// fragments, and the running cumulative gas total.
struct SegAccum {
    seg_layers: Vec<std::sync::Arc<PendingDelta>>,
    receipts: Vec<Receipt>,
    frags: Vec<revm::state::bal::Bal>,
    cumulative: u64,
}

impl SegAccum {
    fn new(records_len: usize) -> Self {
        Self {
            seg_layers: Vec::new(),
            receipts: Vec::with_capacity(records_len),
            frags: Vec::new(),
            cumulative: 0,
        }
    }
}

/// Execute one segment and fold its results into `accum`.
fn process_segment<S: SharedState>(
    pool: &PoolHandle<'_, S>,
    workers: usize,
    stats: &Stats,
    req: &BlockRequest<S>,
    seg: Segment,
    accum: &mut SegAccum,
) -> Result<(), ExecutorError> {
    match seg {
        Segment::Txs { start, txs } => {
            process_txs_segment(pool, workers, stats, req, start, txs, accum)
        }
        Segment::Singleton { at, rec } => process_singleton_segment(req, at, &rec, accum),
    }
}

/// Execute a Tx-run segment on the pool, and fold its results into
/// `accum`.
fn process_txs_segment<S: SharedState>(
    pool: &PoolHandle<'_, S>,
    workers: usize,
    stats: &Stats,
    req: &BlockRequest<S>,
    start: u64,
    txs: Vec<(
        kardamom_engine::exec_types::TxIndex,
        kardamom_types::BPosition,
        kardamom_types::TxEnvelope,
    )>,
    accum: &mut SegAccum,
) -> Result<(), ExecutorError> {
    // One independent snapshot per worker (`fork_view`). A refused fork
    // shares the strategy's view: correct, but serialized, and it is
    // counted.
    let snapshots: Vec<S> = (0..workers)
        .map(|_| {
            req.snapshot.fork_view().unwrap_or_else(|| {
                metrics::counter!("kardamom_executor_snapshot_fork_fallback_total").increment(1);
                req.snapshot.clone()
            })
        })
        .collect();
    // Read stack: the base layer is the actor's parent layer (unsettled
    // predecessor blocks). Prior segments layer above it, newest first.
    let base = req.parent.clone().unwrap_or_default();
    let layers: Vec<_> = accum.seg_layers.iter().rev().cloned().collect();
    let mut session =
        pool.begin_block_layered_bal(snapshots, base, layers, req.env, stats, start)?;
    txs.into_iter()
        .try_for_each(|(tx_idx, position, envelope)| session.push_tx(tx_idx, position, envelope))?;
    let out = session.seal()?;
    if out.wounds > 0 {
        tracing::warn!(
            block = req.block,
            wounds = out.wounds,
            "stm: wound repaired during parallel execution"
        );
    }
    // Receipts come back run-local. Re-derive block-global
    // transaction_index and cumulative gas in arrival order.
    out.receipts.into_iter().enumerate().try_for_each(
        |(j, mut r)| -> Result<(), ExecutorError> {
            let tx_idx = start + j as u64;
            r.transaction_index = tx_idx;
            accum.cumulative = accum.cumulative.checked_add(r.gas_used).ok_or_else(|| {
                ExecutorError::State(format!("cumulative gas overflow at tx {tx_idx}"))
            })?;
            r.cumulative_gas_used = accum.cumulative;
            accum.receipts.push(r);
            Ok(())
        },
    )?;
    accum
        .frags
        .push(out.bal.expect("capture session returns a BAL"));
    accum.seg_layers.push(std::sync::Arc::new(out.delta));
    Ok(())
}

/// Execute a deposit or cross-chain-delivery singleton on the shared
/// deposit path, and fold its result into `accum`.
fn process_singleton_segment<S: SharedState>(
    req: &BlockRequest<S>,
    at: u64,
    rec: &BufferedRecord,
    accum: &mut SegAccum,
) -> Result<(), ExecutorError> {
    // The streaming deposit/cross-chain path, unchanged: execute outside
    // the scope against the snapshot layered with the parent and prior
    // segments, and capture at the block-global index.
    let merged =
        accum
            .seg_layers
            .iter()
            .fold(req.parent.clone().unwrap_or_default(), |mut merged, l| {
                merged.merge_from(l);
                merged
            });

    let mut scope = ExecCore::new(&req.snapshot, Some(&merged), req.env)?;
    let mut frag = revm::state::bal::Bal::new();
    let (receipt, ws) = execute_record_in_scope(
        &mut scope,
        rec,
        at,
        accum.cumulative,
        Some((&mut frag, at + 1)),
    )?;
    accum.cumulative = receipt.cumulative_gas_used;
    accum.receipts.push(receipt);
    if !frag.accounts.is_empty() {
        accum.frags.push(frag);
    }
    let mut d = PendingDelta::new();
    d.apply(ws);
    accum.seg_layers.push(std::sync::Arc::new(d));
    Ok(())
}
