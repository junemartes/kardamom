use super::StmSnapshot;
use super::config::{PoolConfig, nanos};
use super::handle::{PoolHandle, with_pool};
use super::metrics::StmOutcome;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::PendingDelta;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::TxIndex;
use kardamom_exec_core::executor::DecodedTx;
use kardamom_exec_core::executor::Executor;
use kardamom_footprint::classifier::Stats;
use kardamom_types::BPosition;
use kardamom_types::Receipt;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;

/// Batch entry point over a transient pool. Kept for tests and simple
/// callers; long-lived callers (the A/B harness, the live actor) hold a
/// [`with_pool`] scope and amortize the spawn away entirely.
///
/// # Errors
/// Returns an error if the block is declined and the sequential
/// fallback fails, if opening the session fails, or if admitting or
/// sealing a transaction fails.
pub fn execute_block_stm<S: StmSnapshot>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
    stats: &Stats,
    workers: std::num::NonZeroUsize,
) -> Result<StmOutcome, ExecutorError> {
    with_pool(
        PoolConfig {
            workers,
            ..Default::default()
        },
        |pool: &PoolHandle<'_, S>| {
            pool.run_block(
                vec![snapshot.clone(); workers.get()],
                base.cloned().unwrap_or_default(),
                env,
                txs,
                stats,
            )
        },
    )
}

/// One sequential-path transaction: the canonical fields, plus an
/// optional pre-decoded envelope. `decoded: None` takes the inline
/// decode path (the undecodable-envelope skip receipt included), so
/// one record shape covers both the plain and the pre-decoded callers.
#[derive(Clone, Copy)]
pub struct SeqTx<'a> {
    pub tx_idx: TxIndex,
    pub position: BPosition,
    pub envelope: &'a TxEnvelope,
    pub decoded: Option<&'a DecodedTx>,
}

/// [`execute_block_sequential`] with the RLP already decoded, per
/// record: the fair baseline for A/B against the parallel engine. The
/// parallel path receives pre-decoded envelopes from `prepare` (readers
/// do it, off the execution thread); charging the sequential path for
/// its own inline decode would inflate every ratio. Production gets the
/// same benefit: whoever reads the stream can hand the decode to either
/// engine.
///
/// # Errors
/// Returns an error if opening the `Executor` fails, or if any
/// transaction fails to execute.
#[allow(
    clippy::cast_precision_loss,
    reason = "env-gated debug timing print in milliseconds; exec_ns never nears 2^52"
)]
pub fn execute_block_sequential_decoded<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[SeqTx<'_>],
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    sequential_inner(snapshot, base, env, txs.iter().copied(), ", pre-decoded")
}

/// [`execute_block_sequential_decoded`] over an iterator instead of a
/// slice, for a caller (see [`PoolHandle::decline_prepared`]) that
/// already has its records as an iterator and would otherwise collect
/// a throwaway `Vec<SeqTx>` just to call the slice-shaped entry point.
pub(super) fn execute_block_sequential_decoded_iter<'a, S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: impl ExactSizeIterator<Item = SeqTx<'a>>,
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    sequential_inner(snapshot, base, env, txs, ", pre-decoded")
}

/// Shared body for [`execute_block_sequential`] and
/// [`execute_block_sequential_decoded`]. `label_suffix` keeps each
/// caller's own diagnostic line text (see each function's doc). `txs`
/// is an iterator, not a slice, so a caller with only a tuple slice
/// (see [`execute_block_sequential`]) can map its records into `SeqTx`
/// lazily instead of collecting a `Vec<SeqTx>` first.
#[allow(
    clippy::cast_precision_loss,
    reason = "env-gated debug timing print in milliseconds; exec_ns never nears 2^52"
)]
fn sequential_inner<'a, S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: impl ExactSizeIterator<Item = SeqTx<'a>>,
    label_suffix: &str,
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    let mut scope = Executor::new(snapshot, base, env)?;
    let n = txs.len();
    let mut receipts = Vec::with_capacity(n);
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    let timing = std::env::var_os("KARDAMOM_SEQ_TIMING").is_some();
    let mut exec_ns = 0u64;
    for (i, record) in txs.enumerate() {
        let t0 = timing.then(std::time::Instant::now);
        let (receipt, ws) = match record.decoded {
            Some(d) => scope.execute_tx_decoded(
                record.tx_idx,
                record.position,
                record.envelope,
                d,
                i as u64,
                cumulative,
                None,
                None,
            )?,
            // Undecodable: the inline path produces the skip receipt.
            None => scope.execute_tx(
                record.tx_idx,
                record.position,
                record.envelope,
                i as u64,
                cumulative,
                None,
                None,
            )?,
        };
        if let Some(t0) = t0 {
            exec_ns = exec_ns.saturating_add(nanos(t0.elapsed()));
        }
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    if timing && n > 0 {
        eprintln!(
            "seq block {}: execute_tx sum {:.1}ms ({n} txs{label_suffix})",
            env.block_number,
            exec_ns as f64 / 1e6,
        );
    }
    Ok((receipts, delta))
}

/// The sequential reference path (also the fallback): `Executor` per
/// block, in canonical order, the executor's streaming semantics.
///
/// # Errors
/// Returns an error if opening the `Executor` fails, or if any
/// transaction fails to execute.
pub fn execute_block_sequential<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    // Diagnostic split, env-gated: how much of the sequential wall time
    // is `execute_tx` itself versus the delta fold around it. The
    // engines' outer walls alone do not attribute overhead correctly.
    let records = txs.iter().map(|(tx_idx, position, envelope)| SeqTx {
        tx_idx: *tx_idx,
        position: *position,
        envelope,
        decoded: None,
    });
    sequential_inner(snapshot, base, env, records, "")
}
