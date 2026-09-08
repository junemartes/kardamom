//! One worker's execute path: run one transaction against the shared
//! multi-version view and build its result. `worker.rs` owns the job
//! loop that dispatches into [`execute_one`].

use super::config::bal_index;
use super::config::nanos;
use super::metrics::{Metrics, TxResult};
use super::recycle::RecyclePools;
use super::view::MvView;
use crate::FEE_SINK;
use crate::mv::MvCache;
use crate::mv::ReadRecord;
use alloy_primitives::B256;
use alloy_primitives::U256;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::WriteSet;
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::ReceiptStatus;
use kardamom_exec_core::exec_types::{TxIndex, TxSlot};
use kardamom_exec_core::executor::DecodedTx;
use kardamom_exec_core::executor::Executor;
use kardamom_types::BPosition;
use kardamom_types::Receipt;
use kardamom_types::StateDatabase;
use kardamom_types::TxEnvelope;
use revm::ExecuteEvm;
use revm::context::result::ExecutionResult;
use std::sync::atomic::Ordering;

/// One worker's EVM over its multi-version view: Executor's shape with
/// the concurrent DB swapped in.
pub(super) type WorkerEvm<'a, S> = revm::handler::MainnetEvm<
    revm::context::Context<
        revm::context::BlockEnv,
        revm::context::TxEnv,
        revm::context::CfgEnv,
        MvView<'a, S>,
    >,
>;

/// Pop a cleared read-record buffer (batch-refilled from the recycle
/// pool, one lock per 64 transactions); falls back to a fresh
/// allocation while the pool warms up.
pub(super) fn take_read_buf(
    stash: &mut Vec<Vec<ReadRecord>>,
    pools: &RecyclePools,
) -> Vec<ReadRecord> {
    if let Some(b) = stash.pop() {
        return b;
    }
    {
        let mut g = pools.read_bufs.lock().expect("pools poisoned");
        let n = g.len().min(64);
        let at = g.len() - n;
        stash.extend(g.drain(at..));
    }
    stash.pop().unwrap_or_else(|| Vec::with_capacity(128))
}

/// One transaction, addressed for `execute_one`: its position in the
/// block and the decoded form the reader threads already produced.
#[derive(Clone, Copy)]
pub(super) struct TxJob<'a> {
    pub(super) local_idx: u32,
    pub(super) tx_idx: TxIndex,
    pub(super) position: BPosition,
    pub(super) envelope: &'a TxEnvelope,
    pub(super) decoded: Option<&'a DecodedTx>,
}

/// What `execute_one` reads from and publishes into: the block's shared
/// multi-version view, the worker's metrics, and the block-start fee
/// sink state it must not re-derive per transaction.
#[derive(Clone, Copy)]
pub(super) struct ExecCtx<'a> {
    pub(super) mv: &'a MvCache,
    pub(super) metrics: &'a Metrics,
    pub(super) env: ExecEnv,
    pub(super) sink_start_balance: U256,
    pub(super) bal_base: Option<u64>,
}

impl TxJob<'_> {
    /// Build the skip-path result: a receipt and empty write set, no
    /// reads recorded, no fee credited. Used for every reason a
    /// transaction never really ran (undecodable, or an `EVMError` the
    /// executor treats as a skip rather than a hard failure). `self`'s
    /// own `position`, `envelope`, and `local_idx` fill in three of the
    /// skip receipt's fields, so the caller supplies only what is
    /// specific to the failure (the reason, the detail text, the
    /// resolved nonce and recipient, and the block number).
    fn skip<S: StateDatabase>(
        &self,
        reason: kardamom_types::SkipReason,
        detail: &str,
        nonce: u64,
        to: Option<alloy_primitives::Address>,
        block_number: u64,
    ) -> TxResult {
        let slot = TxSlot {
            tx_idx: self.tx_idx,
            tx_position: self.position,
            tx_index_in_block: u64::from(self.local_idx),
            cumulative_gas_used_before: 0,
        };
        let (receipt, ws) = Executor::<S>::skip_receipt(
            reason,
            detail,
            slot,
            self.envelope,
            nonce,
            to,
            block_number,
        );
        TxResult {
            receipt,
            ws,
            reads: Vec::new(),
            fee_delta: U256::ZERO,
            sink_touched: false,
            bal_frag: None,
        }
    }
}

/// Execute one transaction against its multi-version view. Mirrors
/// `Executor::execute_tx` exactly (skip semantics, write-set emission,
/// receipt shape), with `MvCache` publish in place of the sequential
/// commit. The worker's EVM is reused across transactions; only the
/// view's index and read log are re-aimed.
pub(super) fn execute_one<S: StateDatabase, F: FnMut() -> Vec<ReadRecord>>(
    evm: &mut WorkerEvm<'_, S>,
    job: TxJob<'_>,
    ctx: ExecCtx<'_>,
    fresh_reads: &mut F,
) -> Result<TxResult, ExecutorError> {
    use alloy_consensus::Transaction;
    let Some(alloy_env) = job.decoded else {
        return Ok(job.skip::<S>(
            kardamom_types::SkipReason::Undecodable,
            "undecodable raw_tx",
            0,
            None,
            ctx.env.block_number,
        ));
    };
    let (signer, nonce, to) = (job.envelope.sender, alloy_env.nonce(), alloy_env.to());
    let effective_gas_price = alloy_env
        .gas_price()
        .unwrap_or_else(|| alloy_env.max_fee_per_gas());
    let tx_env = reaim_and_build_tx_env(evm, job.local_idx, alloy_env, signer);
    let t_evm = std::time::Instant::now();
    let outcome = match evm.transact(tx_env) {
        Ok(o) => o,
        Err(revm::context::result::EVMError::Transaction(e)) => {
            return Ok(job.skip::<S>(
                kardamom_exec_core::executor::skip_reason_of_tx(&e),
                &format!("{e:?}"),
                nonce,
                to,
                ctx.env.block_number,
            ));
        }
        Err(revm::context::result::EVMError::Header(e)) => {
            return Ok(job.skip::<S>(
                kardamom_types::SkipReason::Header,
                &format!("{e:?}"),
                nonce,
                to,
                ctx.env.block_number,
            ));
        }
        Err(e) => {
            return Err(ExecutorError::Execution {
                idx: job.tx_idx,
                detail: format!("{e:?}"),
            });
        }
    };
    build_tx_result(
        evm,
        job,
        ctx,
        Ran {
            outcome,
            evm_ns: nanos(t_evm.elapsed()),
            signer,
            nonce,
            to,
            effective_gas_price,
        },
        fresh_reads,
    )
}

/// A transaction that ran (did not skip): its EVM outcome, plus the
/// fields [`execute_one`] already resolved (the signer, the decoded
/// nonce and recipient, the effective gas price) so [`build_tx_result`]
/// does not re-derive them from the decoded envelope.
struct Ran {
    outcome: revm::context::result::ResultAndState,
    evm_ns: u64,
    signer: alloy_primitives::Address,
    nonce: u64,
    to: Option<alloy_primitives::Address>,
    effective_gas_price: u128,
}

/// Turn one successful EVM outcome into a [`TxResult`]: publish the
/// write set, capture its BAL fragment, compute the fee-sink credit,
/// take the read log back out of the view, and build the receipt. The
/// `match` in [`execute_one`] stays free of this concern.
fn build_tx_result<S: StateDatabase>(
    evm: &mut WorkerEvm<'_, S>,
    job: TxJob<'_>,
    ctx: ExecCtx<'_>,
    ran: Ran,
    fresh_reads: &mut impl FnMut() -> Vec<ReadRecord>,
) -> Result<TxResult, ExecutorError> {
    let ExecCtx {
        mv,
        metrics,
        env,
        sink_start_balance,
        bal_base,
    } = ctx;
    let Ran {
        mut outcome,
        evm_ns,
        signer,
        nonce,
        to,
        effective_gas_price,
    } = ran;
    metrics.evm_ns.fetch_add(evm_ns, Ordering::Relaxed);
    let gas_used = outcome.result.gas().tx_gas_used();
    // Build wire logs straight from the borrowed result: no
    // intermediate `logs.clone()` (topic Vecs and data Bytes per log).
    let (status, wire_logs) = status_and_wire_logs(&outcome.result);

    let ws = WriteSet::from_evm_state(&outcome.state);
    // EIP-7928 capture: this transaction's fragment at its block-global
    // index, through the same update_account the streaming path uses on
    // the same `outcome.state`. Everything the mv cache tracks reads
    // canonically here (a wound would replace this fragment). The fee
    // sink is the one untracked account: the commit pass rewrites its
    // balance write to the computed prefix, exactly as it rewrites the
    // WriteSet's.
    let bal_frag = capture_bal(bal_base, job.local_idx, env.block_number, &outcome.state)?;
    // Pooled journal: hand the spent state map back so the next
    // transaction does not regrow it (see `recycle_journal`).
    recycle_journal(evm, &mut outcome.state);
    // Publish in the ordered helper's sequence (code and storage, then
    // accounts; see `MvCache::publish_write_set`), skipping the fee
    // sink (Accumulator: all workers see block-start; the commit pass
    // computes the prefixes).
    let t_pub = std::time::Instant::now();
    mv.publish_write_set(job.local_idx, &ws, FEE_SINK);
    let pub_ns = nanos(t_pub.elapsed());
    metrics.publish_ns.fetch_add(pub_ns, Ordering::Relaxed);
    let sink_fee_delta = sink_fee_delta(&ws, sink_start_balance, env.block_number, job.tx_idx)?;
    let sink_touched = sink_fee_delta.is_some();
    let fee_delta = sink_fee_delta.unwrap_or(U256::ZERO);
    let reads = {
        // Take this transaction's read log back out of the worker's
        // view. The replacement comes from the recycle pool (cleared,
        // with capacity intact from a previous block's transaction).
        // The fresh per-transaction Vec and its growth reallocations
        // were the largest STM-specific allocation.
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        std::mem::replace(&mut db.reads, fresh_reads())
    };
    // Hashing now is pure waste when the commit pass must re-hash after
    // patching the accumulator's absolute balance.
    let write_set_hash = if sink_touched { B256::ZERO } else { ws.hash() };
    let receipt = build_receipt(ReceiptArgs {
        position: job.position,
        envelope: job.envelope,
        status,
        gas_used,
        wire_logs,
        write_set_hash,
        nonce,
        signer,
        to,
        effective_gas_price,
        block_number: env.block_number,
        local_idx: job.local_idx,
    });
    Ok(TxResult {
        receipt,
        ws,
        reads,
        bal_frag,
        fee_delta,
        sink_touched,
    })
}

/// One scan answers both sink questions: whether this transaction
/// touched the fee sink at all, and if so, its credit this transaction
/// (see [`fee_delta_from_sink`]). `None` means the sink was not
/// touched; the caller derives `sink_touched` from that instead of
/// carrying a second, separately-fallible bool.
fn sink_fee_delta(
    ws: &WriteSet,
    sink_start_balance: U256,
    block_number: u64,
    tx_idx: TxIndex,
) -> Result<Option<U256>, ExecutorError> {
    let Some((_, fields)) = ws.accounts.iter().find(|(a, _)| *a == FEE_SINK) else {
        return Ok(None);
    };
    Ok(Some(fee_delta_from_sink(
        fields.balance,
        sink_start_balance,
        block_number,
        tx_idx,
    )?))
}

/// A worker's fee-sink credit this transaction: the observed post-tx
/// sink balance minus the block-start value every worker was told (see
/// `ExecCtx::sink_start_balance`). `U256::sub` is `wrapping_sub` in
/// every profile, so a plain subtraction here would turn an observed
/// balance below the block-start value into a huge wrapped delta
/// written straight into a receipt, with no panic in any build. A
/// worker can only see a balance below the block-start value if its
/// view of the sink is corrupted (the fee sink only ever gains value
/// within a block), so this is a hard error, not a clamp.
///
/// # Errors
/// Returns an error naming the block, the transaction, the block-start
/// balance, and the observed balance, if `observed < sink_start_balance`.
fn fee_delta_from_sink(
    observed: U256,
    sink_start_balance: U256,
    block_number: u64,
    tx_idx: TxIndex,
) -> Result<U256, ExecutorError> {
    observed.checked_sub(sink_start_balance).ok_or_else(|| {
        ExecutorError::State(format!(
            "stm: worker read the fee sink below its block-start balance in block \
             {block_number}, tx {tx_idx:?}: block-start={sink_start_balance}, \
             observed={observed} — corrupted view, block must not commit"
        ))
    })
}

/// Re-aim the worker's view at this transaction (the view's index and
/// its read log belong to whichever transaction the worker is running
/// now), then build the transaction's `TxEnv`.
fn reaim_and_build_tx_env<S: StateDatabase>(
    evm: &mut WorkerEvm<'_, S>,
    local_idx: u32,
    alloy_env: &DecodedTx,
    signer: alloy_primitives::Address,
) -> revm::context::TxEnv {
    let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
    db.idx = local_idx;
    db.reads.clear();
    alloy_env.tx_env(signer)
}

/// Classify one EVM outcome into a wire status and wire logs, built
/// straight from the borrowed result: no intermediate `logs.clone()`
/// (topic Vecs and data Bytes per log).
fn status_and_wire_logs(result: &ExecutionResult) -> (ReceiptStatus, Vec<kardamom_types::WireLog>) {
    match result {
        ExecutionResult::Success { logs, .. } => (
            ReceiptStatus::Success,
            logs.iter().map(kardamom_types::WireLog::from).collect(),
        ),
        ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
        ExecutionResult::Halt { reason, .. } => (ReceiptStatus::Halt(reason.clone()), Vec::new()),
    }
}

/// EIP-7928 capture: this transaction's fragment at its block-global
/// index, through the same `update_account` the streaming path uses on
/// the same post-transaction state. Everything the mv cache tracks
/// reads canonically here (a wound would replace this fragment). The
/// fee sink is the one untracked account: the commit pass rewrites its
/// balance write to the computed prefix, exactly as it rewrites the
/// `WriteSet`'s.
/// # Errors
/// Returns an error if `bal_base + local_idx + 1` overflows `u64`
/// (`bal_base` is a caller-supplied count, not bounded by this crate).
fn capture_bal(
    bal_base: Option<u64>,
    local_idx: u32,
    block_number: u64,
    state: &revm::state::EvmState,
) -> Result<Option<revm::state::bal::Bal>, ExecutorError> {
    let Some(b) = bal_base else {
        return Ok(None);
    };
    let idx = bal_index(b, u64::from(local_idx), block_number)?;
    let mut frag = revm::state::bal::Bal::new();
    for (addr, account) in state {
        frag.update_account(idx, *addr, account);
    }
    Ok(Some(frag))
}

/// Pooled journal: revm's finalize `mem::take`s the state map out of the
/// journal into the outcome, leaving a zero-capacity map behind. Every
/// transaction then regrew a fresh table, which measurement showed as a
/// large share of the per-transaction allocation floor on light
/// workloads. Hand the spent table back: its entries drop here (they
/// are transaction-local by contract, so a stale entry would be read as
/// a cached truth by the next transaction's `load_account`), its
/// capacity survives, and the journal's own entry vec and transient
/// storage already clear in place.
fn recycle_journal<S: StateDatabase>(
    evm: &mut WorkerEvm<'_, S>,
    state: &mut revm::state::EvmState,
) {
    let mut spent = std::mem::take(state);
    spent.clear();
    revm::context_interface::ContextTr::journal_mut(&mut **evm)
        .inner
        .state = spent;
}

/// Everything [`build_receipt`] needs, grouped so the call site reads as
/// one record instead of a 12-argument list.
struct ReceiptArgs<'a> {
    position: BPosition,
    envelope: &'a TxEnvelope,
    status: ReceiptStatus,
    gas_used: u64,
    wire_logs: Vec<kardamom_types::WireLog>,
    write_set_hash: B256,
    nonce: u64,
    signer: alloy_primitives::Address,
    to: Option<alloy_primitives::Address>,
    effective_gas_price: u128,
    block_number: u64,
    local_idx: u32,
}

/// Build one transaction's receipt. `contract_address` is derived here
/// (CREATE at this sender/nonce, only on a successful contract
/// creation); every other field is a direct copy of its argument.
fn build_receipt(args: ReceiptArgs<'_>) -> Receipt {
    let ReceiptArgs {
        position,
        envelope,
        status,
        gas_used,
        wire_logs,
        write_set_hash,
        nonce,
        signer,
        to,
        effective_gas_price,
        block_number,
        local_idx,
    } = args;
    let contract_address = if to.is_none() && status.is_success() {
        Some(signer.create(nonce))
    } else {
        None
    };
    Receipt {
        tx_idx: position,
        tx_hash: envelope.tx_hash,
        tx_type: kardamom_types::tx_type_of(&envelope.raw_tx),
        status: status.is_success(),
        gas_used,
        logs: wire_logs,
        write_set_hash,
        nonce,
        from: signer,
        to,
        contract_address,
        effective_gas_price,
        block_number,
        transaction_index: u64::from(local_idx),
        // Canonical prefix sums land in the commit pass.
        cumulative_gas_used: 0,
        skip_reason: None,
    }
}

#[cfg(test)]
mod fee_delta_tests;
