//! The parallel block executor (spec §P2, offline milestone): workers pull
//! DAG-ready txs, execute each against a per-tx multi-version view, publish
//! versions, and a canonical-order commit pass materializes the exact
//! sequential artifacts — receipts (cumulative gas, accumulator-fixed
//! write-set hashes) and the block `PendingDelta`, byte-identical to
//! `ExecScope` output by construction and re-checked by validation.
//!
//! Wound-wait runtime detection (ESTIMATE marks, child self-abort) is
//! deliberately NOT here yet: under pessimistic scheduling a conflict is an
//! ordered edge, so the miss classes it accelerates are expected ~never
//! (P1 shadow: 0 across 18,400 graded txs). Validation + whole-block
//! sequential fallback (invariant #3) carries correctness alone in P2a;
//! the optimization lands with P2b once the A/B shows where it pays.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Condvar, Mutex};

use alloy_primitives::{B256, U256};
use bytes::Bytes;
use kardamom_exec_core::block_env::ExecEnv;
use kardamom_exec_core::delta::{PendingDelta, WriteSet};
use kardamom_exec_core::error::ExecutorError;
use kardamom_exec_core::exec_types::{ReceiptStatus, TxIndex};
use kardamom_exec_core::executor::{
    ExecScope, SnapshotRef, decode_alloy_envelope, invalid_skip, tx_env_from_alloy, wire_log,
    write_set_from_evm_state,
};
use kardamom_footprint::Cell;
use kardamom_footprint::classifier::Stats;
use kardamom_types::{BPosition, Receipt, StateDatabase, TxEnvelope};
use revm::context::result::ExecutionResult;
use revm::database::DatabaseRef;
use revm::state::AccountInfo;
use revm::{Context, ExecuteEvm, MainBuilder, MainContext};

use crate::FEE_SINK;
use crate::mv::{AccountVersion, MvCache, ReadRecord};
use crate::schedule;

/// Layered block-input view: the pre-block delta over the snapshot — what
/// sequential execution sees at the block's first tx. Read-only and shared
/// by every worker.
pub struct BlockInput<'a, S: StateDatabase> {
    pub snapshot: &'a S,
    pub base: Option<&'a PendingDelta>,
}

impl<'a, S: StateDatabase> DatabaseRef for BlockInput<'a, S> {
    type Error = kardamom_exec_core::executor::StateRefError;

    fn basic_ref(
        &self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        if let Some(base) = self.base
            && let Some((nonce, balance, code_hash)) = base.accounts.get(&address)
        {
            let code_hash = if *code_hash == B256::ZERO {
                revm::primitives::KECCAK_EMPTY
            } else {
                *code_hash
            };
            return Ok(Some(AccountInfo {
                nonce: *nonce,
                balance: *balance,
                code_hash,
                account_id: None,
                code: None,
            }));
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .basic_ref(address)
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        if let Some(base) = self.base
            && let Some(code) = base.code.get(&code_hash)
            && !code.is_empty()
        {
            return Ok(revm::state::Bytecode::new_raw(
                alloy_primitives::Bytes::copy_from_slice(code),
            ));
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .code_by_hash_ref(code_hash)
    }

    fn storage_ref(
        &self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        let key = B256::from(index.to_be_bytes::<32>());
        if let Some(base) = self.base
            && let Some(v) = base.storage.get(&(address, key))
        {
            return Ok(*v);
        }
        SnapshotRef {
            inner: self.snapshot,
        }
        .storage_ref(address, index)
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        SnapshotRef {
            inner: self.snapshot,
        }
        .block_hash_ref(number)
    }
}

/// Per-tx database view: multi-version cache at index `idx` over the block
/// input, recording every first read for validation. The fee sink is served
/// from the cached block-start info and never recorded — the `Accumulator`
/// boundary (its correctness is the commit pass's prefix algebra, not
/// version validation).
struct MvView<'a, S: StateDatabase> {
    mv: &'a MvCache,
    base: &'a BlockInput<'a, S>,
    idx: u32,
    reads: Vec<ReadRecord>,
    sink_start: Option<AccountInfo>,
    // Worker-local memos over the IMMUTABLE-for-the-block layers (base
    // input; content-addressed code). Without them every MvCache miss
    // re-walks the delta BTreeMaps and the snapshot — and worse, re-copies
    // full contract bytecode per call. The multi-version lists themselves
    // are never memoized (their answers depend on the reader's index).
    base_accounts: std::collections::HashMap<alloy_primitives::Address, Option<AccountInfo>>,
    base_storage: std::collections::HashMap<(alloy_primitives::Address, B256), U256>,
    code_cache: std::collections::HashMap<B256, revm::state::Bytecode>,
}

impl<'a, S: StateDatabase> MvView<'a, S> {
    fn new(mv: &'a MvCache, base: &'a BlockInput<'a, S>, sink_start: Option<AccountInfo>) -> Self {
        Self {
            mv,
            base,
            idx: 0,
            reads: Vec::new(),
            sink_start,
            base_accounts: std::collections::HashMap::new(),
            base_storage: std::collections::HashMap::new(),
            code_cache: std::collections::HashMap::new(),
        }
    }
}

impl<'a, S: StateDatabase> revm::Database for MvView<'a, S> {
    type Error = kardamom_exec_core::executor::StateRefError;

    fn basic(
        &mut self,
        address: alloy_primitives::Address,
    ) -> Result<Option<AccountInfo>, Self::Error> {
        if address == FEE_SINK {
            return Ok(self.sink_start.clone());
        }
        if let Some((ver, a)) = self.mv.read_account(self.idx, &address) {
            self.reads.push(ReadRecord::Account(address, Some(ver)));
            return Ok(Some(AccountInfo {
                nonce: a.nonce,
                balance: a.balance,
                code_hash: if a.code_hash == B256::ZERO {
                    revm::primitives::KECCAK_EMPTY
                } else {
                    a.code_hash
                },
                account_id: None,
                code: None,
            }));
        }
        self.reads.push(ReadRecord::Account(address, None));
        if let Some(a) = self.base_accounts.get(&address) {
            return Ok(a.clone());
        }
        let a = self.base.basic_ref(address)?;
        self.base_accounts.insert(address, a.clone());
        Ok(a)
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<revm::state::Bytecode, Self::Error> {
        // Content-addressed: no version, no record — memo both sources
        // (Bytecode clones are refcounted; the copy happens once).
        if let Some(c) = self.code_cache.get(&code_hash) {
            return Ok(c.clone());
        }
        let c = if let Some(code) = self.mv.read_code(&code_hash) {
            revm::state::Bytecode::new_raw(alloy_primitives::Bytes::copy_from_slice(&code))
        } else {
            self.base.code_by_hash_ref(code_hash)?
        };
        self.code_cache.insert(code_hash, c.clone());
        Ok(c)
    }

    fn storage(
        &mut self,
        address: alloy_primitives::Address,
        index: U256,
    ) -> Result<U256, Self::Error> {
        let key = B256::from(index.to_be_bytes::<32>());
        if let Some((ver, v)) = self.mv.read_slot(self.idx, &address, &key) {
            self.reads.push(ReadRecord::Slot(address, key, Some(ver)));
            return Ok(v);
        }
        self.reads.push(ReadRecord::Slot(address, key, None));
        if let Some(v) = self.base_storage.get(&(address, key)) {
            return Ok(*v);
        }
        let v = self.base.storage_ref(address, index)?;
        self.base_storage.insert((address, key), v);
        Ok(v)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.base.block_hash_ref(number)
    }
}

/// One executed tx's artifacts, pre-commit (cumulative gas and the
/// accumulator wsh fixup land in the canonical-order commit pass).
struct TxResult {
    receipt: Receipt,
    ws: WriteSet,
    reads: Vec<ReadRecord>,
    /// This tx's exact credit to the fee sink (post − block-start seen).
    fee_delta: U256,
}

/// Outcome of one block through the STM engine.
pub struct StmOutcome {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
    /// Validation failed and the block re-executed sequentially
    /// (invariant #3). Outputs are the sequential ones.
    pub fallback: bool,
    /// Read records convicted by validation (0 unless `fallback`).
    pub validation_failures: usize,
    /// Schedule diagnostics.
    pub cold: usize,
    pub edges: usize,
}

/// Execute one block. `txs` are the block's canonical records in order,
/// with their global indices/positions (receipts carry them verbatim).
/// `stats` are the footprint stats trained on PRIOR blocks only.
#[allow(clippy::too_many_arguments)] // mirrors the sequential entry points'
// shape; see execute_tx's rationale.
pub fn execute_block_stm<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
    stats: &Stats,
    workers: usize,
) -> Result<StmOutcome, ExecutorError> {
    let n = txs.len();
    let mut exclude = HashSet::new();
    exclude.insert(Cell::Account(FEE_SINK));
    let envelopes: Vec<TxEnvelope> = txs.iter().map(|(_, _, e)| e.clone()).collect();
    // ONE decode per tx, shared by schedule and workers (the schedule's
    // envelope_view used to decode a second time — measured at ~1ms per
    // 1000-tx block, pure waste).
    let decoded: Vec<Option<alloy_consensus::TxEnvelope>> = txs
        .iter()
        .map(|(tx_idx, _, e)| decode_alloy_envelope(&e.raw_tx, *tx_idx).ok())
        .collect();
    let sched = schedule::build(stats, &envelopes, &decoded, &exclude);
    let input = BlockInput { snapshot, base };
    let sink_start_info = input
        .basic_ref(FEE_SINK)
        .map_err(|e| ExecutorError::State(format!("fee-sink read: {e}")))?;
    let sink_start_balance = sink_start_info
        .as_ref()
        .map(|a| a.balance)
        .unwrap_or(U256::ZERO);

    let mv = MvCache::new();
    let results: Vec<std::sync::OnceLock<Result<TxResult, ExecutorError>>> =
        (0..n).map(|_| std::sync::OnceLock::new()).collect();
    let indegree: Vec<AtomicU32> = sched.indegree.iter().map(|d| AtomicU32::new(*d)).collect();
    // Canonical-order-first ready policy: the lowest ready index runs
    // first — chains drain in order and read-then-published windows (the
    // only source of validation convictions) stay minimal.
    let ready: Mutex<BinaryHeap<Reverse<u32>>> = Mutex::new(
        (0..n as u32)
            .filter(|i| sched.indegree[*i as usize] == 0)
            .map(Reverse)
            .collect(),
    );
    let ready_cv = Condvar::new();
    let remaining = AtomicU32::new(n as u32);
    let abort = AtomicBool::new(false);

    let worker_count = workers.max(1).min(n.max(1));
    let t_sched_done = std::time::Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..worker_count {
            scope.spawn(|| {
                // ONE EVM per worker for the whole block (the per-tx
                // construction was ~90% of execution-path allocation —
                // ExecScope's lesson applies here verbatim): the view's
                // index/read-log are re-aimed per tx through `db_mut`.
                // Nothing carries across `transact` calls except the DB
                // itself — the same property ExecScope already relies on.
                let view = MvView::new(&mv, &input, sink_start_info.clone());
                let mut evm = Context::mainnet()
                    .with_db(view)
                    .with_block(env.block_env())
                    .with_cfg(env.cfg_env())
                    .build_mainnet();
                loop {
                    let job = {
                        let mut q = ready.lock().expect("ready poisoned");
                        loop {
                            if abort.load(Ordering::SeqCst) || remaining.load(Ordering::SeqCst) == 0
                            {
                                return;
                            }
                            if let Some(Reverse(i)) = q.pop() {
                                break i;
                            }
                            q = ready_cv.wait(q).expect("ready poisoned");
                        }
                    };
                    let (tx_idx, position, envelope) = &txs[job as usize];
                    let r = execute_one(
                        &mut evm,
                        &mv,
                        env,
                        job,
                        *tx_idx,
                        *position,
                        envelope,
                        decoded[job as usize].as_ref(),
                        sink_start_balance,
                    );
                    let errored = r.is_err();
                    let _ = results[job as usize].set(r);
                    if errored {
                        abort.store(true, Ordering::SeqCst);
                        ready_cv.notify_all();
                        return;
                    }
                    let mut newly = Vec::new();
                    for &c in &sched.children[job as usize] {
                        if indegree[c as usize].fetch_sub(1, Ordering::SeqCst) == 1 {
                            newly.push(c);
                        }
                    }
                    let left = remaining.fetch_sub(1, Ordering::SeqCst) - 1;
                    if !newly.is_empty() || left == 0 {
                        let mut q = ready.lock().expect("ready poisoned");
                        q.extend(newly.into_iter().map(Reverse));
                        drop(q);
                        ready_cv.notify_all();
                    }
                }
            });
        }
    });

    let t_exec_wall = t_sched_done.elapsed();
    let t_exec = std::time::Instant::now();
    // Collect: surface the first (local, fail-stop) execution error.
    let mut tx_results = Vec::with_capacity(n);
    for cell in results {
        match cell.into_inner() {
            Some(Ok(r)) => tx_results.push(r),
            Some(Err(e)) => return Err(e),
            None => {
                return Err(ExecutorError::State(
                    "stm: worker pool exited with unexecuted txs (scheduler bug)".into(),
                ));
            }
        }
    }

    // Validation (spec: "Validation remains as the final invariant check"):
    // every recorded read must still be the highest version below the
    // reader. A conviction = the prediction missed a real conflict.
    let t_collect = t_exec.elapsed();
    let t_val = std::time::Instant::now();
    let validation_failures: usize = tx_results
        .iter()
        .enumerate()
        .map(|(i, r)| {
            r.reads
                .iter()
                .filter(|rec| !mv.validate(i as u32, rec))
                .count()
        })
        .sum();
    let t_validate = t_val.elapsed();
    if validation_failures > 0 {
        // Invariant #3: discard, re-execute sequentially, count it.
        tracing::warn!(
            block = env.block_number,
            validation_failures,
            "stm: validation conviction — sequential fallback"
        );
        let (receipts, delta) = execute_block_sequential(snapshot, base, env, txs)?;
        return Ok(StmOutcome {
            receipts,
            delta,
            fallback: true,
            validation_failures,
            cold: sched.cold,
            edges: sched.edges,
        });
    }

    // Canonical-order commit: cumulative gas, accumulator materialization +
    // wsh fixup, delta fold. Receipts/delta byte-identical to sequential.
    let mut receipts = Vec::with_capacity(n);
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    let mut sink_running = sink_start_balance;
    for (i, mut r) in tx_results.into_iter().enumerate() {
        let _ = i;
        cumulative += r.receipt.gas_used;
        r.receipt.cumulative_gas_used = cumulative;
        sink_running += r.fee_delta;
        if let Some(entry) = r.ws.accounts.iter_mut().find(|(a, _)| *a == FEE_SINK) {
            entry.1.1 = sink_running;
            r.receipt.write_set_hash = r.ws.hash();
        }
        delta.apply(r.ws);
        receipts.push(r.receipt);
    }

    if std::env::var("KARDAMOM_STM_PHASE_TIMING").is_ok() {
        eprintln!(
            "phase block={} n={} exec={:?} collect={:?} validate={:?}",
            env.block_number, n, t_exec_wall, t_collect, t_validate
        );
    }
    Ok(StmOutcome {
        receipts,
        delta,
        fallback: false,
        validation_failures: 0,
        cold: sched.cold,
        edges: sched.edges,
    })
}

/// The sequential reference path (also the fallback): `ExecScope` per
/// block, canonical order — the executor's streaming semantics.
pub fn execute_block_sequential<S: StateDatabase + Sync>(
    snapshot: &S,
    base: Option<&PendingDelta>,
    env: ExecEnv,
    txs: &[(TxIndex, BPosition, TxEnvelope)],
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    let mut scope = ExecScope::new(snapshot, base, env)?;
    let mut receipts = Vec::with_capacity(txs.len());
    let mut delta = PendingDelta::new();
    let mut cumulative = 0u64;
    for (i, (tx_idx, position, envelope)) in txs.iter().enumerate() {
        let (receipt, ws) = scope.execute_tx(
            *tx_idx, *position, envelope, i as u64, cumulative, None, None,
        )?;
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    Ok((receipts, delta))
}

/// One worker's EVM over its multi-version view — ExecScope's shape with
/// the concurrent DB swapped in.
type WorkerEvm<'a, S> = revm::handler::MainnetEvm<
    revm::context::Context<
        revm::context::BlockEnv,
        revm::context::TxEnv,
        revm::context::CfgEnv,
        MvView<'a, S>,
    >,
>;

/// Execute one tx against its multi-version view. Mirrors
/// `ExecScope::execute_tx` exactly — #92 skip semantics, write-set
/// emission, receipt shape — with MvCache publish in place of the
/// sequential commit. The worker's EVM is reused across txs; only the
/// view's index and read log are re-aimed.
#[allow(clippy::too_many_arguments)]
fn execute_one<S: StateDatabase>(
    evm: &mut WorkerEvm<'_, S>,
    mv: &MvCache,
    env: ExecEnv,
    local_idx: u32,
    tx_idx: TxIndex,
    position: BPosition,
    envelope: &TxEnvelope,
    decoded: Option<&alloy_consensus::TxEnvelope>,
    sink_start_balance: U256,
) -> Result<TxResult, ExecutorError> {
    let skip = |reason: &str, nonce: u64, to: Option<alloy_primitives::Address>| {
        let (receipt, ws) = invalid_skip(
            reason,
            position,
            envelope,
            nonce,
            to,
            env.block_number,
            local_idx as u64,
            0,
        );
        TxResult {
            receipt,
            ws,
            reads: Vec::new(),
            fee_delta: U256::ZERO,
        }
    };

    let _ = tx_idx;
    let Some(alloy_env) = decoded else {
        return Ok(skip("undecodable raw_tx", 0, None));
    };
    use alloy_consensus::Transaction;
    let signer = envelope.sender;
    let nonce = alloy_env.nonce();
    let to = alloy_env.to();
    let effective_gas_price = alloy_env
        .gas_price()
        .unwrap_or_else(|| alloy_env.max_fee_per_gas());

    {
        // Re-aim the worker's view at this tx.
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        db.idx = local_idx;
        db.reads.clear();
    }
    let tx_env = tx_env_from_alloy(alloy_env, signer);
    let outcome = match evm.transact(tx_env) {
        Ok(o) => o,
        Err(revm::context::result::EVMError::Transaction(reason)) => {
            return Ok(skip(&format!("{reason:?}"), nonce, to));
        }
        Err(revm::context::result::EVMError::Header(reason)) => {
            return Ok(skip(&format!("{reason:?}"), nonce, to));
        }
        Err(e) => {
            return Err(ExecutorError::Execution {
                idx: tx_idx,
                detail: format!("{e:?}"),
            });
        }
    };

    let gas_used = outcome.result.gas().tx_gas_used();
    let (status, logs) = match &outcome.result {
        ExecutionResult::Success { logs, .. } => (ReceiptStatus::Success, logs.clone()),
        ExecutionResult::Revert { .. } => (ReceiptStatus::Revert, Vec::new()),
        ExecutionResult::Halt { reason, .. } => (ReceiptStatus::Halt(reason.clone()), Vec::new()),
    };

    let ws = write_set_from_evm_state(&outcome.state);
    // Publish: every written cell EXCEPT the fee sink (Accumulator — all
    // workers see block-start; the commit pass materializes prefixes).
    for (addr, (n_, b_, ch_)) in ws.accounts.iter() {
        if *addr == FEE_SINK {
            continue;
        }
        mv.publish_account(
            local_idx,
            *addr,
            AccountVersion {
                nonce: *n_,
                balance: *b_,
                code_hash: *ch_,
            },
        );
    }
    for ((addr, key), value) in ws.storage.iter() {
        mv.publish_slot(local_idx, *addr, *key, *value);
    }
    for (hash, code) in ws.code.iter() {
        mv.publish_code(*hash, Bytes::clone(code));
    }
    let fee_delta = ws
        .accounts
        .iter()
        .find(|(a, _)| *a == FEE_SINK)
        .map(|(_, (_, b, _))| *b - sink_start_balance)
        .unwrap_or(U256::ZERO);

    let reads = {
        // Take this tx's read log back out of the worker's view.
        let db = revm::context_interface::ContextTr::db_mut(&mut **evm);
        std::mem::take(&mut db.reads)
    };

    let write_set_hash = ws.hash();
    let contract_address = if to.is_none() && status.is_success() {
        Some(signer.create(nonce))
    } else {
        None
    };
    let receipt = Receipt {
        tx_idx: position,
        tx_hash: envelope.tx_hash,
        tx_type: kardamom_types::tx_type_of(&envelope.raw_tx),
        status: status.is_success(),
        gas_used,
        logs: logs.iter().map(wire_log).collect(),
        write_set_hash,
        nonce,
        from: signer,
        to,
        contract_address,
        effective_gas_price,
        block_number: env.block_number,
        transaction_index: local_idx as u64,
        // Canonical prefix sums land in the commit pass.
        cumulative_gas_used: 0,
    };
    Ok(TxResult {
        receipt,
        ws,
        reads,
        fee_delta,
    })
}
