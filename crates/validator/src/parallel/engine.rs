//! Seeded parallel execution engine: batch seeding/execution/verification,
//! the sequential fallback, and the whole-block strategy handed to the
//! exec loop.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use kardamom_engine::actor::{BlockExec, BlockExecOutput, BufferedRecord};
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::error::ExecutorError;
use kardamom_stm::pool::WorkerPool;
use kardamom_types::{Receipt, StateDatabase};

use super::claims::{ClaimIndex, batch_ranges};
use super::dump::dump_divergence_inputs;

/// A batch's result: its receipts, with local cumulative gas (the caller
/// fixes up block-cumulative gas in order), and its merged writes.
pub struct BatchOutcome {
    pub first_index: u64,
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
}

/// Build the input layer a batch starting at `before` must observe:
/// snapshot state overlaid with the latest claim strictly before the
/// batch, that is, the previous batch's end state. EIP-7928 claims
/// account fields independently, so each triple is built from whichever
/// components have earlier claims, falling back to the snapshot.
pub fn build_seed<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    claims: &ClaimIndex,
    before: u64,
) -> Result<PendingDelta, ExecutorError> {
    // The base is the parent layer: the merged, not-yet-durable writes of
    // earlier blocks. The snapshot alone can be K blocks stale under the
    // depth-K commit pipeline. Claim seeds overlay on top, since
    // intra-block claims are newer than any parent state.
    let mut seed = parent.cloned().unwrap_or_default();

    let mut addrs: Vec<Address> = claims.balance.keys().copied().collect();
    addrs.extend(claims.nonce.keys().copied());
    addrs.extend(claims.code.keys().copied());
    addrs.sort_unstable();
    addrs.dedup();
    for addr in addrs {
        let claimed_bal = claims.balance_seed(addr, before);
        let claimed_nonce = claims.nonce_seed(addr, before);
        let claimed_code = claims.code_seed(addr, before);
        if claimed_bal.is_none() && claimed_nonce.is_none() && claimed_code.is_none() {
            continue; // Nothing is claimed before this batch, so the snapshot stands.
        }
        let base = match seed.accounts.get(&addr) {
            Some(v) => *v, // The parent layer already has the freshest base.
            None => snapshot
                .basic(addr)
                .map_err(|e| ExecutorError::State(format!("seed basic({addr:?}): {e}")))?
                .unwrap_or((0, U256::ZERO, alloy_primitives::KECCAK256_EMPTY)),
        };
        let code_hash = match claimed_code {
            Some(code) => {
                let h = alloy_primitives::keccak256(code);
                seed.code.insert(h, code.clone());
                h
            }
            None => base.2,
        };
        seed.accounts.insert(
            addr,
            (
                claimed_nonce.unwrap_or(base.0),
                claimed_bal.unwrap_or(base.1),
                code_hash,
            ),
        );
    }

    for (addr, slot) in claims.storage.keys() {
        if let Some(v) = claims.storage_seed(*addr, *slot, before) {
            seed.storage.insert((*addr, *slot), v);
        }
    }
    Ok(seed)
}

// Record dispatch (tx vs. deposit, plus the deposit fold) lives in the
// exec core (`stateless::execute_record_in_scope`). The batch path below
// runs the same dispatch as the sequential driver and the zk guest, so the
// actor's streaming arms are the only other dispatch left in the tree.
use kardamom_engine::stateless::execute_record_in_scope;

/// Execute one batch sequentially over `snapshot` and `seed`. `first_index`
/// is the batch's first bal index (1-based). Receipts carry local
/// cumulative gas.
pub fn execute_batch<S: StateDatabase>(
    snapshot: &S,
    seed: &PendingDelta,
    records: &[BufferedRecord],
    claims: &ClaimIndex,
    env: ExecEnv,
    first_index: u64,
    granularity: u16,
) -> Result<BatchOutcome, ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(records.len());
    let mut cumulative = 0u64;
    // At granularity K > 1, the wire claims are chunk-collapsed, so a
    // per-tx comparison is impossible. Verification coarsens to the
    // chunk. The batch is chunk-aligned (batch_size == K, enforced by the
    // caller), its captured Bal is quantized through the same shared code
    // the executor used, and it is compared once at the end.
    let mut batch_bal = revm::state::bal::Bal::new();
    // One execution scope per batch: the EVM and commit-into cache are
    // reused across the batch's txs. Per-tx construction was about 90% of
    // execution-path allocation. The seed layer plays the parent role.
    let mut scope = kardamom_engine::executor::Executor::new(snapshot, Some(seed), env)?;
    for (i, rec) in records.iter().enumerate() {
        let bal_index = first_index + i as u64;
        let global_index_in_block = bal_index - 1;
        // Recompute each record's claims through the executor's exact
        // capture path: revm's Bal records per-field changes for txs, and
        // the synthetic WriteSet path handles deposits. Comparing a
        // WriteSet projection instead diverged on every live transfer.
        // Building both sides the same way is the only drift-proof
        // comparison.
        let (receipt, ws) = execute_record_in_scope(
            &mut scope,
            rec,
            global_index_in_block,
            cumulative,
            Some((&mut batch_bal, bal_index)),
        )?;
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    // Verify claims where they are produced. At granularity 1, that is per
    // tx. A batch-final comparison alone would leave intra-batch claims
    // unchecked (neither as seeds nor as outputs), so a wrong intermediate
    // attribution could ship while the final state matched. At K > 1, the
    // finest producible unit is the chunk, and the aligned batch is one
    // chunk. Both sides of the comparison pass through the shared
    // capture and quantize path, so shape drift cannot happen.
    let computed_alloy =
        kardamom_engine::bal_ladder::quantize(batch_bal.into_alloy_bal(), granularity);
    let computed_idx = ClaimIndex::from_alloy(&computed_alloy);
    let k = u64::from(granularity.max(1));
    let last_index = first_index + records.len() as u64 - 1;
    if granularity <= 1 {
        for unit in first_index..=last_index {
            let claimed = claims.claims_in_range(unit, unit);
            let computed = computed_idx.claims_in_range(unit, unit);
            if claimed != computed {
                return Err(ExecutorError::Divergence(format!(
                    "tx {unit}: {}",
                    claimed.diff_summary(&computed)
                )));
            }
        }
    } else {
        let chunk = kardamom_engine::bal_ladder::chunk_of(first_index, k);
        let claimed = claims.claims_in_range(chunk, chunk);
        let computed = computed_idx.claims_in_range(chunk, chunk);
        if claimed != computed {
            return Err(ExecutorError::Divergence(format!(
                "chunk {chunk} (txs {first_index}..={last_index}): {}",
                claimed.diff_summary(&computed)
            )));
        }
    }
    Ok(BatchOutcome {
        first_index,
        receipts,
        delta,
    })
}

/// Verified result of a whole block.
#[derive(Debug)]
pub struct BlockOutcome {
    /// Receipts in block order, with block-cumulative gas fixed up.
    pub receipts: Vec<Receipt>,
    /// The block's merged writes (fold of every batch, in block order).
    pub delta: PendingDelta,
    /// Batches executed (for telemetry).
    pub batches: usize,
}

/// Re-execute a block's records, transactions and deposits, as fully
/// parallel batches. Each batch seeds from the BAL's claims, and the
/// function verifies every batch's claims where they are produced.
/// Deposits occupy bal indices in the same space as txs (the executor's
/// streaming capture passes `tx_index_in_block + 1` for both), so their
/// claims seed later batches exactly like tx claims: the mint is a
/// balance claim, and a CREATE deposit's bytecode is a code claim.
///
/// Returns `Err(ExecutorError::Divergence)` on the first batch whose
/// recomputed writes differ from what the executor claimed. The claim was
/// checked at its producing batch, so a false claim cannot be laundered
/// by later batches that only consume it.
#[allow(clippy::too_many_arguments)] // This groups the pool handle and the
// block-execution inputs. A params struct would rename the same eight
// fields without removing any.
pub fn execute_block_parallel<S: StateDatabase + Sync>(
    pool: &WorkerPool,
    snapshot: &S,
    parent: Option<&PendingDelta>,
    txs: &[BufferedRecord],
    claims: &ClaimIndex,
    env: ExecEnv,
    batch_size: usize,
    granularity: u16,
) -> Result<BlockOutcome, ExecutorError> {
    if txs.is_empty() {
        return Ok(BlockOutcome {
            receipts: Vec::new(),
            delta: PendingDelta::new(),
            batches: 0,
        });
    }
    // Same-view invariant: the attribution granularity comes from the
    // frame, what the executor actually produced, never from local
    // config. At K > 1, execution batches must be chunk-aligned (batch
    // size == K, and ranges tile from index 1), so the chunk a batch
    // verifies is exactly the chunk the executor collapsed. Claims, and
    // so seeds, are chunk-indexed at K > 1. The pool only distributes the
    // indices of these pre-computed ranges, so it cannot re-batch.
    let k = u64::from(granularity.max(1));
    let effective_batch = if granularity > 1 {
        granularity as usize
    } else {
        batch_size
    };
    let ranges = batch_ranges(txs.len(), effective_batch);

    // Each pool worker gets one independent snapshot (fork_view). Sharing
    // one mdbx snapshot serializes every worker's reads through its
    // single read-only txn's cursors, which measured slower than
    // sequential at 4 workers. A fork can be refused (the writer advanced
    // mid-mint, which is routine under the depth-K commit pipeline). That
    // worker then shares the strategy's snapshot: correct, but
    // serialized. The fallback is counted, so a silent loss of the fix
    // shows up on the dashboard.
    let forks: Vec<Option<S>> = (0..pool.workers()).map(|_| snapshot.fork_view()).collect();
    let refused = forks.iter().filter(|f| f.is_none()).count();
    if refused > 0 {
        crate::metrics::counter_fork_fallback(refused as u64);
    }

    // Every batch runs concurrently on the persistent pool. Its inputs
    // come from the claims, so no batch waits on another. Results land in
    // per-chunk slots, already in first_index order, since ranges tile
    // from 1.
    let slots: Vec<std::sync::OnceLock<Result<BatchOutcome, ExecutorError>>> =
        ranges.iter().map(|_| std::sync::OnceLock::new()).collect();
    let body = |lane: usize, ci: usize| {
        let (from, to) = ranges[ci];
        let slice = &txs[(from as usize - 1)..(to as usize)];
        let snap: &S = forks[lane].as_ref().unwrap_or(snapshot);
        // Seeds look up "latest claim strictly before this batch" in the
        // claim index space: tx indices at K = 1, chunk numbers at K > 1.
        let before = if k > 1 {
            kardamom_engine::bal_ladder::chunk_of(from, k)
        } else {
            from
        };
        let out = build_seed(snap, parent, claims, before)
            .and_then(|seed| execute_batch(snap, &seed, slice, claims, env, from, granularity));
        let _ = slots[ci].set(out);
    };
    pool.run(ranges.len(), &body)
        .map_err(|p| ExecutorError::State(format!("batch worker panicked: {p}")))?;

    // Verify each batch's claims, then fold in block order.
    let mut outcomes = Vec::with_capacity(slots.len());
    for s in slots {
        outcomes.push(s.into_inner().expect("pool ran every chunk")?);
    }
    // Slots are already in block order. This sort stays as a cheap,
    // explicit statement of the fold's ordering invariant.
    outcomes.sort_by_key(|o| o.first_index);

    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(txs.len());
    let mut cumulative = 0u64;
    for o in outcomes.iter() {
        // Claims were verified per tx inside each batch. This is strictly
        // stronger than a batch-final comparison, which cannot see
        // intra-batch attribution.
        // Fold: later batches overwrite earlier ones, in block order.
        delta.merge_from(&o.delta);
        // Block-cumulative gas: each batch computed its gas locally from 0.
        for r in &o.receipts {
            let mut r = r.clone();
            cumulative += r.gas_used;
            r.cumulative_gas_used = cumulative;
            receipts.push(r);
        }
    }

    Ok(BlockOutcome {
        receipts,
        delta,
        batches: ranges.len(),
    })
}

/// The pre-pool implementation, with one `std::thread::scope` spawn per
/// batch. It stays here, compiling, as the reference for the pooled path;
/// engine_tests and the bench repro sweep compare the two byte-for-byte.
/// It is not a production path: `parallel_block_exec` always dispatches
/// onto the persistent pool.
#[doc(hidden)]
pub fn execute_block_parallel_scoped<S: StateDatabase + Sync>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    txs: &[BufferedRecord],
    claims: &ClaimIndex,
    env: ExecEnv,
    batch_size: usize,
    granularity: u16,
) -> Result<BlockOutcome, ExecutorError> {
    if txs.is_empty() {
        return Ok(BlockOutcome {
            receipts: Vec::new(),
            delta: PendingDelta::new(),
            batches: 0,
        });
    }
    let k = u64::from(granularity.max(1));
    let effective_batch = if granularity > 1 {
        granularity as usize
    } else {
        batch_size
    };
    let ranges = batch_ranges(txs.len(), effective_batch);
    let results: Vec<Result<BatchOutcome, ExecutorError>> = std::thread::scope(|scope| {
        let handles: Vec<_> = ranges
            .iter()
            .map(|(from, to)| {
                let slice = &txs[(*from as usize - 1)..(*to as usize)];
                let from = *from;
                scope.spawn(move || {
                    let before = if k > 1 {
                        kardamom_engine::bal_ladder::chunk_of(from, k)
                    } else {
                        from
                    };
                    let seed = build_seed(snapshot, parent, claims, before)?;
                    execute_batch(snapshot, &seed, slice, claims, env, from, granularity)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err(ExecutorError::State("batch worker panicked".into())))
            })
            .collect()
    });
    let mut outcomes = Vec::with_capacity(results.len());
    for r in results {
        outcomes.push(r?);
    }
    outcomes.sort_by_key(|o| o.first_index);
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(txs.len());
    let mut cumulative = 0u64;
    for o in outcomes.iter() {
        delta.merge_from(&o.delta);
        for r in &o.receipts {
            let mut r = r.clone();
            cumulative += r.gas_used;
            r.cumulative_gas_used = cumulative;
            receipts.push(r);
        }
    }
    Ok(BlockOutcome {
        receipts,
        delta,
        batches: ranges.len(),
    })
}

// ---------------------------------------------------------------------------
// Engine strategy: what the validator hands to the exec loop
// ---------------------------------------------------------------------------

/// How long a block waits for its BAL claims before it falls back to
/// sequential re-execution. This is short, so liveness never depends on the BAL.
const CLAIM_WAIT: Duration = Duration::from_millis(250);

/// Sequential re-execution of a whole block: the always-available
/// fallback when the block's BAL claims do not arrive in time. It has the
/// same semantics as the engine's streaming path. The body moved into the
/// `no_std` exec core in phase 3 (the zk guest links the same driver).
/// This delegation is the seam that keeps live-validator and stateless
/// execution one code path.
pub fn execute_block_sequential<S: StateDatabase>(
    snapshot: &S,
    parent: Option<&PendingDelta>,
    records: &[BufferedRecord],
    env: ExecEnv,
) -> Result<BlockExecOutput, ExecutorError> {
    kardamom_engine::stateless::execute_block(snapshot, parent, records, env)
}

/// Build the validator's whole-block execution strategy: seeded parallel
/// batches when this block's BAL claims arrive in time, sequential
/// otherwise. Deposits take part like transactions. The executor captures
/// their writes into the BAL at their block index (mint as a balance
/// claim, CREATE bytecode as a code claim), so deposit-containing blocks
/// validate in parallel too.
pub fn parallel_block_exec<D: StateDatabase + Sync + 'static>(
    claims: Arc<crate::ClaimBuffer>,
    batch_size: usize,
    workers: usize,
    flight: Option<Arc<crate::flight::FlightRing>>,
) -> BlockExec<D> {
    // The pool is built once and captured, so workers persist across
    // blocks. The previous design spawned one OS thread per batch per
    // block, about 500 spawns for a 4k-tx block at K=8, with no bound
    // tied to the machine.
    let pool = Arc::new(WorkerPool::new(workers.max(1), Vec::new()));
    Box::new(
        move |snapshot: &D,
              parent: Option<&PendingDelta>,
              records: &[BufferedRecord],
              env: ExecEnv,
              block: u64| {
            if records.is_empty() {
                // Empty blocks still enter the flight ring. The prover
                // spool proves every block, and a gap here would stall it.
                if let Some(f) = flight.as_ref() {
                    f.push(block, 1, env, records, None);
                }
                return execute_block_sequential(snapshot, parent, records, env);
            }
            let Some((granularity, idx)) = claims.take(block, CLAIM_WAIT) else {
                crate::metrics::counter_parallel_fallback();
                tracing::debug!(block, "no BAL claims in time; sequential re-execution");
                if let Some(f) = flight.as_ref() {
                    f.push(block, 1, env, records, None);
                }
                return execute_block_sequential(snapshot, parent, records, env);
            };
            if let Some(f) = flight.as_ref() {
                f.push(block, granularity, env, records, Some(Arc::clone(&idx)));
            }
            let out = match execute_block_parallel(
                &pool,
                snapshot,
                parent,
                records,
                &idx,
                env,
                batch_size,
                granularity,
            ) {
                Ok(out) => out,
                Err(e) => {
                    // Dump the exact inputs, so the failing block can
                    // replay as a unit test.
                    dump_divergence_inputs(block, records, &idx, parent, granularity, &e);
                    return Err(e);
                }
            };
            crate::metrics::counter_parallel_block(out.batches);
            Ok(BlockExecOutput {
                receipts: out.receipts,
                delta: out.delta,
                // The validator verifies BALs; it never publishes one.
                bal: None,
            })
        },
    )
}
