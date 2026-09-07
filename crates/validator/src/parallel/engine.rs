//! Seeded parallel execution engine: batch seeding/execution/verification,
//! the sequential fallback, and the whole-block strategy handed to the
//! exec loop.

use std::collections::BTreeSet;
use std::num::{NonZeroU16, NonZeroUsize};
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
/// fixes up block-cumulative gas in order), and its merged writes. Slots
/// are already in block order (ranges tile from 1), so the outcome does
/// not need to carry its own `first_index` back out.
pub(crate) struct BatchOutcome {
    pub(crate) receipts: Vec<Receipt>,
    pub(crate) delta: PendingDelta,
}

/// Build the input layer a batch starting at `before` must observe:
/// snapshot state overlaid with the latest claim strictly before the
/// batch, that is, the previous batch's end state. EIP-7928 claims
/// account fields independently, so each triple is built from whichever
/// components have earlier claims, falling back to the snapshot.
pub(crate) fn build_seed<S: StateDatabase>(
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

    // A `BTreeSet` gives the same sorted, deduplicated order as the
    // former `sort_unstable` + `dedup`. Order does not affect
    // correctness either way: `seed.accounts` is a hash map keyed by
    // address (see `PendingDelta`'s doc), and each address here is
    // visited once, so insertion order cannot change the result.
    let addrs: BTreeSet<Address> = claims
        .balance
        .keys()
        .chain(claims.nonce.keys())
        .chain(claims.code.keys())
        .copied()
        .collect();
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

// Record dispatch (tx vs. deposit vs. cross-chain delivery) lives in the
// exec core (`stateless::execute_record_in_scope`). The batch path below
// runs the same dispatch as the sequential driver and the zk guest, so the
// actor's streaming arms are the only other dispatch left in the tree.
use kardamom_engine::stateless::execute_record_in_scope;

impl super::claims::ClaimSlice {
    /// Remove from `self` the entries `claimed` lacks whose value equals
    /// the latest claimed value strictly before `unit` — that is, entries
    /// that restate the already-established value.
    ///
    /// A deposit or cross-chain delivery is a commit-cache style record:
    /// its `WriteSet` runs through `record_writeset_into_bal`, which
    /// fabricates an "original" value to force write classification, so
    /// the batch's fresh `Bal` claims it unconditionally, even when the
    /// value did not change (the fee recipient at its unchanged balance,
    /// for example). The executor's whole-block `Bal` only claims a
    /// write when the value changed versus the last recorded write, so
    /// it dedups this same case away. Comparing the two Bal shapes
    /// directly then reports a false divergence: the batch claims a
    /// write the block-level Bal never made.
    ///
    /// Equality with the prior claim is exactly the dedup condition the
    /// block capture applied, so dropping these entries here is
    /// lossless: a genuinely different value still diverges, and the
    /// reverse direction (claimed but not recomputed) stays strict,
    /// since both sides dedup against the same last-recorded value.
    fn drop_wire_deduped(&mut self, claimed: &Self, claims: &ClaimIndex, unit: u64) {
        self.storage.retain(|(addr, slot), v| {
            claimed.storage.contains_key(&(*addr, *slot))
                || claims.storage_seed(*addr, *slot, unit) != Some(*v)
        });
        self.balance.retain(|addr, v| {
            claimed.balance.contains_key(addr) || claims.balance_seed(*addr, unit) != Some(*v)
        });
        self.nonce.retain(|addr, v| {
            claimed.nonce.contains_key(addr) || claims.nonce_seed(*addr, unit) != Some(*v)
        });
        self.code.retain(|addr, h| {
            claimed.code.contains_key(addr)
                || claims
                    .code_seed(*addr, unit)
                    .map(alloy_primitives::keccak256)
                    != Some(*h)
        });
    }
}

/// Execute one batch sequentially over `snapshot` and `seed`. `first_index`
/// is the batch's first bal index (1-based). Receipts carry local
/// cumulative gas.
pub(crate) fn execute_batch<S: StateDatabase>(
    snapshot: &S,
    seed: &PendingDelta,
    records: &[BufferedRecord],
    claims: &ClaimIndex,
    env: ExecEnv,
    first_index: u64,
    granularity: NonZeroU16,
) -> Result<BatchOutcome, ExecutorError> {
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
    let (receipts, delta) = run_records(&mut scope, records, first_index, &mut batch_bal)?;

    // Verify claims where they are produced. At granularity 1, that is per
    // tx. A batch-final comparison alone would leave intra-batch claims
    // unchecked (neither as seeds nor as outputs), so a wrong intermediate
    // attribution could ship while the final state matched. At K > 1, the
    // finest producible unit is the chunk, and the aligned batch is one
    // chunk. Both sides of the comparison pass through the shared
    // capture and quantize path, so shape drift cannot happen.
    // Bounded: `batch_ranges` (the only caller of `execute_batch`, via
    // `run_batches`) emits `first_index >= 1` and a non-empty slice for
    // every range, and `execute_batch` is `pub(crate)`, so no outside
    // caller can pass `first_index == 0` with an empty `records`.
    let last_index = first_index + records.len() as u64 - 1;
    verify_units(claims, batch_bal, first_index, last_index, granularity)?;

    Ok(BatchOutcome { receipts, delta })
}

/// Execute `records` sequentially through one execution scope, starting at
/// `first_index`. Returns the batch's receipts, with local cumulative gas,
/// and its merged writes. Also records each record's claims into
/// `batch_bal`, through the executor's exact capture path: revm's `Bal`
/// records per-field changes for txs, and the synthetic `WriteSet` path
/// handles deposits.
fn run_records<S: StateDatabase>(
    scope: &mut kardamom_engine::executor::Executor<&S>,
    records: &[BufferedRecord],
    first_index: u64,
    batch_bal: &mut revm::state::bal::Bal,
) -> Result<(Vec<Receipt>, PendingDelta), ExecutorError> {
    let mut delta = PendingDelta::new();
    let mut receipts = Vec::with_capacity(records.len());
    let mut cumulative = 0u64;
    for (i, rec) in records.iter().enumerate() {
        // Bounded: `first_index >= 1` (see `execute_batch`'s comment), so
        // `bal_index >= 1` and `bal_index - 1` cannot underflow.
        let bal_index = first_index + i as u64;
        let global_index_in_block = bal_index - 1;
        let (receipt, ws) = execute_record_in_scope(
            scope,
            rec,
            global_index_in_block,
            cumulative,
            Some((batch_bal, bal_index)),
        )?;
        cumulative = receipt.cumulative_gas_used;
        delta.apply(ws);
        receipts.push(receipt);
    }
    Ok((receipts, delta))
}

/// Verify the batch's recomputed claims (quantized from `batch_bal`)
/// against the wire claims, at tx granularity (K = 1) or chunk
/// granularity (K > 1). Returns [`ExecutorError::Divergence`] naming the
/// first unit (tx or chunk) whose recomputed writes differ from what the
/// executor claimed.
fn verify_units(
    claims: &ClaimIndex,
    batch_bal: revm::state::bal::Bal,
    first_index: u64,
    last_index: u64,
    granularity: NonZeroU16,
) -> Result<(), ExecutorError> {
    let computed_alloy =
        kardamom_engine::bal_ladder::quantize(batch_bal.into_alloy_bal(), granularity.get());
    let computed_idx = ClaimIndex::from_alloy(&computed_alloy);
    if granularity.get() == 1 {
        return (first_index..=last_index).try_for_each(|unit| {
            let claimed = claims.claims_in_range(unit, unit);
            let mut computed = computed_idx.claims_in_range(unit, unit);
            computed.drop_wire_deduped(&claimed, claims, unit);
            if claimed != computed {
                return Err(ExecutorError::Divergence(format!(
                    "tx {unit}: {}",
                    claimed.diff_summary(&computed)
                )));
            }
            Ok(())
        });
    }
    let k = u64::from(granularity.get());
    let chunk = kardamom_engine::bal_ladder::chunk_of(first_index, k);
    let claimed = claims.claims_in_range(chunk, chunk);
    let mut computed = computed_idx.claims_in_range(chunk, chunk);
    computed.drop_wire_deduped(&claimed, claims, chunk);
    if claimed != computed {
        return Err(ExecutorError::Divergence(format!(
            "chunk {chunk} (txs {first_index}..={last_index}): {}",
            claimed.diff_summary(&computed)
        )));
    }
    Ok(())
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
///
/// # Errors
///
/// Returns an error if a batch's recomputed writes diverge from its
/// claimed writes, or if a pool worker panics.
pub fn execute_block_parallel<S: StateDatabase + Sync>(
    pool: &WorkerPool,
    inputs: &BlockInputs<'_, S>,
    batch_size: NonZeroUsize,
) -> Result<BlockOutcome, ExecutorError> {
    if inputs.txs.is_empty() {
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
    let g = NonZeroUsize::from(inputs.granularity);
    let effective_batch = if g.get() > 1 { g } else { batch_size };
    let ranges = batch_ranges(inputs.txs.len(), effective_batch);
    let forks = fork_snapshots(pool, inputs.snapshot);
    let outcomes = run_batches(pool, &ranges, &forks, inputs)?;
    let mut fold = BlockFold::new();
    for o in outcomes {
        fold.absorb(o);
    }
    Ok(fold.finish(ranges.len()))
}

/// The inputs one block's batches share; every batch reads a slice of
/// `txs` and seeds from `claims`. Public so the caller builds it once
/// and this module never re-derives it from a loose argument list.
pub struct BlockInputs<'a, S> {
    pub snapshot: &'a S,
    pub parent: Option<&'a PendingDelta>,
    pub txs: &'a [BufferedRecord],
    pub claims: &'a ClaimIndex,
    pub env: ExecEnv,
    pub granularity: NonZeroU16,
}

/// Fork one independent snapshot per pool worker, so worker reads do not
/// serialize through one shared cursor. Sharing one mdbx snapshot
/// serializes every worker's reads through its single read-only txn's
/// cursors, which measured slower than sequential at 4 workers.
///
/// A fork can be refused (the writer advanced mid-mint, which is routine
/// under the depth-K commit pipeline); that worker then shares the
/// caller's own snapshot instead, correct but serialized. The fallback is
/// counted, so a silent loss of the fix shows up on the dashboard.
fn fork_snapshots<S: StateDatabase + Sync>(pool: &WorkerPool, snapshot: &S) -> Vec<Option<S>> {
    let forks: Vec<Option<S>> = (0..pool.workers()).map(|_| snapshot.fork_view()).collect();
    let refused = forks.iter().filter(|f| f.is_none()).count();
    if refused > 0 {
        crate::metrics::counter_fork_fallback(refused as u64);
    }
    forks
}

/// Run every batch on `pool`, and return the outcomes in block order.
/// Every batch runs concurrently; its inputs come from the claims, so no
/// batch waits on another. Each worker's result lands in the slot
/// matching its own range, and ranges tile from 1, so the slots are
/// already in block order.
fn run_batches<S: StateDatabase + Sync>(
    pool: &WorkerPool,
    ranges: &[(u64, u64)],
    forks: &[Option<S>],
    inputs: &BlockInputs<'_, S>,
) -> Result<Vec<BatchOutcome>, ExecutorError> {
    let k = u64::from(inputs.granularity.get());
    let slots: Vec<std::sync::OnceLock<Result<BatchOutcome, ExecutorError>>> =
        ranges.iter().map(|_| std::sync::OnceLock::new()).collect();
    let body = |lane: usize, ci: usize| {
        let (from, to) = ranges[ci];
        #[allow(
            clippy::cast_possible_truncation,
            reason = "from and to are batch_ranges(txs.len(), ..) outputs, so both are bounded by txs.len(), a usize"
        )]
        let slice = &inputs.txs[(from as usize - 1)..(to as usize)];
        let snap: &S = forks[lane].as_ref().unwrap_or(inputs.snapshot);
        // Seeds look up "latest claim strictly before this batch" in the
        // claim index space: tx indices at K = 1, chunk numbers at K > 1.
        let before = if k > 1 {
            kardamom_engine::bal_ladder::chunk_of(from, k)
        } else {
            from
        };
        let out = build_seed(snap, inputs.parent, inputs.claims, before).and_then(|seed| {
            execute_batch(
                snap,
                &seed,
                slice,
                inputs.claims,
                inputs.env,
                from,
                inputs.granularity,
            )
        });
        let _ = slots[ci].set(out);
    };
    pool.run(ranges.len(), &body)
        .map_err(|p| ExecutorError::State(format!("batch worker panicked: {p}")))?;

    // Claims were verified per tx inside each batch. This is strictly
    // stronger than a batch-final comparison, which cannot see
    // intra-batch attribution.
    slots
        .into_iter()
        .map(|s| s.into_inner().expect("pool ran every chunk"))
        .collect()
}

/// Folds every batch's outcome into one block, in block order: later
/// batches overwrite earlier ones, and each batch's locally computed gas
/// becomes block-cumulative gas. `cumulative` runs across every batch
/// absorbed, so gas stays block-cumulative rather than resetting per
/// batch.
struct BlockFold {
    delta: PendingDelta,
    receipts: Vec<Receipt>,
    cumulative: u64,
}

impl BlockFold {
    fn new() -> Self {
        Self {
            delta: PendingDelta::new(),
            receipts: Vec::new(),
            cumulative: 0,
        }
    }

    /// Absorb one batch's outcome: merge its writes, and fix up its
    /// receipts' gas from batch-local to block-cumulative. Takes `o` by
    /// value so this hot fold moves each receipt in instead of cloning it.
    fn absorb(&mut self, o: BatchOutcome) {
        self.delta.merge_from(&o.delta);
        for mut r in o.receipts {
            self.cumulative += r.gas_used;
            r.cumulative_gas_used = self.cumulative;
            self.receipts.push(r);
        }
    }

    fn finish(self, batches: usize) -> BlockOutcome {
        BlockOutcome {
            receipts: self.receipts,
            delta: self.delta,
            batches,
        }
    }
}

// ---------------------------------------------------------------------------
// Engine strategy: what the validator hands to the exec loop
// ---------------------------------------------------------------------------

/// How long a block waits for its BAL claims before it falls back to
/// sequential re-execution. This is short, so liveness never depends on the BAL.
const CLAIM_WAIT: Duration = Duration::from_millis(250);

/// Sequential re-execution of a whole block: the always-available
/// fallback when the block's BAL claims do not arrive in time. It has the
/// same semantics as the engine's streaming path. The body lives in the
/// `no_std` exec core, which the zk guest links too; this delegation is
/// the seam that keeps live-validator and stateless execution one code
/// path.
pub(crate) fn execute_block_sequential<S: StateDatabase>(
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
    batch_size: NonZeroUsize,
    workers: NonZeroUsize,
    flight: Option<Arc<crate::flight::FlightRing>>,
) -> BlockExec<D> {
    // The pool is built once and captured, so workers persist across
    // blocks, instead of spawning one OS thread per batch per block.
    let pool = Arc::new(WorkerPool::new(workers.get(), Vec::new()));
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
                    f.push(block, NonZeroU16::MIN, env, records, None);
                }
                return execute_block_sequential(snapshot, parent, records, env);
            }
            let Some((granularity, idx)) = claims.take(block, CLAIM_WAIT) else {
                crate::metrics::counter_parallel_fallback();
                tracing::debug!(block, "no BAL claims in time; sequential re-execution");
                if let Some(f) = flight.as_ref() {
                    f.push(block, NonZeroU16::MIN, env, records, None);
                }
                return execute_block_sequential(snapshot, parent, records, env);
            };
            if let Some(f) = flight.as_ref() {
                f.push(block, granularity, env, records, Some(Arc::clone(&idx)));
            }
            let inputs = BlockInputs {
                snapshot,
                parent,
                txs: records,
                claims: &idx,
                env,
                granularity,
            };
            let out = match execute_block_parallel(&pool, &inputs, batch_size) {
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
