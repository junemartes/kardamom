//! This is an offline reproducer for `DeFi` parallel-execution
//! divergence.
//!
//! The invariant under test: `execute_block_parallel` at any worker
//! count must match sequential execution through the executor's exact
//! capture path, for every account and slot, on every block. This is a
//! deterministic workload point, not chaos: the same composition must
//! diverge the same way on every run, or not at all. This sweep runs
//! many deterministic block compositions of the real bench contracts
//! at K=20 and K=1, and compares each against sequential execution.
//! Any composition that diverges is the repro.

use std::num::{NonZeroU16, NonZeroUsize};

use alloy_primitives::Address;
use kardamom_bench::load::defi::{deployment_txs, pregenerate_defi};
use kardamom_bench::load::plan::{PlannedTx, TxPlanParams};
use kardamom_bench::signers::SignerSet;
use kardamom_bench::stm::{BlockAt, SeqExec};
use kardamom_engine::actor::BufferedRecord;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_types::{BPosition, TxEnvelope};
use kardamom_validator::parallel::{BatchSize, BlockInputs, ClaimIndex, execute_block_parallel};

use kardamom_bench::ANVIL_MNEMONIC as ANVIL_PHRASE;
const CHAIN_ID: u64 = 412_346;
const SENDERS: u32 = 15;
/// `execute_block_parallel`'s batch size for every repro sweep in this
/// file: 8 transactions per batch, unless the frame's own granularity
/// forces a chunk-aligned batch (see `BlockInputs::granularity`).
const BATCH_SIZE_8: BatchSize = BatchSize::new(NonZeroUsize::new(8).unwrap());
/// `repro_pool`'s fixed worker count.
const POOL_WORKERS: NonZeroUsize = NonZeroUsize::new(4).unwrap();

/// `block` is 1-based, matching every call site below.
fn env_for(block: u64) -> ExecEnv {
    let index = usize::try_from(block - 1).expect("block index fits in usize on this target");
    BlockAt(index).env(CHAIN_ID)
}

fn envelope(t: &PlannedTx, sender: Address, i: u64) -> TxEnvelope {
    t.to_envelope(sender, i)
}

/// This is xorshift, for deterministic composition shuffling with no
/// external RNG dependency.
fn xs(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// The shared pool for the sweep. It is persistent, like production.
fn repro_pool() -> &'static kardamom_stm::pool::WorkerPool {
    use std::sync::OnceLock;
    static POOL: OnceLock<kardamom_stm::pool::WorkerPool> = OnceLock::new();
    POOL.get_or_init(|| kardamom_stm::pool::WorkerPool::new(POOL_WORKERS, &[]))
}

/// Execute one setup transaction `t` (sent by `sender`) on `setup`, at
/// the current block-global index `gi`. Asserts it succeeded, folds
/// its write set into `parent`, and advances `gi`.
fn run_one_setup_tx(
    setup: &mut SeqExec<'_, kardamom_engine::state::MockStateDatabase>,
    parent: &mut PendingDelta,
    gi: &mut u64,
    t: &PlannedTx,
    sender: Address,
) {
    let slot = kardamom_engine::exec_types::TxSlot {
        tx_idx: TxIndex(*gi),
        tx_position: BPosition::from_index(*gi),
        tx_index_in_block: *gi,
        cumulative_gas_used_before: 0,
    };
    let out = setup
        .run(slot, &envelope(t, sender, *gi), None)
        .expect("setup tx");
    assert!(out.receipt.status);
    parent.apply(out.write_set);
    *gi += 1;
}

/// Deploy every contract, then run every sender's seed operation, in
/// order, on one scope. Returns the accumulated write sets, to become
/// the parent layer for the repro blocks.
fn run_setup_block(
    snap: &kardamom_engine::state::MockStateDatabase,
    signers: &SignerSet,
    deploys: &[PlannedTx],
    queues: &[Vec<PlannedTx>],
    env: ExecEnv,
) -> PendingDelta {
    let mut parent = PendingDelta::new();
    let mut gi = 0u64;
    let mut setup = SeqExec::new(snap, None, env).expect("open setup scope");
    for d in deploys {
        run_one_setup_tx(
            &mut setup,
            &mut parent,
            &mut gi,
            d,
            signers[0].signer.address(),
        );
    }
    for (si, q) in queues.iter().enumerate() {
        let t = &q[0]; // This is the seed() operation.
        run_one_setup_tx(
            &mut setup,
            &mut parent,
            &mut gi,
            t,
            signers[si].signer.address(),
        );
    }
    parent
}

/// Pick the next round-robin sender and operation, or `None` if that
/// sender's queue is exhausted at its current cursor.
#[allow(
    clippy::cast_possible_truncation,
    reason = "si stays under queues.len(), a small fixed test parameter"
)]
fn try_pick<'a>(
    rng: &mut u64,
    queues: &'a [Vec<PlannedTx>],
    next_op_idx: &mut [usize],
) -> Option<(usize, &'a PlannedTx)> {
    let si = (xs(rng) % queues.len() as u64) as usize;
    let idx = next_op_idx[si];
    if idx >= queues[si].len() {
        return None;
    }
    next_op_idx[si] += 1;
    Some((si, &queues[si][idx]))
}

/// Compose one block: weighted round-robin over `queues`, with random
/// skips, up to `count` transactions. Each sender's order within the
/// block stays nonce-monotonic, since the cursor only advances.
fn compose_block<'a>(
    rng: &mut u64,
    queues: &'a [Vec<PlannedTx>],
    next_op_idx: &mut [usize],
    count: usize,
) -> Vec<(usize, &'a PlannedTx)> {
    let mut txs = Vec::with_capacity(count);
    while txs.len() < count {
        txs.extend(try_pick(rng, queues, next_op_idx));
    }
    txs
}

/// The sequential truth for one block: the resulting delta, the
/// whole-block access list captured alongside it, and each receipt's
/// status.
struct SequentialTruth {
    delta: PendingDelta,
    alloy_bal: alloy_eip7928::BlockAccessList,
    statuses: Vec<bool>,
}

/// Execute one transaction `t` (sent by `sender`) on `exec`, at index
/// `i`, capturing its touches into `bal`. Folds its write set into
/// `delta`, and returns the receipt's status.
fn run_one_seq_tx(
    exec: &mut SeqExec<'_, kardamom_engine::state::MockStateDatabase>,
    delta: &mut PendingDelta,
    bal: &mut revm::state::bal::Bal,
    i: u64,
    t: &PlannedTx,
    sender: Address,
) -> bool {
    let slot = kardamom_engine::exec_types::TxSlot {
        tx_idx: TxIndex(i),
        tx_position: BPosition::from_index(i),
        tx_index_in_block: i,
        cumulative_gas_used_before: 0,
    };
    let out = exec
        .run(slot, &envelope(t, sender, i), Some((bal, i + 1)))
        .expect("seq");
    delta.apply(out.write_set);
    out.receipt.status
}

/// Execute `txs` sequentially on one scope over `parent`, capturing a
/// whole-block `Bal` alongside each receipt's status and the resulting
/// delta. This is the sequential truth the parallel executor's claims
/// are checked against.
fn run_sequential_block(
    snap: &kardamom_engine::state::MockStateDatabase,
    signers: &SignerSet,
    parent: &PendingDelta,
    env: ExecEnv,
    txs: &[(usize, &PlannedTx)],
) -> SequentialTruth {
    let mut seq_delta = PendingDelta::new();
    let mut bal = revm::state::bal::Bal::new();
    let mut statuses = Vec::with_capacity(txs.len());
    let mut exec = SeqExec::new(snap, Some(parent), env).expect("open block scope");
    for (i, (si, t)) in txs.iter().enumerate() {
        let status = run_one_seq_tx(
            &mut exec,
            &mut seq_delta,
            &mut bal,
            i as u64,
            t,
            signers[*si].signer.address(),
        );
        statuses.push(status);
    }
    SequentialTruth {
        delta: seq_delta,
        alloy_bal: bal.into_alloy_bal(),
        statuses,
    }
}

/// One block's check inputs: the sequential truth and the fixed
/// arguments `execute_block_parallel` needs, gathered so a check at
/// one worker-shard count `k` reads as one call, not nine arguments.
struct BlockCheck<'a> {
    block: u64,
    snap: &'a kardamom_engine::state::MockStateDatabase,
    parent: &'a PendingDelta,
    block_txs: &'a [BufferedRecord],
    env: ExecEnv,
    truth: &'a SequentialTruth,
}

impl BlockCheck<'_> {
    /// Check both worker-shard counts this sweep runs at, K=1 and K=20,
    /// against the sequential truth for this block.
    fn run(&self) {
        for k in [1u16, 20] {
            self.check_one_k(k);
        }
    }

    /// Check the parallel executor's claims at one worker-shard count
    /// `k` against the sequential truth for this block.
    fn check_one_k(&self, k: u16) {
        let q = kardamom_engine::bal_ladder::quantize(self.truth.alloy_bal.clone(), k);
        let claims = ClaimIndex::from_alloy(&q);
        let inputs = BlockInputs {
            snapshot: self.snap,
            parent: Some(self.parent),
            txs: self.block_txs,
            claims: &claims,
            env: self.env,
            granularity: NonZeroU16::new(k).expect("K is 1 or 20 in this sweep"),
        };
        let out = execute_block_parallel(repro_pool(), &inputs, BATCH_SIZE_8).unwrap_or_else(|e| {
            let block = self.block;
            let statuses = &self.truth.statuses;
            panic!(
                "REPRO block {block} K={k}: {e:?}\n  statuses: {statuses:?}\n  rng composition block {block}"
            )
        });
        assert_eq!(
            out.delta.accounts, self.truth.delta.accounts,
            "block {} K={k}: account state mismatch",
            self.block
        );
        assert_eq!(
            out.delta.storage, self.truth.delta.storage,
            "block {} K={k}: storage mismatch",
            self.block
        );
    }
}

/// Compose, check (both worker-shard counts against the sequential
/// truth), and fold one repro block into the sweep's running `parent`.
#[allow(
    clippy::too_many_arguments,
    reason = "each argument is a distinct piece of the sweep's carried-forward state; the caller reads the whole sweep loop as one call per block"
)]
fn check_one_block(
    snap: &kardamom_engine::state::MockStateDatabase,
    signers: &SignerSet,
    queues: &[Vec<PlannedTx>],
    next_op_idx: &mut [usize],
    rng: &mut u64,
    parent: &mut PendingDelta,
    block: u64,
) {
    let env = env_for(block);
    let txs = compose_block(rng, queues, next_op_idx, 60);
    let truth = run_sequential_block(snap, signers, parent, env, &txs);
    let block_txs = to_buffered_records(&txs, signers);
    BlockCheck {
        block,
        snap,
        parent,
        block_txs: &block_txs,
        env,
        truth: &truth,
    }
    .run();
    parent.merge_from(&truth.delta);
}

/// Build the `BufferedRecord` view of `txs`, for the parallel executor.
fn to_buffered_records(txs: &[(usize, &PlannedTx)], signers: &SignerSet) -> Vec<BufferedRecord> {
    txs.iter()
        .enumerate()
        .map(|(i, (si, t))| BufferedRecord::Tx {
            tx_idx: TxIndex(i as u64),
            position: BPosition::from_index(i as u64),
            envelope: envelope(t, signers[*si].signer.address(), i as u64),
        })
        .collect()
}

/// Run the parallel executor at worker-shard count `k` against
/// `block_txs`, quantizing `alloy_bal`'s claims at that granularity,
/// and assert its delta matches `seq_delta` exactly.
fn assert_matches_at_granularity(
    k: u16,
    snap: &kardamom_engine::state::MockStateDatabase,
    block_txs: &[BufferedRecord],
    env: ExecEnv,
    alloy_bal: &alloy_eip7928::BlockAccessList,
    seq_delta: &PendingDelta,
) {
    let q = kardamom_engine::bal_ladder::quantize(alloy_bal.clone(), k);
    let claims = ClaimIndex::from_alloy(&q);
    let inputs = BlockInputs {
        snapshot: snap,
        parent: None,
        txs: block_txs,
        claims: &claims,
        env,
        granularity: NonZeroU16::new(k).expect("K is 1 or 20 in this sweep"),
    };
    let out = execute_block_parallel(repro_pool(), &inputs, BATCH_SIZE_8)
        .unwrap_or_else(|e| panic!("K={k} create-then-call diverged: {e:?}"));
    assert_eq!(out.delta.storage, seq_delta.storage, "K={k}: storage");
    assert_eq!(out.delta.accounts, seq_delta.accounts, "K={k}: accounts");
}

#[test]
fn k20_defi_parallel_matches_sequential_across_compositions() {
    let signers = SignerSet::derive(ANVIL_PHRASE, SENDERS).unwrap();
    let snap = kardamom_bench::stm::workload::funded_snapshot(&signers);
    let plan_params = TxPlanParams {
        chain_id: CHAIN_ID,
        nonce_start: 0,
        gas_price: 1_000_000_000,
    };
    let dep = deployment_txs(&signers, plan_params).unwrap();
    let queues = pregenerate_defi(&signers, &dep.contracts, 120, plan_params).unwrap();
    assert!(!queues.is_empty(), "SENDERS must be at least 1");

    // Block 1 is setup. The accumulated write sets become the parent
    // layer for the repro blocks.
    let parent = run_setup_block(&snap, &signers, &dep.txs, &queues, env_for(1));

    // Sweep: many deterministic compositions of blocks starting at 2.
    // Interleave the senders' operation queues under different shuffles,
    // 60 transactions per block, and check K=20 and K=1 parallel against
    // sequential for each.
    let mut next_op_idx = vec![1usize; queues.len()]; // A per-sender queue cursor.
    let mut rng: u64 = 0x00C0_FFEE_D15E_A5E5;
    let mut parent = parent;
    for block in 2..14u64 {
        check_one_block(
            &snap,
            &signers,
            &queues,
            &mut next_op_idx,
            &mut rng,
            &mut parent,
            block,
        );
    }
}

/// This is the burst-block case that produced the live divergence. A
/// stall makes the sealer pack the whole backlog into one giant block,
/// so the contract deployments land in chunk 1 and the calls land in
/// later chunks: a CREATE, then a CALL, across a chunk boundary within
/// one block.
///
/// The original spec excluded code from attribution, reasoning that
/// the account-entry dependency orders it. That reasoning is correct
/// for the wave-DAG model, but wrong for the seeded model: batch 2
/// never waits for batch 1. So without code claims, its seed has
/// account entries but empty bytecode, every contract call becomes a
/// no-op, and verification reports "recomputed absent".
#[test]
fn create_then_call_across_chunks_in_one_block() {
    let signers = SignerSet::derive(ANVIL_PHRASE, SENDERS).unwrap();
    let snap = kardamom_bench::stm::workload::funded_snapshot(&signers);
    let plan_params = TxPlanParams {
        chain_id: CHAIN_ID,
        nonce_start: 0,
        gas_price: 1_000_000_000,
    };
    let dep = deployment_txs(&signers, plan_params).unwrap();
    let queues = pregenerate_defi(&signers, &dep.contracts, 8, plan_params).unwrap();

    // One block: the deploys (bal 1-3), all seeds, then two rounds of
    // operations. This spans multiple K=20 chunks, with the CREATEs in
    // chunk 1.
    let mut txs: Vec<(usize, &PlannedTx)> = dep.txs.iter().map(|d| (0usize, d)).collect();
    for (si, q) in queues.iter().enumerate() {
        txs.push((si, &q[0])); // This is the seed() operation.
    }
    txs.extend((1..3).flat_map(|round| {
        queues
            .iter()
            .enumerate()
            .map(move |(si, q)| (si, &q[round]))
    }));
    assert!(txs.len() > 40, "must span >2 chunks at K=20: {}", txs.len());

    let env = env_for(2);
    let mut seq_delta = PendingDelta::new();
    let mut bal = revm::state::bal::Bal::new();
    let mut exec = SeqExec::new(&snap, None, env).expect("open scope");
    for (i, (si, t)) in txs.iter().enumerate() {
        let status = run_one_seq_tx(
            &mut exec,
            &mut seq_delta,
            &mut bal,
            i as u64,
            t,
            signers[*si].signer.address(),
        );
        assert!(status, "tx {i} must execute sequentially");
    }
    let alloy_bal = bal.into_alloy_bal();

    let block_txs: Vec<BufferedRecord> = txs
        .iter()
        .enumerate()
        .map(|(i, (_si, t))| BufferedRecord::Tx {
            tx_idx: TxIndex(i as u64),
            position: BPosition::from_index(i as u64),
            envelope: envelope(t, signers[txs[i].0].signer.address(), i as u64),
        })
        .collect();

    for k in [20u16, 1] {
        assert_matches_at_granularity(k, &snap, &block_txs, env, &alloy_bal, &seq_delta);
    }
}
