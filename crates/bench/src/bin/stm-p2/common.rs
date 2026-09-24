//! Shared types and helpers for the STM A/B benchmark.

use std::num::NonZeroUsize;

use kardamom_bench::stm::BlockAt;
use kardamom_engine::block_env::ExecEnv;
use kardamom_engine::delta::PendingDelta;
use kardamom_engine::exec_types::TxIndex;
use kardamom_state::{Durability, StateEnv, StateEnvBuilder, StateWriter, WriterHandle};
use kardamom_stm::execute::{Scheduler, StmOutcome};
use kardamom_types::{AccountChange, BPosition, Receipt, TxEnvelope};

/// The engine knobs. These map onto `PoolConfig`.
pub(crate) struct EngineOpts {
    pub(crate) worker_counts: Vec<NonZeroUsize>,
    pub(crate) parallel_worth_ns: u64,
    pub(crate) scheduler: Scheduler,
    pub(crate) admit_shards: Option<NonZeroUsize>,
    pub(crate) pin_cores: Vec<usize>,
    pub(crate) keep_hot: bool,
}

impl EngineOpts {
    /// The pool config for one worker count, prune batch, and tail policy.
    pub(crate) fn pool_config(
        &self,
        workers: NonZeroUsize,
        prune_batch: NonZeroUsize,
        tail_on_workers: bool,
    ) -> kardamom_stm::execute::PoolConfig {
        kardamom_stm::execute::PoolConfig {
            workers,
            prune_batch,
            parallel_worth_ns: self.parallel_worth_ns,
            scheduler: self.scheduler,
            admit_shards: self.admit_shards,
            keep_hot: self.keep_hot,
            tail_on_workers,
            pin_cores: self.pin_cores.clone(),
        }
    }
}

/// The block stream under test.
pub(crate) struct Workload<'a> {
    pub(crate) signers: &'a [kardamom_bench::signers::DerivedSigner],
    pub(crate) all_blocks: &'a [Vec<TxEnvelope>],
    pub(crate) n_setup: usize,
    pub(crate) warmup_blocks: usize,
    pub(crate) chain_id: u64,
}

impl Workload<'_> {
    /// The first timed flow block.
    pub(crate) fn warm(&self) -> usize {
        self.n_setup + self.warmup_blocks
    }

    /// The `ExecEnv` for block `bi` (0-based) in this workload.
    pub(crate) fn env_for(&self, bi: usize) -> ExecEnv {
        BlockAt(bi).env(self.chain_id)
    }

    /// The genesis account list: every signer, funded.
    pub(crate) fn genesis(&self) -> Vec<AccountChange> {
        self.signers
            .iter()
            .map(|s| AccountChange {
                address: s.signer.address(),
                nonce: 0,
                balance: alloy_primitives::U256::from(10u128.pow(21)),
                code_hash: alloy_primitives::KECCAK256_EMPTY,
            })
            .collect()
    }
}

/// What the mdbx A/B run prints and checks.
pub(crate) struct RunOpts {
    pub(crate) per_block: bool,
    pub(crate) prune_batches: Vec<NonZeroUsize>,
}

/// A tempdir-backed mdbx environment, opened and seeded with genesis.
pub(crate) struct MdbxHandles {
    /// Kept alive for the environment's lifetime; the directory is
    /// removed when this drops.
    _dir: tempfile::TempDir,
    pub(crate) env_for_reads: StateEnv,
    pub(crate) writer: WriterHandle,
}

/// Open a fresh mdbx environment in a temp directory and seed it.
pub(crate) fn open_mdbx_env(genesis: &[AccountChange]) -> anyhow::Result<MdbxHandles> {
    let dir = tempfile::tempdir()?;
    let env = StateEnvBuilder::new(dir.path())
        .durability(Durability::SafeNoSync)
        .write_map(true)
        .open()?;
    kardamom_state::seed_genesis(&env, genesis, &[])?;
    let env_for_reads = env.clone();
    let writer = StateWriter::spawn(env)?;
    Ok(MdbxHandles {
        _dir: dir,
        env_for_reads,
        writer,
    })
}

/// Open `n` independent read views, all at the block the writer has
/// most recently published.
pub(crate) fn open_views(
    env: &StateEnv,
    n: usize,
) -> anyhow::Result<Vec<kardamom_state::StateSnapshot>> {
    (0..n)
        .map(|_| kardamom_state::StateSnapshot::open(env))
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

/// The writer's initial published snapshot, right after
/// `open_mdbx_env` seeds genesis.
pub(crate) fn initial_snapshot(writer: &WriterHandle) -> kardamom_state::StateSnapshot {
    writer
        .snapshot_rx
        .current()
        .expect("writer publishes an initial snapshot")
}

/// Block until the writer publishes a snapshot at block `at_least` or
/// later, and return it.
pub(crate) fn wait_for_block(
    writer: &WriterHandle,
    at_least: u64,
) -> kardamom_state::StateSnapshot {
    std::iter::from_fn(|| writer.snapshot_rx.recv())
        .find(|snap| snap.block_number() >= at_least)
        .expect("writer alive")
}

/// A block's records, in `kardamom_stm::execute`'s tuple form: that
/// crate's block-execution functions take this exact slice type, so a
/// bench-side named record would need a collect-and-clone back to
/// tuples at every call, inside the timed window these benchmarks
/// measure.
pub(crate) type FlowRecs = Vec<(TxIndex, BPosition, TxEnvelope)>;

/// One block's records, upstream-prepared: what `run_block_prepared`
/// needs to admit them without re-decoding.
pub(crate) type FeedPayload = Vec<kardamom_stm::execute::PreparedTx>;

/// One admitted block handed from the driver thread to the settler
/// thread: its flow index (`pfi`) and its pool ticket to resolve.
pub(crate) struct SettleJob {
    pub(crate) pfi: usize,
    pub(crate) ticket: kardamom_stm::execute::BlockTicket,
}

/// One block's records, indexed by its position among `w.all_blocks`.
pub(crate) struct BlockRecs {
    pub(crate) bi: usize,
    pub(crate) recs: FlowRecs,
}

pub(crate) fn records(base_idx: u64, envs: &[TxEnvelope]) -> Vec<(TxIndex, BPosition, TxEnvelope)> {
    envs.iter()
        .enumerate()
        .map(|(i, e)| {
            let g = base_idx + i as u64;
            (TxIndex(g), BPosition::from_index(g), e.clone())
        })
        .collect()
}

/// One block's receipts and resulting delta: the sequential engine's
/// canonical output, checked byte for byte against the pool's
/// [`StmOutcome`].
pub(crate) struct BlockOutputs {
    pub(crate) receipts: Vec<Receipt>,
    pub(crate) delta: PendingDelta,
}

pub(crate) fn assert_identical(seq: &BlockOutputs, stm: &StmOutcome, block: u64, w: usize) {
    if seq.receipts != stm.receipts {
        let i = seq
            .receipts
            .iter()
            .zip(stm.receipts.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(seq.receipts.len().min(stm.receipts.len()));
        panic!("block {block} workers {w}: receipt divergence at tx {i}");
    }
    assert!(
        seq.delta.accounts == stm.delta.accounts
            && seq.delta.storage == stm.delta.storage
            && seq.delta.code == stm.delta.code,
        "block {block} workers {w}: delta divergence"
    );
}

pub(crate) use kardamom_bench::stm::StatsExt;
