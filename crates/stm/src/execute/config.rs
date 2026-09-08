/// How long `seal` waits for a block to drain before it declares a
/// scheduler bug. Generous by orders of magnitude: a 30M-gas block takes
/// milliseconds to execute, so anything past this is a stranded edge,
/// not slow work.
pub(super) const STALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Mean per-transaction execution time below which the pool declines a
/// block and runs it sequentially.
///
/// Parallel execution buys down only the execution span. It cannot buy
/// down the serial feed (about 0.4us/tx of admission) or the commit
/// tail, and it adds cross-core traffic to every read and publish. Below
/// some amount of work per transaction, those fixed costs exceed
/// anything more cores can return, and the honest choice is not to
/// compete.
///
/// This is a floor, not a verdict on transfers: the costs it defends
/// against (the single-threaded feed and the serial delta fold) are
/// implementation limits. If they come down, this constant should come
/// down with them.
pub const PARALLEL_WORTH_NS: u64 = 2_500;

/// Sticky-assignment map bound; beyond it new domains hash as before.
pub(super) const STICKY_CAP: usize = 65_536;

/// Spins before a dry worker parks. Sized so the spin costs far less than
/// the park/unpark syscall pair it avoids, while still yielding promptly
/// when a block really has drained. Do not run more workers than cores
/// minus the feed thread; that is the real fix for oversubscription.
pub(super) const SPIN_BEFORE_PARK: u32 = 256;

/// Mean per-transaction execution time above which moving a ready
/// transaction to an idle core beats keeping its state warm on the
/// owning one. Set between a 21k-gas transfer (about 2.75us, where
/// migration loses) and a uniswap swap (about 15us, where migration
/// wins).
pub(super) const STEAL_WORTH_NS: u64 = 6_000;

/// Longest a parked worker sleeps before re-checking its queue and the
/// drain condition itself. Bounds the damage of a missed wake to one
/// poll interval instead of a permanent hang.
pub(super) const PARK_POLL: std::time::Duration = std::time::Duration::from_micros(200);

/// Transactions per sharded-admission batch (see `flush_admit_batch`).
pub(super) const ADMIT_BATCH: usize = 512;

/// Gas-limit-derived hard cap on transactions per block:
/// `BLOCK_GAS_LIMIT` / 21k intrinsic gas = 1,428, with about 2.8x
/// headroom. Slots are pre-allocated per block so workers address them
/// lock-free while the feed is still admitting.
pub(super) const MAX_BLOCK_TXS: usize = 4_096;

/// FIFO-scheduler-only knobs. Meaningless under [`Scheduler::Bag`] (the
/// bag has no per-worker queues to dispatch on, assign to, or steal
/// between), so they live inside [`Scheduler::Fifo`] instead of beside
/// it: a bag config cannot carry a stale FIFO setting by construction.
#[derive(Debug, Clone, Copy)]
pub struct FifoOptions {
    /// Dispatch on the sender rather than the first non-sender cell.
    ///
    /// A transfer writes two accounts, and dispatch can only own one of
    /// them, so this chooses which side is foreign. Measured at 4
    /// workers on transfers, the default (recipient) yields 62.2%
    /// own-domain writes, matching `50% + 1/workers x 50%` exactly. This
    /// is pure scheduling policy: the DAG still takes edges on every
    /// cell either way, so it cannot change results, only locality.
    pub dispatch_by_sender: bool,
    /// Enqueue a transaction at admission when every unfinished
    /// predecessor is already released to the same worker's FIFO. Queue
    /// order then enforces the chain, and the edge and prune hand-off is
    /// skipped entirely. Per-link hand-off through batched pruning is
    /// the prime suspect for the span floor (chains release one
    /// transaction per prune).
    pub eager_chain: bool,
    /// Assign each new domain to the least-loaded worker and remember
    /// the choice for the pool's lifetime, instead of hashing.
    ///
    /// Hashing is stable but collision-blind: 4 hot pairs over 4 workers
    /// land on 4 distinct threads only about 28% of the time, and a
    /// collision puts two serial chains on one core, measured as one
    /// worker carrying half the block in a 4-pair scenario.
    /// Round-robin-on-first-sight was tested and reverted, because its
    /// assignment reshuffled every block. This keeps the cross-block
    /// stickiness that made hashing win, and fixes only the collisions.
    pub sticky_assign: bool,
}

impl Default for FifoOptions {
    fn default() -> Self {
        Self {
            dispatch_by_sender: false,
            eager_chain: true,
            sticky_assign: false,
        }
    }
}

/// Which per-worker scheduler runs a block.
#[derive(Debug, Clone, Copy, Default)]
pub enum Scheduler {
    /// Every runnable transaction goes into one shared lock-free bag,
    /// popped by whichever worker is free. Completion is inline (the
    /// finishing worker closes its node and dispatches children, with no
    /// prune batching) with chain-local hand-off (the first ready child
    /// stays on the completing worker, so chains stream on one core with
    /// zero queue operations). No per-worker queues, no stealing, no
    /// eager coverage (every dependency is an edge). Measured at or
    /// above the FIFO scheduler on every workload tested.
    #[default]
    Bag,
    /// The per-worker FIFO scheduler, with its own dispatch, chaining,
    /// and assignment knobs.
    Fifo(FifoOptions),
}

impl Scheduler {
    pub(super) fn is_bag(self) -> bool {
        matches!(self, Self::Bag)
    }

    /// This scheduler's FIFO knobs, or the defaults under `Bag` (where
    /// they have no effect, since the bag has no per-worker queues).
    pub(super) fn fifo_options(self) -> FifoOptions {
        match self {
            Self::Bag => FifoOptions::default(),
            Self::Fifo(opts) => opts,
        }
    }
}

/// Pool configuration.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub workers: std::num::NonZeroUsize,
    /// Apply completions to the DAG in batches of this many. `1` updates
    /// the graph on every completion (the immediate policy). Larger
    /// values trade graph-lock traffic for dispatch latency. A worker
    /// that runs dry always force-prunes first, so batching can never
    /// starve the pool, only delay a handoff.
    pub prune_batch: std::num::NonZeroUsize,
    /// Mean per-transaction execution time below which the pool declines
    /// a block and runs it sequentially. See `PARALLEL_WORTH_NS` for the
    /// measured default. Injectable so the policy is testable without
    /// depending on how loaded the machine is, and tunable per
    /// deployment.
    pub parallel_worth_ns: u64,
    /// Which per-worker scheduler runs a block, and its knobs.
    pub scheduler: Scheduler,
    /// Sharded admission: the number of cell-space shards the feed's
    /// dependency discovery is split across. `None` means the serial
    /// feed. Cell `c` belongs to shard `h(c) % K`, so every real
    /// conflict is owned by exactly one shard and no shard writes
    /// another's table. Discovery is batched: the batch boundary is the
    /// synchronization point, so a transaction cannot dispatch until
    /// every shard has registered its edges, and no per-shard guards
    /// are needed. Shard lanes live on the caller cores; the worker
    /// cores stay dedicated to execution.
    pub admit_shards: Option<std::num::NonZeroUsize>,
    /// Between blocks, workers spin-yield instead of sleeping on the
    /// condvar. schedutil drops a core to base clock the moment it
    /// idles, and burst-park execution never ramps it back up. A
    /// yielding spinner holds the governor's utilization signal up
    /// while surrendering the core within microseconds to any real
    /// work, including the commit tail's scoped threads, which pin to
    /// these same cores. This costs idle watts; production executor
    /// pools run continuously busy, so this setting mainly serves
    /// dedicated-core deployments and honest benchmarking.
    pub keep_hot: bool,
    /// Run the commit tail's parallel phases on the worker cores. Right
    /// for block-at-a-time, where workers park during the tail and
    /// their cores are hot and instantly yielded. Wrong for the
    /// pipeline, where the next block executes on those cores while
    /// this block's tail runs; the phases then stay on the caller's
    /// mask.
    pub tail_on_workers: bool,
    /// Pin worker i to `pin_cores[i % len]`.
    ///
    /// Measured reason: on a machine with two CPU core clusters sharing
    /// a split L3 cache, a worker sharing its cluster with the mdbx
    /// writer ran the same block roughly twice as slow as it ran
    /// isolated. The writer's page churn evicts the interpreter's
    /// working set from the shared cache, a memory-level tax that no
    /// code-level timer can see. An empty list lets the scheduler place
    /// workers, which settles them on the writer's cluster often enough
    /// to produce a floating per-block performance step.
    pub pin_cores: Vec<usize>,
}

/// Batching 8 completions per graph-lock acquisition cuts prune time
/// with no wall-clock cost. Worth taking, but not the main lever: the
/// binding constraint is the serial feed, which is why `prune_batch` is
/// a tuning knob, not a fix.
pub(crate) const DEFAULT_PRUNE_BATCH: std::num::NonZeroUsize =
    std::num::NonZeroUsize::new(8).expect("8 != 0");

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            workers: std::num::NonZeroUsize::new(1).expect("1 != 0"),
            prune_batch: DEFAULT_PRUNE_BATCH,
            parallel_worth_ns: PARALLEL_WORTH_NS,
            scheduler: Scheduler::default(),
            admit_shards: None,
            keep_hot: false,
            tail_on_workers: true,
            pin_cores: Vec::new(),
        }
    }
}

/// A code hash normalized against revm's empty-code marker: the delta
/// stores an all-zero hash for an account with no code, but revm
/// expects `KECCAK_EMPTY` there.
pub(super) struct CodeHash(alloy_primitives::B256);

impl CodeHash {
    pub(super) fn normalize(raw: alloy_primitives::B256) -> Self {
        if raw == alloy_primitives::B256::ZERO {
            Self(revm::primitives::KECCAK_EMPTY)
        } else {
            Self(raw)
        }
    }

    pub(super) fn get(self) -> alloy_primitives::B256 {
        self.0
    }
}

/// Build an `AccountInfo` from the three fields every read stack layer
/// stores, normalizing the code hash (see [`CodeHash`]) once. Every
/// read source (mv layers, delta layers, the base cache, and the
/// backend read stack) builds this same 5-field record.
pub(super) fn account_info(
    nonce: u64,
    balance: alloy_primitives::U256,
    code_hash: alloy_primitives::B256,
) -> revm::state::AccountInfo {
    revm::state::AccountInfo {
        nonce,
        balance,
        code_hash: CodeHash::normalize(code_hash).get(),
        account_id: None,
        code: None,
    }
}

/// A duration's nanoseconds as `u64`, for the pool's timing counters.
/// Saturates instead of wrapping: a duration over `u64::MAX` nanoseconds
/// (about 584 years) cannot happen on a real timer, so this only ever
/// widens in practice.
pub(super) fn nanos(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// The block-global EIP-7928 BAL fragment index for local index `i`:
/// `bal_base + i + 1` (`bal_base` is a caller-supplied count of
/// canonical records before this run, not bounded by this crate).
/// Shared by the worker's streaming capture and the tail's repair
/// path, which differ only in whether `i` starts as a `u32` or a
/// `usize`.
///
/// # Errors
/// Returns an error naming the block, `bal_base`, and `i`, if the sum
/// overflows `u64`.
pub(super) fn bal_index(
    bal_base: u64,
    i: u64,
    block_number: u64,
) -> Result<u64, kardamom_exec_core::error::ExecutorError> {
    bal_base
        .checked_add(i)
        .and_then(|v| v.checked_add(1))
        .ok_or_else(|| {
            kardamom_exec_core::error::ExecutorError::State(format!(
                "stm: block {block_number} BAL index overflowed u64 (bal_base={bal_base}, i={i})"
            ))
        })
}
