//! CLI arguments for `kardamom-stm-p2`.

use std::num::NonZeroUsize;

use clap::{Parser, ValueEnum};
use kardamom_bench::signers::DerivedSigner;
use kardamom_types::TxEnvelope;

use super::common::{EngineOpts, Workload};

/// The block-generation scenario.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Scenario {
    Uniswap,
    Defi,
    Transfers,
    Partransfer,
    Parcounter,
}

impl std::fmt::Display for Scenario {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            self.to_possible_value()
                .expect("every Scenario variant has a value name")
                .get_name(),
        )
    }
}

/// The state backend a run executes against.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StateBackend {
    /// In-memory, the harshest baseline, where a read costs nothing.
    Mock,
    /// The real backend, where a read does cost something.
    Mdbx,
}

#[derive(Parser, Debug)]
#[command(name = "kardamom-stm-p2")]
#[allow(
    clippy::struct_excessive_bools,
    reason = "each bool is an independent CLI flag; grouping them would rename flags"
)]
pub(crate) struct Args {
    #[arg(long, value_enum, default_value_t = Scenario::Uniswap)]
    pub(crate) scenario: Scenario,
    /// Non-zero: an empty pair set leaves every flow op with nothing
    /// to divide by (see `stm::uniswap::UniswapParams::pairs`).
    #[arg(long, default_value_t = NonZeroUsize::new(8).expect("8 != 0"))]
    pub(crate) pairs: NonZeroUsize,
    /// Non-zero: `stm::workload::defi_blocks` divides the flow
    /// transaction budget by this count.
    #[arg(long, default_value_t = NonZeroUsize::new(96).expect("96 != 0"))]
    pub(crate) senders: NonZeroUsize,
    #[arg(long, default_value_t = 16)]
    pub(crate) blocks: usize,
    #[arg(long, default_value_t = 500)]
    pub(crate) block_size: usize,
    #[arg(long, default_value_t = 70)]
    pub(crate) swap_share: u64,
    #[arg(long, default_value_t = 10)]
    pub(crate) cross: u64,
    /// The worker counts to sweep, comma-separated.
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8,12")]
    pub(crate) workers: Vec<usize>,
    #[arg(long, default_value = ".")]
    pub(crate) repo_root: String,
    #[arg(long, default_value_t = 412346)]
    pub(crate) chain_id: u64,
    /// Print a line for each block.
    #[arg(long, default_value_t = false)]
    pub(crate) per_block: bool,
    /// Write an on-CPU flame graph, through pprof, to this path. This
    /// settles which part of the read path costs what, instead of guessing.
    #[arg(long)]
    pub(crate) pprof_out: Option<String>,
    #[arg(long, value_enum, default_value_t = StateBackend::Mock)]
    pub(crate) state: StateBackend,
    /// The DAG prune batch sizes to sweep. This is the number of
    /// completions applied for each graph-lock acquisition; 1 means
    /// update on every completion.
    #[arg(long, value_delimiter = ',', default_value = "1")]
    pub(crate) prune_batch: Vec<usize>,
    /// The mean per-transaction nanoseconds below which the pool
    /// declines a block and runs it in sequence. `0` forces parallel
    /// execution regardless. Use `0` when measuring scaling, since the
    /// default policy would route a cheap workload to the sequential
    /// path and hide the numbers under test.
    #[arg(long)]
    pub(crate) parallel_worth_ns: Option<u64>,
    /// Dispatch on the sender, instead of the first non-sender cell.
    #[arg(long, default_value_t = false)]
    pub(crate) dispatch_by_sender: bool,
    /// The eager chain FIFO: enqueue at admission when every unfinished
    /// predecessor is already in the same worker's queue. Disable this
    /// for the A/B baseline.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) eager_chain: bool,
    /// Use sticky least-loaded domain assignment, instead of pure hashing.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub(crate) sticky_assign: bool,
    /// Pin worker i to core list[i % len], for example "2,3,4,5". An
    /// empty value leaves it to the OS.
    #[arg(long, value_delimiter = ',')]
    pub(crate) pin_cores: Vec<usize>,
    /// parcounter: how many sload, add, and sstore rounds each call
    /// performs. `1` is the 10-byte micro counter, about 4 microseconds
    /// per transaction, a stress test of fixed costs. About 25
    /// approximates real contract weight, about 10 microseconds per
    /// transaction, the honest substrate for scaling questions.
    /// Non-zero: 0 loop iterations would empty the runtime bytecode's
    /// jump target (see `scenario::parcounter_blocks`).
    #[arg(long, default_value_t = NonZeroUsize::new(1).expect("1 != 0"))]
    pub(crate) call_work: NonZeroUsize,
    /// The flow blocks excluded from timing while the footprint stats
    /// warm up. The first flow block runs entirely cold, with nothing
    /// trained yet: every transaction is a barrier, the DAG degenerates
    /// to a serial chain with O(n^2) edge fan-in, and the average over
    /// several blocks mostly measures that one cold block. Production
    /// stats are continuously warm, so steady state is the honest number.
    #[arg(long, default_value_t = 1)]
    pub(crate) warmup_blocks: usize,
    /// Engine keep-hot: workers spin and yield between blocks. This
    /// holds core frequency, and replaces external `SCHED_IDLE` spinners.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub(crate) keep_hot: bool,
    /// The pipelined measurement: two independent databases. Pass A
    /// runs sequentially, one block at a time, timed. Pass B streams
    /// blocks through submit-ahead, at depth 2, with lag-1
    /// byte-identical checks and production-shaped settlement. Reports
    /// aggregate throughput.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub(crate) pipeline: bool,
    /// With --pipeline, layer block N+1 on the engine's own deltas,
    /// released speculatively at block N's fold. This is
    /// the production shape. The default keeps the baseline-delta
    /// layering, which measures pipeline mechanics only. A scenario
    /// must be wound-free: a corrected release fails the run loudly.
    #[arg(long, default_value_t = false, action = clap::ArgAction::Set)]
    pub(crate) pipeline_speculative: bool,
    /// The bag scheduler, the default: one shared lock-free runnable
    /// set, inline completion, and chain-local hand-off. `false` runs
    /// the per-worker FIFO scheduler instead, with stealing and eager
    /// coverage, for an A/B comparison.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub(crate) bag_scheduler: bool,
    /// Sharded admission: cell-space shards for dependency discovery.
    /// `0` means a serial feed. Lanes run on the caller cores.
    #[arg(long, default_value_t = 0)]
    pub(crate) admit_shards: usize,
}

impl Args {
    /// The pool config knobs, from the CLI flags.
    pub(crate) fn engine_opts(&self) -> EngineOpts {
        EngineOpts {
            worker_counts: self.workers.clone(),
            parallel_worth_ns: self
                .parallel_worth_ns
                .unwrap_or(kardamom_stm::execute::PARALLEL_WORTH_NS),
            dispatch_by_sender: self.dispatch_by_sender,
            eager_chain: self.eager_chain,
            bag_scheduler: self.bag_scheduler,
            admit_shards: self.admit_shards,
            sticky_assign: self.sticky_assign,
            pin_cores: self.pin_cores.clone(),
            keep_hot: self.keep_hot,
        }
    }

    /// The block-stream view over already-built blocks.
    pub(crate) fn workload<'a>(
        &self,
        signers: &'a [DerivedSigner],
        all_blocks: &'a [Vec<TxEnvelope>],
        n_setup: usize,
    ) -> Workload<'a> {
        Workload {
            signers,
            all_blocks,
            n_setup,
            warmup_blocks: self.warmup_blocks,
            chain_id: self.chain_id,
        }
    }
}
