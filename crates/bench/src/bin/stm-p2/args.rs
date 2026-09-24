//! CLI arguments for `kardamom-stm-p2`.

use std::num::NonZeroUsize;

use clap::{Parser, ValueEnum};
use kardamom_bench::signers::DerivedSigner;
use kardamom_stm::execute::{FifoOptions, Scheduler};
use kardamom_types::TxEnvelope;

use super::common::{EngineOpts, Workload};

/// Default `--pairs`.
const DEFAULT_PAIRS: NonZeroUsize = NonZeroUsize::new(8).unwrap();
/// Default `--senders`.
const DEFAULT_SENDERS: NonZeroUsize = NonZeroUsize::new(96).unwrap();
/// Default `--call-work`.
const DEFAULT_CALL_WORK: NonZeroUsize = NonZeroUsize::new(1).unwrap();

/// The `--scheduler` flag's values. `Bag` collapses onto
/// [`Scheduler::Bag`]; `Fifo` collapses onto [`Scheduler::Fifo`] with
/// its default knobs. This replaces the four fine-grained scheduler
/// flags (`--dispatch-by-sender`, `--eager-chain`, `--sticky-assign`,
/// `--bag-scheduler`) with the choice production makes: bag or FIFO.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(crate) enum SchedulerArg {
    #[default]
    Bag,
    Fifo,
}

impl From<SchedulerArg> for Scheduler {
    fn from(a: SchedulerArg) -> Self {
        match a {
            SchedulerArg::Bag => Scheduler::Bag,
            SchedulerArg::Fifo => Scheduler::Fifo(FifoOptions::default()),
        }
    }
}

/// The block-generation scenario.
#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Scenario {
    Uniswap,
    Defi,
    Transfers,
    Partransfer,
    Parcounter,
}

impl Scenario {
    /// This variant's `--scenario` value name. One source for both the
    /// `ValueEnum` derive (parsing) and `Display` (printing), so the
    /// two can never drift apart.
    const fn name(self) -> &'static str {
        match self {
            Self::Uniswap => "uniswap",
            Self::Defi => "defi",
            Self::Transfers => "transfers",
            Self::Partransfer => "partransfer",
            Self::Parcounter => "parcounter",
        }
    }
}

impl std::fmt::Display for Scenario {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
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
    #[arg(long, default_value_t = DEFAULT_PAIRS)]
    pub(crate) pairs: NonZeroUsize,
    /// Non-zero: `stm::workload::defi_blocks` divides the flow
    /// transaction budget by this count.
    #[arg(long, default_value_t = DEFAULT_SENDERS)]
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
    pub(crate) workers: Vec<NonZeroUsize>,
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
    pub(crate) prune_batch: Vec<NonZeroUsize>,
    /// The mean per-transaction nanoseconds below which the pool
    /// declines a block and runs it in sequence. `0` forces parallel
    /// execution regardless. Use `0` when measuring scaling, since the
    /// default policy would route a cheap workload to the sequential
    /// path and hide the numbers under test.
    #[arg(long)]
    pub(crate) parallel_worth_ns: Option<u64>,
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
    #[arg(long, default_value_t = DEFAULT_CALL_WORK)]
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
    /// The per-worker scheduler: `bag`, the default (one shared
    /// lock-free runnable set, inline completion, chain-local
    /// hand-off), or `fifo` (per-worker queues, stealing, eager
    /// coverage), for an A/B comparison.
    #[arg(long, value_enum, default_value_t = SchedulerArg::Bag)]
    pub(crate) scheduler: SchedulerArg,
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
            scheduler: self.scheduler.into(),
            admit_shards: NonZeroUsize::new(self.admit_shards),
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

#[cfg(test)]
mod tests {
    use super::Scenario;
    use clap::ValueEnum;

    /// `Scenario::name` feeds both the `ValueEnum` parser and the
    /// `Display` impl. Both must agree, or `--scenario <value>` and the
    /// printed scenario name would drift apart.
    #[test]
    fn scenario_name_matches_the_value_enum_parse_name() {
        for variant in Scenario::value_variants() {
            let parsed_name = variant
                .to_possible_value()
                .expect("every Scenario variant has a possible value")
                .get_name()
                .to_string();
            assert_eq!(
                variant.name(),
                parsed_name,
                "Scenario::name() must match the ValueEnum parse name for {variant:?}"
            );
        }
    }
}
