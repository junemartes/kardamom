//! `kardamom-bench-harness` — single-process node + bench with
//! `tracing-flame` and optional `pprof` recording scoped to the
//! measurement window.
//!
//! See `kardamom_bench::harness` for the mechanism.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};

use kardamom_bench::config::{
    DEFAULT_CONCURRENCY, DEFAULT_MAX_IN_FLIGHT, DEFAULT_TIMEOUT_STR, DEFAULT_TXS_PER_TASK,
    DEFAULT_WARMUP_STR,
};
use kardamom_bench::harness::Harness;
use kardamom_bench::{BenchWorkflow, Benchmark, CallsWorkflow, MixedWorkflow, TransfersWorkflow};

#[derive(Parser, Debug)]
#[command(
    name = "kardamom-bench-harness",
    about = "Run the bench against an in-process kardamom node and write flame.folded for the measurement window only."
)]
struct Args {
    /// Path to write `flame.folded`. Render with `inferno-flamegraph`.
    #[arg(long, default_value = "flame.folded")]
    flame_out: PathBuf,

    /// Chain ID for the in-process node.
    #[arg(long, default_value_t = 412_346)]
    chain_id: u64,

    /// Safety timeout for the measurement window; senders also stop when
    /// their work vec is drained.
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_TIMEOUT_STR)]
    timeout: Duration,

    /// Pure sleep gap between `prepare` and `dispatch`. No senders run
    /// during this window — flame and pprof recordings stay off, and the
    /// only thing that happens is the OS/CPU settling.
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_WARMUP_STR)]
    warmup: Duration,

    /// Number of sender tasks (= number of derived signers, one per task).
    #[arg(long, default_value_t = DEFAULT_CONCURRENCY)]
    concurrency: u32,

    /// Pre-signed transactions queued per sender task.
    #[arg(long = "txs-per-task", default_value_t = DEFAULT_TXS_PER_TASK)]
    txs_per_task: u32,

    /// Cap on outstanding requests per sender task.
    #[arg(long = "max-in-flight", default_value_t = DEFAULT_MAX_IN_FLIGHT)]
    max_in_flight: u32,

    /// Write bench report JSON here in addition to stdout.
    #[arg(long)]
    report_json: Option<PathBuf>,

    /// Path to write a `pprof` CPU flamegraph SVG (sampled at 999Hz over the
    /// measurement window only).
    #[arg(long)]
    pprof_out: Option<PathBuf>,

    #[command(subcommand)]
    workload: WorkloadCmd,
}

#[derive(Subcommand, Debug)]
enum WorkloadCmd {
    /// Saturate the node's write path with signed value transfers.
    Transfers,
    /// Saturate the node's read path with `eth_call` to a deterministic
    /// contract.
    Calls,
    /// Interleave transfers and calls per a 1:4 ratio.
    Mixed,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.workload {
        WorkloadCmd::Transfers => harness_with(TransfersWorkflow::default(), &args).await,
        WorkloadCmd::Calls => harness_with(CallsWorkflow::default(), &args).await,
        WorkloadCmd::Mixed => harness_with(MixedWorkflow::default(), &args).await,
    }
}

async fn harness_with<W: BenchWorkflow>(workflow: W, args: &Args) -> anyhow::Result<()> {
    let bench = Benchmark {
        workflow,
        timeout: args.timeout,
        warmup: args.warmup,
        concurrency: args.concurrency,
        txs_per_task: args.txs_per_task,
        max_in_flight: args.max_in_flight,
    };
    Harness {
        chain_id: args.chain_id,
        bench,
        flame_out: args.flame_out.clone(),
        report_json: args.report_json.clone(),
        pprof_out: args.pprof_out.clone(),
    }
    .run()
    .await
}
