//! `kardamom-bench-harness` — single-process ingress stand-in + bench with
//! `tracing-flame` and optional `pprof` recording scoped to the
//! measurement window.
//!
//! The stand-in serves only the ingress write path (no `eth_call`), so only
//! the `transfers` workload can run here; `calls`/`mixed` need a full node
//! and are rejected with a clear error. See `kardamom_bench::harness` for
//! the mechanism.

use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};

use kardamom_bench::config::{
    DEFAULT_CONCURRENCY, DEFAULT_MAX_IN_FLIGHT, DEFAULT_TIMEOUT_STR, DEFAULT_TXS_PER_TASK,
};
use kardamom_bench::harness::Harness;
use kardamom_bench::{BenchWorkflow, Benchmark, TransfersWorkflow};

#[derive(Parser, Debug)]
#[command(
    name = "kardamom-bench-harness",
    about = "Run the bench against an in-process ingress stand-in (write path only) and write a tracing-flame SVG for the measurement window only."
)]
struct Args {
    /// Path to write the tracing-flame SVG. Ready to open in a browser
    /// — no `inferno-flamegraph` post-processing needed.
    #[arg(long, default_value = "flame.svg")]
    flame_out: PathBuf,

    /// Chain ID for the in-process node.
    #[arg(long, default_value_t = 412_346)]
    chain_id: u64,

    /// Safety timeout applied to each phase (warmup and dispatch get one
    /// each); senders also stop when their work vec is drained.
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_TIMEOUT_STR)]
    timeout: Duration,

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
    /// Saturate the ingress stand-in's write path with signed value
    /// transfers.
    Transfers,
    /// Unavailable here: needs `eth_call`, which the write-only ingress
    /// stand-in does not serve. Use `kardamom-bench` against a full node.
    Calls,
    /// Unavailable here: interleaves calls, which the write-only ingress
    /// stand-in does not serve. Use `kardamom-bench` against a full node.
    Mixed,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.workload {
        WorkloadCmd::Transfers => harness_with(TransfersWorkflow::default(), &args).await,
        WorkloadCmd::Calls | WorkloadCmd::Mixed => anyhow::bail!(
            "the in-process ingress stand-in serves only the write path (no eth_call), \
             so `calls`/`mixed` cannot run here — use `transfers`, or run \
             `kardamom-bench` against a node that serves reads"
        ),
    }
}

async fn harness_with<W: BenchWorkflow>(workflow: W, args: &Args) -> anyhow::Result<()> {
    let bench = Benchmark {
        workflow,
        timeout: args.timeout,
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
