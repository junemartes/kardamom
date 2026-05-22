//! `kardamom-bench` — closed-loop RPC load generator.
//!
//! Each subcommand picks one of the three built-in workflows
//! (`transfers`, `calls`, `mixed`), each constructed via `Default` with
//! hardcoded sensible values. External users skip this binary and build a
//! `Benchmark<MyWorkflow>` directly.

use std::path::Path;
use std::time::Duration;

use clap::{Parser, Subcommand};
use jsonrpsee::http_client::HttpClientBuilder;
use tracing_subscriber::EnvFilter;

use kardamom_bench::config::{
    DEFAULT_CONCURRENCY, DEFAULT_MAX_IN_FLIGHT, DEFAULT_TIMEOUT_STR, DEFAULT_TXS_PER_TASK,
    MAX_IN_FLIGHT_SLACK, REQUEST_TIMEOUT,
};
use kardamom_bench::report::{self, ReportInputs};
use kardamom_bench::{
    BenchWorkflow, Benchmark, CallsWorkflow, MixedWorkflow, Outputs, TransfersWorkflow,
};

#[derive(Parser, Debug)]
#[command(name = "kardamom-bench", about = "Closed-loop RPC load generator.")]
struct Args {
    /// Target RPC URL.
    #[arg(long)]
    rpc: String,

    /// Safety timeout applied to each phase (warmup and dispatch get one
    /// each). Senders also stop when their work vec is drained, whichever
    /// comes first.
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

    /// Write the report as JSON to this path in addition to printing it.
    #[arg(long)]
    output: Option<String>,

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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    let client = HttpClientBuilder::default()
        .request_timeout(REQUEST_TIMEOUT)
        .max_concurrent_requests(args.max_in_flight as usize + MAX_IN_FLIGHT_SLACK)
        .build(&args.rpc)?;

    let report = match &args.workload {
        WorkloadCmd::Transfers => {
            let bench = bench_with(TransfersWorkflow::default(), &args);
            run_one(client, bench, &args).await?
        }
        WorkloadCmd::Calls => {
            let bench = bench_with(CallsWorkflow::default(), &args);
            run_one(client, bench, &args).await?
        }
        WorkloadCmd::Mixed => {
            let bench = bench_with(MixedWorkflow::default(), &args);
            run_one(client, bench, &args).await?
        }
    };

    report::print_terminal(&report);
    if let Some(path) = &args.output {
        report::write_json(Path::new(path), &report)?;
        eprintln!("wrote report to {path}");
    }
    Ok(())
}

fn bench_with<W: BenchWorkflow>(workflow: W, args: &Args) -> Benchmark<W> {
    // Plain field copy — kept as a regular fn rather than `const` because
    // `Benchmark::workflow` is generic and may carry non-`const` data.
    Benchmark {
        workflow,
        timeout: args.timeout,
        concurrency: args.concurrency,
        txs_per_task: args.txs_per_task,
        max_in_flight: args.max_in_flight,
    }
}

async fn run_one<W: BenchWorkflow>(
    client: jsonrpsee::http_client::HttpClient,
    bench: Benchmark<W>,
    args: &Args,
) -> anyhow::Result<report::BenchReport> {
    tracing::info!(
        rpc = %args.rpc,
        workload = bench.workflow.name(),
        txs_per_task = bench.txs_per_task,
        max_in_flight = bench.max_in_flight,
        timeout = ?bench.timeout,
        concurrency = bench.concurrency,
        "starting bench"
    );
    let outputs: Outputs = bench.run(client).await?;
    Ok(report::build_report(
        ReportInputs {
            workload_name: bench.workflow.name(),
            txs_per_task: bench.txs_per_task,
            max_in_flight: bench.max_in_flight,
            concurrency: bench.concurrency,
            configured_timeout: bench.timeout,
        },
        &outputs.counters,
        outputs.histograms,
        outputs.measurement_duration,
    ))
}
