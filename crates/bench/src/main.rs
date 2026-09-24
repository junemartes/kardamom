//! `kardamom-bench` is a closed-loop RPC load generator.
//!
//! Each subcommand picks one of the three built-in workflows:
//! `transfers`, `calls`, or `mixed`. Each workflow is built with
//! `Default`, using fixed, sensible values. An external user skips this
//! binary and builds a `Benchmark<MyWorkflow>` directly.

use std::path::Path;

use clap::{Parser, Subcommand};

use kardamom_bench::config::{BenchArgs, rpc_client};
use kardamom_bench::report;
use kardamom_bench::{
    BenchWorkflow, Benchmark, CallsWorkflow, MixedWorkflow, Outputs, TransfersWorkflow,
};

#[derive(Parser, Debug)]
#[command(name = "kardamom-bench", about = "Closed-loop RPC load generator.")]
struct Args {
    /// Target RPC URL.
    #[arg(long)]
    rpc: String,

    #[command(flatten)]
    bench: BenchArgs,

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
    kardamom_obs::bin::init_tracing();

    let args = Args::parse();

    let client = rpc_client(&args.rpc, args.bench.max_in_flight)?;

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
    Benchmark {
        workflow,
        timeout: args.bench.timeout,
        concurrency: args.bench.concurrency,
        txs_per_task: args.bench.txs_per_task,
        max_in_flight: args.bench.max_in_flight,
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
        txs_per_task = bench.txs_per_task.get(),
        max_in_flight = bench.max_in_flight,
        timeout = ?bench.timeout,
        concurrency = bench.concurrency.get(),
        "starting bench"
    );
    let outputs: Outputs = bench.run(client).await?;
    Ok(bench.report(outputs))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kardamom_bench::config::{
        DEFAULT_CONCURRENCY, DEFAULT_MAX_IN_FLIGHT, DEFAULT_TXS_PER_TASK,
    };

    use super::*;

    #[test]
    fn args_parse_with_only_the_required_flag() {
        let args = Args::parse_from([
            "kardamom-bench",
            "--rpc",
            "http://localhost:8545",
            "transfers",
        ]);
        assert_eq!(args.rpc, "http://localhost:8545");
        assert_eq!(args.bench.timeout, Duration::from_secs(10));
        assert_eq!(args.bench.concurrency, DEFAULT_CONCURRENCY);
        assert_eq!(args.bench.txs_per_task, DEFAULT_TXS_PER_TASK);
        assert_eq!(args.bench.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert!(args.output.is_none());
        assert!(matches!(args.workload, WorkloadCmd::Transfers));
    }

    #[test]
    fn args_parse_overrides_every_bench_flag() {
        let args = Args::parse_from([
            "kardamom-bench",
            "--rpc",
            "http://localhost:8545",
            "--timeout",
            "5s",
            "--concurrency",
            "4",
            "--txs-per-task",
            "9",
            "--max-in-flight",
            "3",
            "calls",
        ]);
        assert_eq!(args.bench.timeout, Duration::from_secs(5));
        assert_eq!(args.bench.concurrency.get(), 4);
        assert_eq!(args.bench.txs_per_task.get(), 9);
        assert_eq!(args.bench.max_in_flight, 3);
        assert!(matches!(args.workload, WorkloadCmd::Calls));
    }
}
