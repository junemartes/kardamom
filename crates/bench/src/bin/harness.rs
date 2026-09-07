//! `kardamom-bench-harness` is a single-process ingress stand-in and
//! bench, with `tracing-flame` and optional `pprof` recording scoped
//! to the measurement window.
//!
//! The stand-in serves only the ingress write path, with no
//! `eth_call`. So only the `transfers` workload can run here; `calls`
//! and `mixed` need a full node, and are rejected with a clear error.
//! See `kardamom_bench::harness` for the mechanism.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use kardamom_bench::config::BenchArgs;
use kardamom_bench::harness::Harness;
use kardamom_bench::{BenchWorkflow, Benchmark, TransfersWorkflow};

#[derive(Parser, Debug)]
#[command(
    name = "kardamom-bench-harness",
    about = "Run the bench against an in-process ingress stand-in (write path only) and write a tracing-flame SVG for the measurement window only."
)]
struct Args {
    /// The path to write the tracing-flame SVG to. It is ready to open
    /// in a browser, with no `inferno-flamegraph` post-processing needed.
    #[arg(long, default_value = "flame.svg")]
    flame_out: PathBuf,

    /// The chain ID for the in-process node.
    #[arg(long, default_value_t = 412_346)]
    chain_id: u64,

    #[command(flatten)]
    bench: BenchArgs,

    /// Write the bench report as JSON to this path, in addition to stdout.
    #[arg(long)]
    report_json: Option<PathBuf>,

    /// The path to write a `pprof` CPU flame graph SVG to. This samples
    /// at 999Hz, over the measurement window only.
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
    /// Not available here: this needs `eth_call`, which the write-only
    /// ingress stand-in does not serve. Use `kardamom-bench` against a
    /// full node.
    Calls,
    /// Not available here: this interleaves calls, which the
    /// write-only ingress stand-in does not serve. Use `kardamom-bench`
    /// against a full node.
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
        timeout: args.bench.timeout,
        concurrency: args.bench.concurrency,
        txs_per_task: args.bench.txs_per_task,
        max_in_flight: args.bench.max_in_flight,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kardamom_bench::config::{
        DEFAULT_CONCURRENCY, DEFAULT_MAX_IN_FLIGHT, DEFAULT_TXS_PER_TASK,
    };

    use super::*;

    #[test]
    fn args_parse_with_only_defaults() {
        let args = Args::parse_from(["kardamom-bench-harness", "transfers"]);
        assert_eq!(args.flame_out, PathBuf::from("flame.svg"));
        assert_eq!(args.chain_id, 412_346);
        assert_eq!(args.bench.timeout, Duration::from_secs(10));
        assert_eq!(args.bench.concurrency.get(), DEFAULT_CONCURRENCY);
        assert_eq!(args.bench.txs_per_task.get(), DEFAULT_TXS_PER_TASK);
        assert_eq!(args.bench.max_in_flight, DEFAULT_MAX_IN_FLIGHT);
        assert!(args.report_json.is_none());
        assert!(args.pprof_out.is_none());
        assert!(matches!(args.workload, WorkloadCmd::Transfers));
    }

    #[test]
    fn args_parse_overrides_every_bench_flag() {
        let args = Args::parse_from([
            "kardamom-bench-harness",
            "--concurrency",
            "4",
            "--txs-per-task",
            "9",
            "--max-in-flight",
            "3",
            "--timeout",
            "5s",
            "transfers",
        ]);
        assert_eq!(args.bench.timeout, Duration::from_secs(5));
        assert_eq!(args.bench.concurrency.get(), 4);
        assert_eq!(args.bench.txs_per_task.get(), 9);
        assert_eq!(args.bench.max_in_flight, 3);
    }
}
