//! Single-process kardamom bench harness with `tracing-flame` recording
//! and optional `pprof`-based CPU sampling, both scoped to the dispatch
//! window. Generic over any `BenchWorkflow`.
//!
//! Running the standalone node + bench together for a long session
//! pushes the resulting flamegraph 99%+ into the bare
//! `ThreadId(N)-tokio-rt-worker` root frame — every nanosecond a worker
//! is alive but not inside an entered span is attributed there. Embedding
//! the node in the bench process and only flushing flame samples during
//! the measurement window keeps the recording tight around the work we
//! actually care about.
//!
//! `pprof` samples actual on-CPU time via SIGPROF (everything tokio,
//! jsonrpsee, revm, k256 actually do — not just our tracing spans). Its
//! `ProfilerGuard` is built right between warmup and dispatch and dropped
//! right after dispatch returns, so the SVG is bounded to the same window
//! as the `tracing-flame` recording.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::server::Server;
use tracing_flame::FlameLayer;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use kardamom_node::rpc::{EthApiServer, EthHandlers};
use kardamom_node::{Genesis, Node};

use crate::Benchmark;
use crate::config::{MAX_IN_FLIGHT_SLACK, PPROF_HZ, REQUEST_TIMEOUT};
use crate::report::{self, ReportInputs};
use crate::workflow::BenchWorkflow;

/// In-process node + RPC server + benchmark dispatcher, with the
/// flame/pprof recordings scoped to the dispatch window.
pub struct Harness<W: BenchWorkflow> {
    /// Chain ID used by the in-process [`kardamom_node::Node`].
    pub chain_id: u64,
    /// The benchmark dispatcher this harness drives.
    pub bench: Benchmark<W>,
    /// Path to write the `tracing-flame` folded output. Post-processed
    /// to drop per-worker thread prefixes before being written.
    pub flame_out: PathBuf,
    /// If set, write the [`BenchReport`](crate::report::BenchReport) as
    /// pretty JSON to this path in addition to printing to stdout.
    pub report_json: Option<PathBuf>,
    /// If set, record a `pprof` CPU sample over the dispatch window and
    /// write an inferno-flamegraph SVG to this path.
    pub pprof_out: Option<PathBuf>,
}

/// Build the node + RPC server and drive a measured dispatch with
/// flame/pprof recording scoped to the dispatch window.
///
/// Flow: prepare the workflow against a live client, sleep out the
/// warmup gap, then flip the flame/pprof gates on and run the dispatch.
/// The recordings see only the dispatch window — no signer derivation,
/// no presigning, no warmup.
///
/// # Errors
///
/// Errors if the genesis derived from `workflow.genesis_alloc` is
/// invalid, the in-process RPC server can't bind, the workflow's
/// `prepare`/`dispatch` fails, or any of the output files (flame, pprof,
/// report JSON) can't be written.
///
/// # Panics
///
/// Panics if the hardcoded loopback bind literal `"127.0.0.1:0"` fails
/// to parse (impossible). Indirectly, panics if a sender task panics
/// mid-iteration (poisons the per-task accumulator mutex — see
/// `Benchmark::dispatch`).
// Five linear sections, each well-commented: (1) tracing init,
// (2) node + server + client, (3) prepare + warmup, (4) measured
// dispatch under flame/pprof gates, (5) post-dispatch rendering +
// report. Decomposing into helpers would force them to thread eight
// borrowed values each, which is more noise than the line count saves.
#[allow(clippy::too_many_lines)]
pub async fn run_harness<W: BenchWorkflow>(args: Harness<W>) -> anyhow::Result<()> {
    let active = Arc::new(AtomicBool::new(false));
    let active_for_filter = Arc::clone(&active);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("kardamom=info,kardamom_bench=info"));
    let fmt_layer = fmt::layer();

    let (flame, flame_guard) = FlameLayer::with_file(&args.flame_out)?;
    let gated_flame = flame.with_filter(FilterFn::new(move |_meta| {
        active_for_filter.load(Ordering::Relaxed)
    }));

    // `try_init` fails if a global subscriber was already installed (e.g.
    // by an enclosing binary or test harness). That's not fatal — the flame
    // layer just won't receive events. Warn and continue rather than abort.
    if let Err(e) = tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(gated_flame)
        .try_init()
    {
        eprintln!(
            "kardamom-bench harness: tracing subscriber already installed; \
             flame layer will not capture events ({e})"
        );
    }

    // Build genesis from the workflow's alloc declaration, then validate
    // it through the node's own validator (catches duplicate addresses,
    // chain_id == 0, etc.).
    let alloc = args.bench.workflow.genesis_alloc(args.bench.concurrency)?;
    let genesis = Genesis {
        chain_id: args.chain_id,
        alloc,
    };
    genesis.validate().map_err(|e| {
        anyhow::anyhow!(
            "invalid Genesis (chain_id={}, allocs from workflow.genesis_alloc): {e}",
            args.chain_id
        )
    })?;
    let node = Node::new(&genesis);

    // Hard-coded loopback bind: parse cannot fail on this literal.
    let bind: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("hardcoded loopback `127.0.0.1:0` is a valid SocketAddr");
    let server = Server::builder().build(bind).await?;
    let bound = server.local_addr()?;

    let module = EthHandlers::new(node).into_rpc();
    let server_handle = server.start(module);

    let url = format!("http://{bound}");
    let client = HttpClientBuilder::default()
        .request_timeout(REQUEST_TIMEOUT)
        .max_concurrent_requests(args.bench.max_in_flight as usize + MAX_IN_FLIGHT_SLACK)
        .build(&url)?;

    // All client-side crypto (signer derivation, presigning) happens here,
    // before the dispatcher starts and well before the flame/pprof gates
    // flip on.
    let prepared = args.bench.prepare(&client).await?;

    // Warmup is a pure sleep gap before dispatch — no senders are running
    // during it. After the sleep we flip the flame gate and build the
    // pprof guard, then call dispatch, then drop the guard. The recordings
    // see only the dispatch window.
    if !args.bench.warmup.is_zero() {
        tracing::info!(duration = ?args.bench.warmup, "harness: warmup sleep");
        tokio::time::sleep(args.bench.warmup).await;
    }
    let pprof_guard = if args.pprof_out.is_some() {
        Some(
            pprof::ProfilerGuardBuilder::default()
                .frequency(PPROF_HZ)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .map_err(|e| anyhow::anyhow!("pprof guard build failed: {e}"))?,
        )
    } else {
        None
    };

    tracing::info!(
        workload = args.bench.workflow.name(),
        duration = ?args.bench.duration,
        concurrency = args.bench.concurrency,
        max_in_flight = args.bench.max_in_flight,
        txs_per_task = args.bench.txs_per_task,
        flame_out = %args.flame_out.display(),
        pprof_out = ?args.pprof_out.as_ref().map(|p| p.display().to_string()),
        "harness: dispatch (flame/pprof on)"
    );

    let outputs = {
        let _scope = ActiveScope::enter(&active);
        args.bench.dispatch(client, prepared).await?
    };

    flame_guard
        .flush()
        .map_err(|e| anyhow::anyhow!("flush flamegraph: {e}"))?;
    // Drop the guard before rewriting the file so no further writes race
    // with the merge step below.
    drop(flame_guard);
    // Rewrite the tracing-flame folded file: drop the per-worker thread
    // prefix and bucket by remaining stack. Same `merge_folded_text` we
    // apply to pprof below, just sourced from a file instead of a Report.
    let raw_flame = std::fs::read_to_string(&args.flame_out)
        .map_err(|e| anyhow::anyhow!("read {}: {e}", args.flame_out.display()))?;
    let merged_flame = merge_folded_text(&raw_flame);
    std::fs::write(&args.flame_out, &merged_flame)
        .map_err(|e| anyhow::anyhow!("write {}: {e}", args.flame_out.display()))?;
    tracing::info!(
        flame_out = %args.flame_out.display(),
        raw_bytes = raw_flame.len(),
        merged_bytes = merged_flame.len(),
        "harness: merged flame.folded across tokio workers"
    );

    if let (Some(guard), Some(path)) = (pprof_guard, args.pprof_out.as_ref()) {
        let report = guard
            .report()
            .build()
            .map_err(|e| anyhow::anyhow!("pprof report build failed: {e}"))?;
        // 1. Keep only stacks that contain at least one `kardamom_node::*`
        //    frame. The harness runs the node and the bench client on the
        //    same tokio runtime, so the raw report mixes node and client
        //    work; the user wants the SVG to show node work only.
        let filtered = filter_to_node(&report);
        // 2. Convert the filtered Report to folded text, then run the same
        //    `merge_folded_text` we use for tracing-flame above to drop the
        //    thread-name prefix and sum identical stacks across workers.
        //    Same primitive, two sources.
        let pprof_folded = pprof_report_to_folded_text(&filtered.report);
        let merged_pprof = merge_folded_text(&pprof_folded);
        let file = std::fs::File::create(path)
            .map_err(|e| anyhow::anyhow!("create {}: {e}", path.display()))?;
        let mut opts = pprof::flamegraph::Options::default();
        pprof::flamegraph::from_lines(&mut opts, merged_pprof.lines(), file)
            .map_err(|e| anyhow::anyhow!("render pprof flamegraph to {}: {e}", path.display()))?;
        tracing::info!(
            pprof_out = %path.display(),
            kept_samples = filtered.kept_count,
            dropped_samples = filtered.dropped_count,
            "harness: wrote pprof flamegraph (filtered to kardamom_node frames, merged across workers)"
        );
    }

    let bench_report = report::build_report(
        ReportInputs {
            workload_name: args.bench.workflow.name(),
            txs_per_task: args.bench.txs_per_task,
            max_in_flight: args.bench.max_in_flight,
            concurrency: args.bench.concurrency,
            configured_duration: args.bench.duration,
        },
        &outputs.counters,
        outputs.histograms,
        outputs.measurement_duration,
    );
    report::print_terminal(&bench_report);
    if let Some(path) = &args.report_json {
        report::write_json(path, &bench_report)?;
    }

    let _ = server_handle.stop();
    server_handle.stopped().await;
    Ok(())
}

/// Substring matched against demangled symbol names to decide whether a
/// pprof stack belongs to "the node." Matches `kardamom_node::node::...`,
/// `kardamom_node::executor::...`, etc.
const NODE_FRAME_NEEDLE: &str = "kardamom_node";

struct FilteredReport {
    report: pprof::Report,
    kept_count: isize,
    dropped_count: isize,
}

/// Walk `report.data` and keep only entries whose `Frames` contains at
/// least one symbol whose demangled name contains `NODE_FRAME_NEEDLE`.
/// Samples that land entirely in bench-side / jsonrpsee-client / tokio /
/// hyper code are dropped.
fn filter_to_node(report: &pprof::Report) -> FilteredReport {
    let mut kept = std::collections::HashMap::new();
    let mut kept_count: isize = 0;
    let mut dropped_count: isize = 0;
    for (frames, count) in &report.data {
        if frames_contains_node(frames) {
            kept.insert(frames.clone(), *count);
            kept_count += *count;
        } else {
            dropped_count += *count;
        }
    }
    FilteredReport {
        report: pprof::Report {
            data: kept,
            timing: report.timing.clone(),
        },
        kept_count,
        dropped_count,
    }
}

fn frames_contains_node(frames: &pprof::Frames) -> bool {
    frames.frames.iter().any(|frame| {
        frame
            .iter()
            .any(|sym| sym.name().contains(NODE_FRAME_NEEDLE))
    })
}

/// Render a `pprof::Report` to inferno-flamegraph folded text, using the
/// same `thread_name;leaf;...;root count` layout that `Report::flamegraph`
/// builds internally. Output gets piped through `merge_folded_text` to
/// drop the thread prefix.
fn pprof_report_to_folded_text(report: &pprof::Report) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for (key, value) in &report.data {
        let mut line = key.thread_name_or_id();
        for frame in key.frames.iter().rev() {
            for symbol in frame.iter().rev() {
                write!(&mut line, ";{symbol}").unwrap();
            }
        }
        write!(&mut line, " {value}").unwrap();
        out.push_str(&line);
        out.push('\n');
    }
    out
}

/// Inferno-flamegraph folded format is `"<thread>;<leaf>;...;<root> count"`.
/// This drops the `<thread>;` prefix, buckets by the remaining stack, and
/// sums counts. Used both for the `tracing-flame` `.folded` file and for
/// the on-CPU pprof report — both produce folded text with a per-thread
/// prefix, so the same merge collapses both correctly.
///
/// Lines with no `;` after the thread label (bare-root samples like
/// `ThreadId(N)-tokio-rt-worker 1234`) are dropped, matching the old
/// `grep ';'` recipe from the docs.
fn merge_folded_text(input: &str) -> String {
    use std::collections::BTreeMap;
    let mut merged: BTreeMap<String, u64> = BTreeMap::new();
    for line in input.lines() {
        let Some((stack, count_str)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(count) = count_str.parse::<u64>() else {
            continue;
        };
        let Some((_, rest)) = stack.split_once(';') else {
            continue;
        };
        // tracing-flame uses `"; "` as the separator after the thread
        // label, so the rest has a leading space we trim defensively.
        let rest = rest.trim_start();
        if rest.is_empty() {
            continue;
        }
        *merged.entry(rest.to_string()).or_insert(0) += count;
    }
    let mut out = String::with_capacity(input.len());
    for (stack, count) in &merged {
        out.push_str(stack);
        out.push(' ');
        out.push_str(&count.to_string());
        out.push('\n');
    }
    out
}

/// RAII guard tying an `AtomicBool` to a lexical scope. Stores `true` on
/// `enter`, restores `false` on drop — including on `?` early-return from
/// the surrounding `await`, which is the whole point.
struct ActiveScope<'a> {
    flag: &'a AtomicBool,
}

impl<'a> ActiveScope<'a> {
    fn enter(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self { flag }
    }
}

impl Drop for ActiveScope<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_folded_text_collapses_per_thread_prefix() {
        let input = "\
ThreadId(1)-tokio-worker;decode;execute 100
ThreadId(2)-tokio-worker;decode;execute 250
ThreadId(3)-tokio-worker;decode;execute 50
";
        let out = merge_folded_text(input);
        // All three lines collapse into one with the prefix dropped and
        // counts summed.
        assert_eq!(out.trim(), "decode;execute 400");
    }

    #[test]
    fn merge_folded_text_drops_bare_root_samples() {
        let input = "\
ThreadId(1)-tokio-worker 9999
ThreadId(2)-tokio-worker 5555
";
        let out = merge_folded_text(input);
        // Neither line has a `;` — both are bare-root samples and get
        // dropped (matching the legacy `grep ';'` recipe).
        assert_eq!(out.trim(), "");
    }

    #[test]
    fn merge_folded_text_skips_unparseable_count() {
        let input = "\
thr;a;b not_a_number
thr;a;b 42
";
        let out = merge_folded_text(input);
        assert_eq!(out.trim(), "a;b 42");
    }

    #[test]
    fn merge_folded_text_sums_within_a_thread() {
        let input = "\
ThreadId(1);a;b 10
ThreadId(1);a;b 20
ThreadId(1);a;c 5
";
        let out = merge_folded_text(input);
        // Output is BTreeMap-sorted so deterministic for assertion.
        assert_eq!(out, "a;b 30\na;c 5\n");
    }
}
