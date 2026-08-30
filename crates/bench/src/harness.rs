//! This is a single-process kardamom bench harness. It records with
//! `tracing-flame` and, if you turn it on, `pprof` CPU sampling. Both
//! scope to the dispatch window. The harness is generic over any
//! `BenchWorkflow`.
//!
//! The harness drives an in-process `IngressProxy` over in-memory
//! [`MockChannels`], with a simple fake executor. The fake executor
//! reflects every published `TxEnvelope` straight back as a success
//! `Receipt`. This exercises the real ingress hot path: batched
//! secp256k1 recovery, sender routing, jsonrpsee framing, and
//! parked-receipt release. It runs in a single process, with no live
//! Aeron media driver, so the profiling recordings stay close to the
//! dispatch window.
//!
//! This is the stand-in left in place after the removal of
//! `kardamom-node`. It profiles ingress only. A full in-process Aeron
//! pipeline harness, with a real sequencer, executor, and sealer so the
//! flame graph also shows revm and ordering work, is a follow-up item.
//! Ingress emits no `tracing` spans, so the `tracing-flame` SVG is
//! sparse for this stand-in. The `pprof` on-CPU sampler, which is
//! frame-based and filtered to `kardamom_ingress` frames, gives the
//! useful output here.
//!
//! `pprof` samples actual on-CPU time through SIGPROF. This covers
//! everything tokio, jsonrpsee, and secp256k1 do, not only our tracing
//! spans. The code builds its `ProfilerGuard` between warmup and
//! dispatch, and drops it right after dispatch returns, so the SVG
//! covers the same window as the `tracing-flame` recording.

mod flame;
mod inprocess;

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use jsonrpsee::http_client::HttpClient;
use tracing_flame::{FlameLayer, FlushGuard};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use crate::Benchmark;
use crate::benchmark::Outputs;
use crate::config::PPROF_HZ;
use crate::report::{self, ReportInputs};
use crate::workflow::BenchWorkflow;

use flame::{
    filter_to_ingress, flamegraph_options, merge_folded_text, pprof_report_to_folded_text,
};

pub use inprocess::{InProcessIngress, spawn_inprocess_ingress};

/// An in-process node, RPC server, and benchmark dispatcher, with
/// flame and pprof recordings scoped to the dispatch window.
pub struct Harness<W: BenchWorkflow> {
    /// The chain ID the in-process ingress serves through `eth_chainId`.
    /// Workflows presign transactions against this ID.
    pub chain_id: u64,
    /// The benchmark dispatcher this harness drives.
    pub bench: Benchmark<W>,
    /// The path to write the `tracing-flame` SVG to. The code merges
    /// this across tokio workers and renders it with inferno, so no
    /// `inferno-flamegraph` CLI post-processing is needed.
    pub flame_out: PathBuf,
    /// If set, write the [`BenchReport`](crate::report::BenchReport) as
    /// pretty JSON to this path, in addition to printing it to stdout.
    pub report_json: Option<PathBuf>,
    /// If set, record a `pprof` CPU sample over the dispatch window
    /// and write an inferno-flamegraph SVG to this path.
    pub pprof_out: Option<PathBuf>,
}

impl<W: BenchWorkflow> Harness<W> {
    /// Build the node and RPC server, and run a measured dispatch with
    /// flame and pprof recording scoped to the dispatch window.
    ///
    /// Flow: prepare the workflow against a live client, drain the
    /// workflow's warmup queue in order with recordings off, then turn
    /// on the flame and pprof gates and run the metered dispatch. The
    /// recordings see only the dispatch window, not signer derivation,
    /// presigning, or warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-process RPC server cannot bind, if the
    /// workflow's `prepare` or `dispatch` fails, or if the code cannot
    /// write an output file for flame, pprof, or the report JSON.
    /// This method does not check `workflow.genesis_alloc`: the ingress
    /// stand-in accepts submissions without balance checks, so it builds
    /// and checks no genesis state.
    ///
    /// # Panics
    ///
    /// Panics if the fixed loopback address `"127.0.0.1:0"` fails to
    /// parse. This cannot happen. It also panics, indirectly, if a
    /// sender task panics mid-iteration, which poisons the per-task
    /// accumulator mutex. See `Benchmark::dispatch`.
    pub async fn run(self) -> anyhow::Result<()> {
        let (active, flame_guard) = self.init_tracing()?;
        let (client, ingress) = self.build_ingress_and_client().await?;

        // All client-side cryptography (signer derivation, presigning) happens
        // here, before the dispatcher starts and well before the flame and
        // pprof gates turn on.
        let prepared = self.bench.prepare(&client).await?;

        // Drain the warmup queue with the flame and pprof gates still off.
        // After warmup, this builds the pprof guard, turns on the flame
        // gate, and runs the metered dispatch. The recordings see only the
        // dispatch window.
        self.bench.warmup(&client, prepared.warmup).await?;
        let pprof_guard = self.build_pprof_guard()?;
        self.log_dispatch_start();

        let outputs = {
            let _scope = ActiveScope::enter(&active);
            self.bench.dispatch(client, prepared.main).await?
        };

        self.write_flame_output(flame_guard)?;
        self.write_pprof_output(pprof_guard)?;
        self.emit_report(outputs)?;

        ingress.shutdown().await;
        Ok(())
    }

    /// The sidecar path, next to the SVG output, where `tracing-flame`
    /// writes its raw folded text. The code deletes this after it
    /// renders the SVG.
    fn folded_tmp_path(&self) -> PathBuf {
        let mut p = self.flame_out.clone().into_os_string();
        p.push(".folded.tmp");
        PathBuf::from(p)
    }

    /// Install the global tracing subscriber, with the gated flame
    /// layer. Returns the active-flag `AtomicBool`, which turns on
    /// around the dispatch window, and the flame layer's flush guard.
    fn init_tracing(&self) -> anyhow::Result<(Arc<AtomicBool>, FlushGuard<BufWriter<File>>)> {
        let active = Arc::new(AtomicBool::new(false));
        let active_for_filter = Arc::clone(&active);

        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("kardamom=info,kardamom_bench=info"));
        let fmt_layer = fmt::layer();

        let (flame, flame_guard) = FlameLayer::with_file(self.folded_tmp_path())?;
        let gated_flame = flame.with_filter(FilterFn::new(move |_meta| {
            active_for_filter.load(Ordering::Relaxed)
        }));

        // `try_init` fails if a global subscriber is already installed, for
        // example by an enclosing binary or test harness. This is not fatal:
        // the flame layer just does not receive events. Warn and continue,
        // instead of stopping.
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

        Ok((active, flame_guard))
    }

    /// Start the in-process ingress stand-in, see
    /// [`spawn_inprocess_ingress`], and a jsonrpsee client that points at
    /// it. A single shard is enough for the profiling stand-in: the fake
    /// executor reflects receipts regardless of partitioning, and the
    /// bench client drives one URL.
    async fn build_ingress_and_client(&self) -> anyhow::Result<(HttpClient, InProcessIngress)> {
        spawn_inprocess_ingress(self.chain_id, 1, self.bench.max_in_flight as usize).await
    }

    fn build_pprof_guard(&self) -> anyhow::Result<Option<pprof::ProfilerGuard<'static>>> {
        if self.pprof_out.is_none() {
            return Ok(None);
        }
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(PPROF_HZ)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .map_err(|e| anyhow::anyhow!("pprof guard build failed: {e}"))?;
        Ok(Some(guard))
    }

    fn log_dispatch_start(&self) {
        tracing::info!(
            workload = self.bench.workflow.name(),
            timeout = ?self.bench.timeout,
            concurrency = self.bench.concurrency,
            max_in_flight = self.bench.max_in_flight,
            txs_per_task = self.bench.txs_per_task,
            flame_out = %self.flame_out.display(),
            pprof_out = ?self.pprof_out.as_ref().map(|p| p.display().to_string()),
            "harness: dispatch (flame/pprof on)"
        );
    }

    /// Flush the flame layer, merge across tokio workers, and render the
    /// result to an inferno-flamegraph SVG at `self.flame_out`. This
    /// method cleans up the raw folded sidecar file on the way out.
    fn write_flame_output(&self, flame_guard: FlushGuard<BufWriter<File>>) -> anyhow::Result<()> {
        flame_guard
            .flush()
            .map_err(|e| anyhow::anyhow!("flush flamegraph: {e}"))?;
        // Drop the guard before reading, so no further writes race with this read.
        drop(flame_guard);
        let folded_tmp = self.folded_tmp_path();
        let raw_flame = std::fs::read_to_string(&folded_tmp)
            .map_err(|e| anyhow::anyhow!("read {}: {e}", folded_tmp.display()))?;
        let merged_flame = merge_folded_text(&raw_flame);

        // The in-process ingress stand-in emits no `tracing` spans (only the
        // node did). So the folded text is often just bare tokio-worker roots,
        // which `merge_folded_text` drops, leaving nothing to render. Inferno
        // fails on empty input, so skip the SVG and log instead. The `pprof`
        // on-CPU path, which is frame-based, is the useful profiling output
        // for this stand-in. The full pipeline harness restores span-based
        // flame graphs.
        if merged_flame.trim().is_empty() {
            let _ = std::fs::remove_file(&folded_tmp);
            tracing::warn!(
                flame_out = %self.flame_out.display(),
                "harness: no tracing spans recorded (ingress emits none); \
                 skipping tracing-flame SVG — use --pprof-out for on-CPU frames"
            );
            return Ok(());
        }

        let svg = File::create(&self.flame_out)
            .map_err(|e| anyhow::anyhow!("create {}: {e}", self.flame_out.display()))?;
        let mut opts = flamegraph_options();
        pprof::flamegraph::from_lines(&mut opts, merged_flame.lines(), svg).map_err(|e| {
            anyhow::anyhow!("render flame SVG to {}: {e}", self.flame_out.display())
        })?;
        // This cleanup is best-effort. Leaving the sidecar file behind is
        // not fatal, only untidy.
        let _ = std::fs::remove_file(&folded_tmp);
        tracing::info!(
            flame_out = %self.flame_out.display(),
            raw_bytes = raw_flame.len(),
            merged_bytes = merged_flame.len(),
            "harness: rendered tracing-flame SVG (merged across tokio workers)"
        );
        Ok(())
    }

    /// Render the pprof CPU sample to an inferno-flamegraph SVG. This
    /// method does nothing if the guard or the output path is missing.
    fn write_pprof_output(
        &self,
        pprof_guard: Option<pprof::ProfilerGuard<'static>>,
    ) -> anyhow::Result<()> {
        let (Some(guard), Some(path)) = (pprof_guard, self.pprof_out.as_ref()) else {
            return Ok(());
        };
        let report = guard
            .report()
            .build()
            .map_err(|e| anyhow::anyhow!("pprof report build failed: {e}"))?;
        // 1. Keep only stacks with at least one `kardamom_ingress::*` frame.
        //    The harness runs the ingress proxy and the bench client on the
        //    same tokio runtime, so the raw report mixes ingress and client
        //    work. The SVG should show ingress work only.
        let filtered = filter_to_ingress(&report);
        // 2. Convert the filtered report to folded text. Then run the same
        //    `merge_folded_text` function used for tracing-flame above, to
        //    drop the thread-name prefix and sum identical stacks across
        //    workers. This is the same primitive, applied to two sources.
        let pprof_folded = pprof_report_to_folded_text(&filtered.report);
        let merged_pprof = merge_folded_text(&pprof_folded);
        let file =
            File::create(path).map_err(|e| anyhow::anyhow!("create {}: {e}", path.display()))?;
        let mut opts = flamegraph_options();
        pprof::flamegraph::from_lines(&mut opts, merged_pprof.lines(), file)
            .map_err(|e| anyhow::anyhow!("render pprof flamegraph to {}: {e}", path.display()))?;
        tracing::info!(
            pprof_out = %path.display(),
            kept_samples = filtered.kept_count,
            dropped_samples = filtered.dropped_count,
            "harness: wrote pprof flamegraph (filtered to kardamom_ingress frames, merged across workers)"
        );
        Ok(())
    }

    fn emit_report(&self, outputs: Outputs) -> anyhow::Result<()> {
        let bench_report = report::build_report(
            ReportInputs {
                workload_name: self.bench.workflow.name(),
                txs_per_task: self.bench.txs_per_task,
                max_in_flight: self.bench.max_in_flight,
                concurrency: self.bench.concurrency,
                configured_timeout: self.bench.timeout,
            },
            &outputs.counters,
            outputs.histograms,
            outputs.measurement_duration,
        );
        report::print_terminal(&bench_report);
        if let Some(path) = &self.report_json {
            report::write_json(path, &bench_report)?;
        }
        Ok(())
    }
}

/// An RAII guard that ties an `AtomicBool` to a lexical scope. It stores
/// `true` on `enter`, and restores `false` on drop. This includes drop
/// on a `?` early return from the surrounding `await`, which is the
/// reason this guard exists.
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
