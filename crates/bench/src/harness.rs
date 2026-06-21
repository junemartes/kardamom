//! Single-process kardamom bench harness with `tracing-flame` recording
//! and optional `pprof`-based CPU sampling, both scoped to the dispatch
//! window. Generic over any `BenchWorkflow`.
//!
//! The harness drives an **in-process `IngressProxy`** over in-memory
//! [`MockChannels`] plus a trivial "fake executor" that reflects every
//! published `TxEnvelope` straight back as a success `Receipt`. This
//! exercises the real ingress hot path (batched secp256k1 recovery, sender
//! routing, jsonrpsee framing, parked-receipt release) in a single process
//! with no live Aeron media driver — so the profiling recordings stay tight
//! around the dispatch window.
//!
//! This is the decoupled stand-in left in place after `kardamom-node` was
//! removed. It profiles ingress only; the **full in-process Aeron pipeline
//! harness** (real sequencer + executor + sealer, so the flame shows revm and
//! ordering work too) is tracked as a follow-up. Because ingress emits no
//! `tracing` spans, the `tracing-flame` SVG is sparse for this stand-in; the
//! `pprof` on-CPU sampler (frame-based, filtered to `kardamom_ingress` frames)
//! is the useful output here.
//!
//! `pprof` samples actual on-CPU time via SIGPROF (everything tokio,
//! jsonrpsee, secp256k1 actually do — not just our tracing spans). Its
//! `ProfilerGuard` is built right between warmup and dispatch and dropped
//! right after dispatch returns, so the SVG is bounded to the same window
//! as the `tracing-flame` recording.

use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use tracing_flame::{FlameLayer, FlushGuard};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::FilterFn;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, fmt};

use kardamom_ingress::{InMemoryStateDb, IngressConfig, IngressHandle, IngressProxy, MockChannels};
use kardamom_types::{AckPolicy, BPosition, Receipt};

use crate::Benchmark;
use crate::benchmark::Outputs;
use crate::config::{MAX_IN_FLIGHT_SLACK, PPROF_HZ, REQUEST_TIMEOUT};
use crate::report::{self, ReportInputs};
use crate::workflow::BenchWorkflow;

/// In-process node + RPC server + benchmark dispatcher, with the
/// flame/pprof recordings scoped to the dispatch window.
pub struct Harness<W: BenchWorkflow> {
    /// Chain ID the in-process ingress serves via `eth_chainId` (workflows
    /// presign against it).
    pub chain_id: u64,
    /// The benchmark dispatcher this harness drives.
    pub bench: Benchmark<W>,
    /// Path to write the `tracing-flame` SVG. Merged across tokio
    /// workers and rendered via inferno (no `inferno-flamegraph` CLI
    /// post-processing required).
    pub flame_out: PathBuf,
    /// If set, write the [`BenchReport`](crate::report::BenchReport) as
    /// pretty JSON to this path in addition to printing to stdout.
    pub report_json: Option<PathBuf>,
    /// If set, record a `pprof` CPU sample over the dispatch window and
    /// write an inferno-flamegraph SVG to this path.
    pub pprof_out: Option<PathBuf>,
}

impl<W: BenchWorkflow> Harness<W> {
    /// Build the node + RPC server and drive a measured dispatch with
    /// flame/pprof recording scoped to the dispatch window.
    ///
    /// Flow: prepare the workflow against a live client, drain the
    /// workflow's warmup queue sequentially with recordings off, then
    /// flip the flame/pprof gates on and run the metered dispatch. The
    /// recordings see only the dispatch window — no signer derivation,
    /// no presigning, no warmup.
    ///
    /// # Errors
    ///
    /// Errors if the genesis derived from `workflow.genesis_alloc` is
    /// invalid, the in-process RPC server can't bind, the workflow's
    /// `prepare`/`dispatch` fails, or any of the output files (flame,
    /// pprof, report JSON) can't be written.
    ///
    /// # Panics
    ///
    /// Panics if the hardcoded loopback bind literal `"127.0.0.1:0"` fails
    /// to parse (impossible). Indirectly, panics if a sender task panics
    /// mid-iteration (poisons the per-task accumulator mutex — see
    /// `Benchmark::dispatch`).
    pub async fn run(self) -> anyhow::Result<()> {
        let (active, flame_guard) = self.init_tracing()?;
        let (client, ingress) = self.build_ingress_and_client().await?;

        // All client-side crypto (signer derivation, presigning) happens
        // here, before the dispatcher starts and well before the
        // flame/pprof gates flip on.
        let prepared = self.bench.prepare(&client).await?;

        // Drain the warmup queue with the flame/pprof gates still off.
        // After warmup we build the pprof guard, flip the flame gate, and
        // run the metered dispatch. The recordings see only the dispatch
        // window.
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

    /// Sidecar path next to the SVG output where `tracing-flame` writes
    /// its raw folded text. Deleted after the SVG is rendered.
    fn folded_tmp_path(&self) -> PathBuf {
        let mut p = self.flame_out.clone().into_os_string();
        p.push(".folded.tmp");
        PathBuf::from(p)
    }

    /// Install the global tracing subscriber with the gated flame layer.
    /// Returns the active-flag `AtomicBool` (flipped on around the
    /// dispatch window) and the flame layer's flush guard.
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

        // `try_init` fails if a global subscriber was already installed
        // (e.g. by an enclosing binary or test harness). That's not fatal
        // — the flame layer just won't receive events. Warn and continue
        // rather than abort.
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

    /// Spin up the in-process ingress stand-in (see [`spawn_inprocess_ingress`])
    /// and a jsonrpsee client pointed at it. A single shard is plenty for the
    /// profiling stand-in — the fake executor reflects receipts regardless of
    /// partitioning, and the bench client drives one URL.
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
    /// result to an inferno-flamegraph SVG at `self.flame_out`. The raw
    /// folded sidecar is cleaned up on the way out.
    fn write_flame_output(&self, flame_guard: FlushGuard<BufWriter<File>>) -> anyhow::Result<()> {
        flame_guard
            .flush()
            .map_err(|e| anyhow::anyhow!("flush flamegraph: {e}"))?;
        // Drop the guard before reading so no further writes race with us.
        drop(flame_guard);
        let folded_tmp = self.folded_tmp_path();
        let raw_flame = std::fs::read_to_string(&folded_tmp)
            .map_err(|e| anyhow::anyhow!("read {}: {e}", folded_tmp.display()))?;
        let merged_flame = merge_folded_text(&raw_flame);

        // The in-process ingress stand-in emits no `tracing` spans (only the
        // node did), so the folded text is frequently just bare tokio-worker
        // roots that `merge_folded_text` drops — leaving nothing to render.
        // inferno errors on empty input, so skip the SVG and log instead. The
        // `pprof` on-CPU path (frame-based) is the useful profiling output for
        // this stand-in; the full pipeline harness restores span-based flames.
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
        // Best-effort cleanup. Leaving the sidecar around isn't fatal,
        // just untidy.
        let _ = std::fs::remove_file(&folded_tmp);
        tracing::info!(
            flame_out = %self.flame_out.display(),
            raw_bytes = raw_flame.len(),
            merged_bytes = merged_flame.len(),
            "harness: rendered tracing-flame SVG (merged across tokio workers)"
        );
        Ok(())
    }

    /// Render the pprof CPU sample to an inferno-flamegraph SVG. No-op if
    /// either the guard or the output path is absent.
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
        // 1. Keep only stacks that contain at least one `kardamom_ingress::*`
        //    frame. The harness runs the ingress proxy and the bench client on
        //    the same tokio runtime, so the raw report mixes ingress and client
        //    work; the SVG should show ingress work only.
        let filtered = filter_to_ingress(&report);
        // 2. Convert the filtered Report to folded text, then run the same
        //    `merge_folded_text` we use for tracing-flame above to drop
        //    the thread-name prefix and sum identical stacks across
        //    workers. Same primitive, two sources.
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

/// Spin up an in-process [`IngressProxy`] over in-memory [`MockChannels`] plus
/// a trivial "fake executor" that turns every published `TxEnvelope` straight
/// into a success `Receipt`. Returns a jsonrpsee client pointed at the proxy's
/// ephemeral loopback port and a handle that stops everything on
/// [`InProcessIngress::shutdown`].
///
/// This is the decoupled stand-in backing the profiling [`Harness`] and the
/// crate's smoke tests after `kardamom-node` was removed: it exercises the real
/// ingress hot path (sig recovery, routing, RPC framing, receipt release)
/// without a live Aeron media driver or the real sequencer/executor/sealer. The
/// full in-process Aeron pipeline harness is tracked as a follow-up.
///
/// `ack_policy` is forced to [`AckPolicy::OnOffer`] so a submission is released
/// on receipt arrival alone — there is no recorder/quorum watermark here.
pub async fn spawn_inprocess_ingress(
    chain_id: u64,
    shards: u32,
    max_in_flight: usize,
) -> anyhow::Result<(HttpClient, InProcessIngress)> {
    let (mock, shard_rxs) = MockChannels::new(shards as usize);
    let receipt_tx = mock.receipt_bus.clone();

    // One fake-executor task per shard: drain published envelopes and reflect a
    // success receipt back onto the receipt bus so the proxy releases the
    // parked submission. `from`/`nonce` mirror exactly what the proxy parked on
    // (it keys pending submissions by `(sender, nonce)`).
    let mut fake_exec = Vec::with_capacity(shard_rxs.len());
    for (shard, mut rx) in shard_rxs.into_iter().enumerate() {
        let receipt_tx = receipt_tx.clone();
        fake_exec.push(tokio::spawn(async move {
            let mut idx: i32 = 0;
            while let Some(env) = rx.recv().await {
                idx = idx.wrapping_add(1);
                let nonce = decode_nonce(env.raw_tx.as_ref()).unwrap_or(0);
                let receipt = Receipt {
                    tx_idx: BPosition {
                        term_id: shard as i32,
                        term_offset: idx,
                    },
                    tx_hash: env.tx_hash,
                    status: true,
                    gas_used: 21_000,
                    nonce,
                    from: env.sender,
                    ..Default::default()
                };
                // A send error means the broadcast bus has no receivers, i.e.
                // the proxy is gone — nothing left to release.
                if receipt_tx.send(receipt).is_err() {
                    break;
                }
            }
        }));
    }

    let cfg = IngressConfig {
        chain_id,
        partition_count_m: shards,
        ack_policy: AckPolicy::OnOffer,
        ..IngressConfig::default()
    };
    let state_db = Arc::new(InMemoryStateDb::new());
    let proxy = IngressProxy::new(cfg, mock.clone(), mock, state_db);
    let handle = proxy.start().await?;

    let url = format!("http://{}", handle.jsonrpc_addr);
    let client = HttpClientBuilder::default()
        .request_timeout(REQUEST_TIMEOUT)
        .max_concurrent_requests(max_in_flight + MAX_IN_FLIGHT_SLACK)
        .build(&url)?;
    Ok((client, InProcessIngress { handle, fake_exec }))
}

/// Owns the in-process ingress server and the fake-executor reflector tasks.
/// Call [`InProcessIngress::shutdown`] (or drop it) to tear them down.
pub struct InProcessIngress {
    handle: IngressHandle,
    fake_exec: Vec<tokio::task::JoinHandle<()>>,
}

impl InProcessIngress {
    /// Stop the jsonrpsee server and abort the fake-executor tasks.
    pub async fn shutdown(self) {
        let _ = self.handle.jsonrpc_handle.stop();
        self.handle.jsonrpc_handle.stopped().await;
        for h in self.fake_exec {
            h.abort();
        }
    }
}

/// Decode the tx nonce from raw envelope bytes the same way the ingress proxy
/// does (`alloy_consensus::TxEnvelope::decode`), so the synthesized receipt's
/// `(from, nonce)` matches the parked-submission key. Returns `None` if the
/// bytes don't decode (the proxy would already have rejected such a tx).
fn decode_nonce(raw: &[u8]) -> Option<u64> {
    use alloy_consensus::transaction::Transaction;
    use alloy_rlp::Decodable;
    let mut slice = raw;
    alloy_consensus::TxEnvelope::decode(&mut slice)
        .ok()
        .map(|env| env.nonce())
}

/// Inferno-flamegraph options shared by the tracing-flame and pprof
/// renderers. `min_width = 0.0` keeps narrow frames visible (matches
/// `inferno-flamegraph --minwidth 0`); `image_width = 2000` is enough
/// resolution for the dispatch-window stacks without bloating the SVG.
fn flamegraph_options() -> pprof::flamegraph::Options<'static> {
    let mut opts = pprof::flamegraph::Options::default();
    opts.min_width = 0.0;
    opts.image_width = Some(2_000);
    opts
}

/// Substring matched against demangled symbol names to decide whether a
/// pprof stack belongs to the ingress proxy. Matches
/// `kardamom_ingress::proxy::...`, `kardamom_ingress::sig_verify::...`, etc.
const INGRESS_FRAME_NEEDLE: &str = "kardamom_ingress";

struct FilteredReport {
    report: pprof::Report,
    kept_count: isize,
    dropped_count: isize,
}

/// Walk `report.data` and keep only entries whose `Frames` contains at
/// least one symbol whose demangled name contains `INGRESS_FRAME_NEEDLE`.
/// Samples that land entirely in bench-side / jsonrpsee-client / tokio /
/// hyper code are dropped.
fn filter_to_ingress(report: &pprof::Report) -> FilteredReport {
    let mut kept = std::collections::HashMap::new();
    let mut kept_count: isize = 0;
    let mut dropped_count: isize = 0;
    for (frames, count) in &report.data {
        if frames_contains_ingress(frames) {
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

fn frames_contains_ingress(frames: &pprof::Frames) -> bool {
    frames.frames.iter().any(|frame| {
        frame
            .iter()
            .any(|sym| sym.name().contains(INGRESS_FRAME_NEEDLE))
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
