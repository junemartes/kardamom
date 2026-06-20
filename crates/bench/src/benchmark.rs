//! `Benchmark<W>` — the dispatcher. Generic over a `BenchWorkflow`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use jsonrpsee::http_client::HttpClient;

use crate::config::{
    DEFAULT_CONCURRENCY, DEFAULT_MAX_IN_FLIGHT, DEFAULT_TIMEOUT, DEFAULT_TXS_PER_TASK,
};
use crate::report::Counters;
use crate::workflow::BenchWorkflow;

const HIST_LOWEST_US: u64 = 1;
const HIST_HIGHEST_US: u64 = 60_000_000; // 60s
const HIST_SIGFIGS: u8 = 3;

/// Knobs + workflow. Construct directly; `Default` fills in the standard
/// `DEFAULT_*` constants from `crate::config`.
pub struct Benchmark<W: BenchWorkflow> {
    /// Workflow that produces work items and dispatches them. Generic
    /// over [`BenchWorkflow`] so external crates can plug in their own.
    pub workflow: W,
    /// Safety timeout applied to each phase (warmup and dispatch get one
    /// each). The runtime applies `tokio::time::timeout(timeout, ...)`
    /// per sender task; whichever of "vec drained" or "timeout fired"
    /// comes first ends the phase.
    pub timeout: Duration,
    /// Number of sender tasks (= number of derived signers, one per
    /// task). Built-in workflows use this to size their alloc set.
    pub concurrency: u32,
    /// Pre-signed transactions queued per sender task. Total work the
    /// run will attempt = `txs_per_task * concurrency`.
    pub txs_per_task: u32,
    /// Cap on outstanding requests across all senders. Enforced at the
    /// HTTP client layer (`max_concurrent_requests`), not by a per-task
    /// semaphore — see the docstring on [`Benchmark::dispatch`].
    pub max_in_flight: u32,
}

impl<W: BenchWorkflow + Default> Default for Benchmark<W> {
    fn default() -> Self {
        Self {
            workflow: W::default(),
            timeout: DEFAULT_TIMEOUT,
            concurrency: DEFAULT_CONCURRENCY,
            txs_per_task: DEFAULT_TXS_PER_TASK,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
        }
    }
}

/// Work items returned by [`BenchWorkflow::prepare`]. Two phases with
/// different shapes:
/// - `warmup` is a single flat queue dispatched **sequentially** by one
///   sender — no concurrency, no metering.
/// - `main` is `n_tasks`-by-`txs_per_task` and runs concurrently in the
///   metered dispatch window.
///
/// Workflows that align tx state across phases (e.g. transfers consume
/// nonces) must lay the warmup queue out so the per-task `main` chunks
/// pick up at the right nonce.
pub struct Prepared<I> {
    /// Flat warmup queue. Dispatched sequentially by a single sender,
    /// unmetered.
    pub warmup: Vec<I>,
    /// Per-task metered dispatch items — what the report measures.
    pub main: Vec<Vec<I>>,
}

impl<I> std::fmt::Debug for Prepared<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prepared")
            .field("warmup_items", &self.warmup.len())
            .field("tasks", &self.main.len())
            .field(
                "main_per_task",
                &self.main.first().map_or(0, std::vec::Vec::len),
            )
            .finish()
    }
}

/// Result of one `Benchmark::dispatch` (or `run`) call.
pub struct Outputs {
    /// Aggregate `ok` / `err` / `sent` counts summed across all tasks.
    pub counters: Counters,
    /// Per-method histograms merged across all tasks. Use for global
    /// p50/p90/p99.
    pub histograms: BTreeMap<String, Histogram<u64>>,
    /// Wall-clock from the start of `dispatch` to the moment the last
    /// sender task returned (cancelled or vec-exhausted).
    pub measurement_duration: Duration,
}

impl<W: BenchWorkflow> Benchmark<W> {
    /// Stage 1 — build per-task work vecs against a live client. All
    /// crypto, all signer derivation, all chain-state probes happen here.
    /// **No measurement.** Returns both warmup and main items per task.
    ///
    /// # Errors
    ///
    /// Forwards errors from `BenchWorkflow::prepare` (workflow-specific
    /// chain-state probes, signer derivation, presigning). Also bails
    /// early if `concurrency == 0` or `txs_per_task == 0` — both are
    /// user knobs that would otherwise silently produce a zero-sample
    /// run.
    pub async fn prepare(&self, client: &HttpClient) -> anyhow::Result<Prepared<W::Item>> {
        if self.concurrency == 0 {
            anyhow::bail!("Benchmark.concurrency must be > 0");
        }
        if self.txs_per_task == 0 {
            anyhow::bail!("Benchmark.txs_per_task must be > 0");
        }
        self.workflow
            .prepare(client, self.concurrency, self.txs_per_task)
            .await
    }

    /// Stage 2 — drain the warmup queue **sequentially** with a single
    /// in-flight request, unmetered. No histograms, no counters, no
    /// concurrency — the whole point is to JIT/warm the hot paths and
    /// stabilize chain state before the metered window. The caller is
    /// expected to keep flame/pprof recording gates *off* during this call.
    ///
    /// Bounded by `self.timeout` total wall-clock; whichever of "queue
    /// drained" or "timeout fired" comes first ends the phase.
    ///
    /// A no-op (returns immediately) when `warmup` is empty.
    ///
    /// # Errors
    ///
    /// Workflow dispatch errors are intentionally swallowed — warmup is
    /// best-effort.
    pub async fn warmup(&self, client: &HttpClient, warmup: Vec<W::Item>) -> anyhow::Result<()> {
        if warmup.is_empty() {
            return Ok(());
        }
        let total = warmup.len();
        let workflow = self.workflow.clone();
        let start = Instant::now();
        let _ = tokio::time::timeout(self.timeout, async {
            for item in warmup {
                let _ = workflow.dispatch(client, item).await;
            }
        })
        .await;
        tracing::info!(
            items = total,
            elapsed = ?start.elapsed(),
            "benchmark: warmup complete"
        );
        Ok(())
    }

    /// Stage 3 — the measured window. Spawns one sender task per work vec
    /// inside a `tokio::time::timeout(self.timeout, ...)`. Each sender
    /// loops serially over its vec (per-task in-flight = 1); the
    /// runtime-wide `max_in_flight` budget is enforced at the HTTP client
    /// layer (`max_concurrent_requests = max_in_flight + MAX_IN_FLIGHT_SLACK`),
    /// not here.
    /// Per-task `Arc<TaskAccum>` preserves samples through timeout-driven
    /// cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if histogram allocation fails, if a sender task
    /// panics (the join handle propagates the panic), or if histogram
    /// merging detects a unit mismatch (cannot happen with the bounds
    /// configured here, but propagated for completeness).
    ///
    /// # Panics
    ///
    /// Panics if a sender task panicked while holding the per-task
    /// accumulator mutex — that poisons the mutex and we surface it as a
    /// hard error rather than silently dropping samples.
    pub async fn dispatch(
        &self,
        client: HttpClient,
        main: Vec<Vec<W::Item>>,
    ) -> anyhow::Result<Outputs> {
        let methods = self.workflow.methods();
        let workflow = Arc::new(self.workflow.clone());
        let client = Arc::new(client);

        let start = Instant::now();

        let mut accums: Vec<Arc<TaskAccum>> = Vec::with_capacity(main.len());
        let mut handles = Vec::with_capacity(main.len());
        for work in main {
            let accum = Arc::new(TaskAccum::new(methods)?);
            accums.push(Arc::clone(&accum));
            let client = Arc::clone(&client);
            let workflow = Arc::clone(&workflow);
            let timeout = self.timeout;
            handles.push(tokio::spawn(async move {
                let _ =
                    tokio::time::timeout(timeout, send_loop(workflow, accum, client, work)).await;
            }));
        }

        for h in handles {
            h.await.map_err(|e| anyhow::anyhow!("task join: {e}"))?;
        }

        let measurement_duration = start.elapsed();

        let mut counters = Counters {
            sent: 0,
            ok: 0,
            err: 0,
        };
        let mut per_task: Vec<BTreeMap<String, Histogram<u64>>> = Vec::with_capacity(accums.len());
        for accum in accums {
            counters.ok += accum.ok.load(Ordering::Relaxed);
            counters.err += accum.err.load(Ordering::Relaxed);
            // SAFETY-ish: the per-task `TaskAccum` mutex is only locked
            // here and inside `send_loop`. A poisoned mutex means a sender
            // task panicked mid-iteration — that's a real bug we want to
            // surface, not silently drop samples for.
            let h = accum
                .histograms
                .lock()
                .expect("task accumulator mutex poisoned (sender task panicked)")
                .clone();
            per_task.push(h);
        }
        counters.sent = counters.ok + counters.err;

        let mut merged: BTreeMap<String, Histogram<u64>> = BTreeMap::new();
        for m in methods {
            merged.insert(
                (*m).to_string(),
                Histogram::<u64>::new_with_bounds(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIGFIGS)?,
            );
        }
        for task_hist in &per_task {
            for (k, h) in task_hist {
                if let Some(m) = merged.get_mut(k) {
                    m.add(h).map_err(|e| anyhow::anyhow!("hist merge: {e:?}"))?;
                }
            }
        }

        Ok(Outputs {
            counters,
            histograms: merged,
            measurement_duration,
        })
    }

    /// Convenience: `prepare` → `warmup` → `dispatch`. Callers that need
    /// finer scoping (the in-process harness flips flame/pprof gates
    /// between warmup and dispatch) should call the stages separately.
    ///
    /// # Errors
    ///
    /// Forwards errors from `workflow.prepare` (chain-state probes,
    /// signer derivation, presigning), `Benchmark::warmup`, and
    /// `Benchmark::dispatch`.
    pub async fn run(&self, client: HttpClient) -> anyhow::Result<Outputs> {
        let prepared = self.prepare(&client).await?;
        self.warmup(&client, prepared.warmup).await?;
        self.dispatch(client, prepared.main).await
    }
}

struct TaskAccum {
    ok: AtomicU64,
    err: AtomicU64,
    histograms: Mutex<BTreeMap<String, Histogram<u64>>>,
}

impl TaskAccum {
    fn new(methods: &[&'static str]) -> anyhow::Result<Self> {
        let mut histograms = BTreeMap::new();
        for m in methods {
            histograms.insert(
                (*m).to_string(),
                Histogram::<u64>::new_with_bounds(HIST_LOWEST_US, HIST_HIGHEST_US, HIST_SIGFIGS)?,
            );
        }
        Ok(Self {
            ok: AtomicU64::new(0),
            err: AtomicU64::new(0),
            histograms: Mutex::new(histograms),
        })
    }
}

async fn send_loop<W: BenchWorkflow>(
    workflow: Arc<W>,
    accum: Arc<TaskAccum>,
    client: Arc<HttpClient>,
    work: Vec<W::Item>,
) {
    // No per-task semaphore: each sender task awaits a single in-flight
    // request before issuing the next, so per-task in-flight is always 1.
    // The `max_in_flight` knob lives on at the HTTP client (the harness
    // configures `max_concurrent_requests(max_in_flight + MAX_IN_FLIGHT_SLACK)`)
    // and acts
    // as the runtime-wide budget across all sender tasks.
    for item in work {
        let t0 = Instant::now();
        let (method, ok) = workflow.dispatch(&client, item).await;
        // `as_micros` returns `u128`; saturate to `u64` for any dispatch
        // that returns in under ~584,500 years.
        let elapsed_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);

        if ok {
            accum.ok.fetch_add(1, Ordering::Relaxed);
        } else {
            accum.err.fetch_add(1, Ordering::Relaxed);
        }
        if let Ok(mut h) = accum.histograms.lock()
            && let Some(hist) = h.get_mut(method)
        {
            let _ = hist.record(elapsed_us.clamp(HIST_LOWEST_US, HIST_HIGHEST_US));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflows::TransfersWorkflow;
    use jsonrpsee::http_client::HttpClientBuilder;

    fn dummy_bench(concurrency: u32, txs_per_task: u32) -> Benchmark<TransfersWorkflow> {
        Benchmark {
            workflow: TransfersWorkflow::default(),
            timeout: Duration::from_secs(1),
            concurrency,
            txs_per_task,
            max_in_flight: 1,
        }
    }

    fn dummy_client() -> HttpClient {
        // Never actually used — the `prepare` zero-knob guards bail before
        // touching the client.
        HttpClientBuilder::default()
            .build("http://127.0.0.1:1")
            .expect("dummy client build")
    }

    #[tokio::test]
    async fn prepare_bails_on_zero_concurrency() {
        let err = dummy_bench(0, 10)
            .prepare(&dummy_client())
            .await
            .expect_err("concurrency=0 should bail");
        assert!(format!("{err:#}").contains("concurrency"));
    }

    #[tokio::test]
    async fn prepare_bails_on_zero_txs_per_task() {
        let err = dummy_bench(4, 0)
            .prepare(&dummy_client())
            .await
            .expect_err("txs_per_task=0 should bail");
        assert!(format!("{err:#}").contains("txs_per_task"));
    }
}
