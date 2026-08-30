//! `Benchmark<W>` is the dispatcher. It is generic over a `BenchWorkflow`.

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
const HIST_HIGHEST_US: u64 = 60_000_000; // 60 seconds
const HIST_SIGFIGS: u8 = 3;

/// The settings and the workflow for a run.
/// Construct this directly. `Default` fills in the standard
/// `DEFAULT_*` constants from `crate::config`.
pub struct Benchmark<W: BenchWorkflow> {
    /// The workflow that produces work items and dispatches them.
    /// This is generic over [`BenchWorkflow`], so an external crate
    /// can plug in its own workflow.
    pub workflow: W,
    /// A safety timeout for each phase. Warmup and dispatch each get
    /// their own timeout. The runtime applies
    /// `tokio::time::timeout(timeout, ...)` per sender task. The phase
    /// ends when the work vector is drained or the timeout fires,
    /// whichever comes first.
    pub timeout: Duration,
    /// The number of sender tasks. This equals the number of derived
    /// signers, one per task. Built-in workflows use this value to size
    /// their allocation set.
    pub concurrency: u32,
    /// The number of pre-signed transactions in the queue of each sender
    /// task. The run attempts a total of `txs_per_task * concurrency`
    /// items of work.
    pub txs_per_task: u32,
    /// The limit on outstanding requests across all senders. The HTTP
    /// client layer enforces this limit, through `max_concurrent_requests`,
    /// not a per-task semaphore. See the doc comment on
    /// [`Benchmark::dispatch`].
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

/// Work items returned by [`BenchWorkflow::prepare`]. This struct has two
/// phases, with different shapes:
/// - `warmup` is a single flat queue. One sender dispatches it in order,
///   with no concurrency and no metering.
/// - `main` has `n_tasks` rows of `txs_per_task` items each. It runs
///   concurrently, in the metered dispatch window.
///
/// A workflow that aligns transaction state across phases, for example
/// transfers that use up nonces, must lay out the warmup queue so each
/// per-task `main` chunk starts at the right nonce.
pub struct Prepared<I> {
    /// The flat warmup queue. One sender dispatches it in order,
    /// without metering.
    pub warmup: Vec<I>,
    /// The per-task metered dispatch items. The report measures these.
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

/// The result of one `Benchmark::dispatch` call, or one `run` call.
pub struct Outputs {
    /// The `ok`, `err`, and `sent` counts, summed across all tasks.
    pub counters: Counters,
    /// The per-method histograms, merged across all tasks.
    /// Use these for the global p50, p90, and p99 values.
    pub histograms: BTreeMap<String, Histogram<u64>>,
    /// The wall-clock time from the start of `dispatch` to the moment the
    /// last sender task returns, either cancelled or with an empty vector.
    pub measurement_duration: Duration,
}

impl<W: BenchWorkflow> Benchmark<W> {
    /// Stage 1: build per-task work vectors against a live client. All
    /// cryptography, signer derivation, and chain-state checks happen
    /// here. This stage does no measurement. It returns both warmup and
    /// main items for each task.
    ///
    /// # Errors
    ///
    /// Forwards errors from `BenchWorkflow::prepare`, such as
    /// workflow-specific chain-state checks, signer derivation, or
    /// presigning. Also fails early if `concurrency == 0` or
    /// `txs_per_task == 0`. Both are user settings that would otherwise
    /// silently produce a run with zero samples.
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

    /// Stage 2: drain the warmup queue in order, with one request in
    /// flight at a time, without metering. This stage keeps no
    /// histograms and no counters, and runs with no concurrency. Its
    /// purpose is to warm the hot paths and the JIT, and to stabilize
    /// chain state, before the metered window starts. The caller should
    /// keep flame and pprof recording off during this call.
    ///
    /// The wall-clock time is bounded by `self.timeout`. The phase ends
    /// when the queue is drained or the timeout fires, whichever comes
    /// first.
    ///
    /// This method returns immediately, and does nothing, when `warmup`
    /// is empty.
    ///
    /// # Errors
    ///
    /// This method ignores workflow dispatch errors on purpose.
    /// Warmup is best-effort.
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

    /// Stage 3: the measured window. This method spawns one sender task
    /// per work vector, inside a `tokio::time::timeout(self.timeout, ...)`.
    /// Each sender loops over its vector in order, so each task has one
    /// request in flight at a time. The HTTP client layer enforces the
    /// runtime-wide `max_in_flight` budget, as
    /// `max_concurrent_requests = max_in_flight + MAX_IN_FLIGHT_SLACK`,
    /// not this method.
    /// A per-task `Arc<TaskAccum>` keeps samples even when a timeout
    /// cancels the task.
    ///
    /// # Errors
    ///
    /// Returns an error if histogram allocation fails, if a sender task
    /// panics (the join handle forwards the panic), or if histogram
    /// merging finds a unit mismatch. A unit mismatch cannot happen with
    /// the bounds set here, but the method reports it for completeness.
    ///
    /// # Panics
    ///
    /// Panics if a sender task panicked while it held the per-task
    /// accumulator mutex. That poisons the mutex. This method reports
    /// the poisoning as a hard error, instead of silently dropping samples.
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
            // The per-task `TaskAccum` mutex is locked only here and inside
            // `send_loop`. A poisoned mutex means a sender task panicked
            // mid-iteration. This is a real bug: report it, do not drop samples.
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

    /// A convenience method that runs `prepare`, then `warmup`, then
    /// `dispatch`. A caller that needs finer control, such as the
    /// in-process harness that flips flame and pprof gates between
    /// warmup and dispatch, should call the stages one by one.
    ///
    /// # Errors
    ///
    /// Forwards errors from `workflow.prepare`, such as chain-state
    /// checks, signer derivation, and presigning, and from
    /// `Benchmark::warmup` and `Benchmark::dispatch`.
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
    // This function uses no per-task semaphore. Each sender task waits for
    // one in-flight request before it sends the next, so per-task in-flight
    // count is always 1. The `max_in_flight` setting lives on the HTTP
    // client instead. The harness sets
    // `max_concurrent_requests(max_in_flight + MAX_IN_FLIGHT_SLACK)`
    // as the runtime-wide budget across all sender tasks.
    for item in work {
        let t0 = Instant::now();
        let (method, ok) = workflow.dispatch(&client, item).await;
        // `as_micros` returns a `u128`. Saturate to `u64`: this only
        // matters for a dispatch that takes over 584,500 years.
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
        // This client is never used. The `prepare` zero-value guards fail
        // before the code touches the client.
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
