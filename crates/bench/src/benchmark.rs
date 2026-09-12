//! `Benchmark<W>` is the dispatcher. It is generic over a `BenchWorkflow`.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use jsonrpsee::http_client::HttpClient;

use crate::config::{
    DEFAULT_CONCURRENCY, DEFAULT_MAX_IN_FLIGHT, DEFAULT_TIMEOUT, DEFAULT_TXS_PER_TASK,
    HIST_HIGH_US, HIST_LOW_US,
};
use crate::report::Counters;
use crate::workflow::{BenchWorkflow, DispatchOutcome};

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
    /// their allocation set. Non-zero: a 0-task run produces no samples.
    pub concurrency: NonZeroU32,
    /// The number of pre-signed transactions in the queue of each sender
    /// task. The run attempts a total of `txs_per_task * concurrency`
    /// items of work. Non-zero: a 0-item queue produces no samples.
    pub txs_per_task: NonZeroU32,
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
    /// presigning. `concurrency` and `txs_per_task` are `NonZeroU32`,
    /// so a 0-task or 0-item run, which would otherwise silently
    /// produce zero samples, is a construction-time type error instead.
    pub async fn prepare(&self, client: &HttpClient) -> anyhow::Result<Prepared<W::Item>> {
        self.workflow
            .prepare(client, self.concurrency.get(), self.txs_per_task.get())
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

    /// Spawn one sender task for `work`: race [`TaskCounts::send_all`]
    /// against `self.timeout`, and return the task's owned counts
    /// either way (a timeout drops only the in-flight `send_all`
    /// future; every result already folded in survives).
    fn spawn_worker(
        &self,
        work: Vec<W::Item>,
        client: &Arc<HttpClient>,
        workflow: &Arc<W>,
        methods: &[&'static str],
    ) -> anyhow::Result<tokio::task::JoinHandle<TaskCounts>> {
        let client = Arc::clone(client);
        let workflow = Arc::clone(workflow);
        let timeout = self.timeout;
        let counts = TaskCounts {
            ok: 0,
            err: 0,
            histograms: empty_histograms(methods)?,
        };
        Ok(tokio::spawn(
            counts.race_send_all(workflow, client, work, timeout),
        ))
    }

    /// Stage 3: the measured window. This method spawns one sender task
    /// per work vector, inside a `tokio::time::timeout(self.timeout, ...)`.
    /// Each sender loops over its vector in order, so each task has one
    /// request in flight at a time. The HTTP client layer enforces the
    /// runtime-wide `max_in_flight` budget, as
    /// `max_concurrent_requests = max_in_flight + MAX_IN_FLIGHT_SLACK`,
    /// not this method.
    /// Each sender task owns its counts and histograms locally, and
    /// returns them from its `JoinHandle` when it finishes. A timeout
    /// drops only the in-flight `send_all` future; the task's owned
    /// state, already updated for every completed request, survives
    /// and is still returned.
    ///
    /// # Errors
    ///
    /// Returns an error if histogram allocation fails, if a sender task
    /// panics (the join handle forwards the panic), or if histogram
    /// merging finds a unit mismatch. A unit mismatch cannot happen with
    /// the bounds set here, but the method reports it for completeness.
    pub async fn dispatch(
        &self,
        client: HttpClient,
        main: Vec<Vec<W::Item>>,
    ) -> anyhow::Result<Outputs> {
        let methods = self.workflow.methods();
        let workflow = Arc::new(self.workflow.clone());
        let client = Arc::new(client);

        let start = Instant::now();

        let handles: Vec<tokio::task::JoinHandle<TaskCounts>> = main
            .into_iter()
            .map(|work| self.spawn_worker(work, &client, &workflow, methods))
            .collect::<anyhow::Result<_>>()?;

        let mut task_counts = Vec::with_capacity(handles.len());
        for h in handles {
            task_counts.push(h.await.map_err(|e| anyhow::anyhow!("task join: {e}"))?);
        }

        let measurement_duration = start.elapsed();

        let mut counters = Counters {
            sent: 0,
            ok: 0,
            err: 0,
        };
        let mut per_task: Vec<BTreeMap<String, Histogram<u64>>> =
            Vec::with_capacity(task_counts.len());
        for c in task_counts {
            counters.fold(c, &mut per_task);
        }
        counters.sent = counters.ok + counters.err;

        let mut merged = empty_histograms(methods)?;
        for task_hist in &per_task {
            merge_task_histograms(&mut merged, task_hist)?;
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

    /// Build a [`crate::report::BenchReport`] from this run's settings
    /// and `outputs`. Both callers of `run`, the closed-loop binary and
    /// the in-process harness, report the same five settings fields
    /// this way.
    pub fn report(&self, outputs: Outputs) -> crate::report::BenchReport {
        crate::report::build_report(
            crate::report::ReportInputs {
                workload_name: self.workflow.name(),
                txs_per_task: self.txs_per_task.get(),
                max_in_flight: self.max_in_flight,
                concurrency: self.concurrency.get(),
                configured_timeout: self.timeout,
            },
            &outputs.counters,
            outputs.histograms,
            outputs.measurement_duration,
        )
    }
}

impl Counters {
    /// Fold one task's counts into `self`'s running totals, and move
    /// its histograms into `per_task`.
    fn fold(&mut self, counts: TaskCounts, per_task: &mut Vec<BTreeMap<String, Histogram<u64>>>) {
        self.ok += counts.ok;
        self.err += counts.err;
        per_task.push(counts.histograms);
    }
}

/// One sender task's owned result: counts and per-method latency
/// histograms. Returned from the task's `JoinHandle`, instead of
/// shared through a mutex.
struct TaskCounts {
    ok: u64,
    err: u64,
    histograms: BTreeMap<String, Histogram<u64>>,
}

impl TaskCounts {
    /// Race [`Self::send_all`] against `timeout`, and return `self`
    /// either way.
    ///
    /// A timeout drops only the in-flight `send_all` future; every
    /// result already folded into `self` survives and is still
    /// returned.
    async fn race_send_all<W: BenchWorkflow>(
        mut self,
        workflow: Arc<W>,
        client: Arc<HttpClient>,
        work: Vec<W::Item>,
        timeout: Duration,
    ) -> Self {
        tokio::select! {
            () = self.send_all(&*workflow, &client, work) => {}
            () = tokio::time::sleep(timeout) => {}
        }
        self
    }

    /// Send every item in `work`, in order, one request in flight at a
    /// time, and fold each result into `self`.
    ///
    /// This method uses no per-task semaphore. Each sender task waits
    /// for one in-flight request before it sends the next, so per-task
    /// in-flight count is always 1. The `max_in_flight` setting lives
    /// on the HTTP client instead. The harness sets
    /// `max_concurrent_requests(max_in_flight + MAX_IN_FLIGHT_SLACK)`
    /// as the runtime-wide budget across all sender tasks.
    ///
    /// The caller races this against a timeout. A cancelled call drops
    /// this future mid-request, but `self` holds every result already
    /// folded in, so the caller keeps those samples regardless of
    /// which side of the race wins.
    async fn send_all<W: BenchWorkflow>(
        &mut self,
        workflow: &W,
        client: &HttpClient,
        work: Vec<W::Item>,
    ) {
        for item in work {
            self.dispatch_one(workflow, client, item).await;
        }
    }

    /// Dispatch one item, and fold its outcome and elapsed time into
    /// `self`.
    async fn dispatch_one<W: BenchWorkflow>(
        &mut self,
        workflow: &W,
        client: &HttpClient,
        item: W::Item,
    ) {
        let t0 = Instant::now();
        let outcome = workflow.dispatch(client, item).await;
        // `as_micros` returns a `u128`. Saturate to `u64`: this only
        // matters for a dispatch that takes over 584,500 years.
        let elapsed_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.record(outcome, elapsed_us);
    }

    /// Fold one dispatch outcome into this task's counts and its
    /// method's latency histogram.
    fn record(&mut self, outcome: DispatchOutcome, elapsed_us: u64) {
        if outcome.success {
            self.ok += 1;
        } else {
            self.err += 1;
        }
        if let Some(hist) = self.histograms.get_mut(outcome.method) {
            let _ = hist.record(elapsed_us.clamp(HIST_LOW_US, HIST_HIGH_US));
        }
    }
}

/// An empty histogram for each method, at the run's fixed bounds.
fn empty_histograms(methods: &[&'static str]) -> anyhow::Result<BTreeMap<String, Histogram<u64>>> {
    methods
        .iter()
        .map(|m| {
            crate::config::new_latency_hist()
                .map(|h| ((*m).to_string(), h))
                .map_err(|e| anyhow::anyhow!("hist init: {e}"))
        })
        .collect()
}

/// Merge one task's per-method histograms into `merged`.
fn merge_task_histograms(
    merged: &mut BTreeMap<String, Histogram<u64>>,
    task_hist: &BTreeMap<String, Histogram<u64>>,
) -> anyhow::Result<()> {
    task_hist
        .iter()
        .try_for_each(|(k, h)| merge_one_histogram(merged, k, h))
}

/// Merge one method's histogram `h` into `merged[k]`, or do nothing if
/// `merged` has no entry for `k` yet.
fn merge_one_histogram(
    merged: &mut BTreeMap<String, Histogram<u64>>,
    k: &str,
    h: &Histogram<u64>,
) -> anyhow::Result<()> {
    let Some(m) = merged.get_mut(k) else {
        return Ok(());
    };
    m.add(h).map_err(|e| anyhow::anyhow!("hist merge: {e:?}"))
}

#[cfg(test)]
mod tests {
    use kardamom_types::AllocEntry;

    use super::*;

    /// A workflow with no genesis state and no network calls, so a test
    /// can exercise `Benchmark::prepare`'s NonZero-to-`u32` handoff
    /// without a live client.
    #[derive(Debug, Clone)]
    struct NoopWorkflow;

    impl BenchWorkflow for NoopWorkflow {
        type Item = ();

        fn name(&self) -> &'static str {
            "noop"
        }

        fn methods(&self) -> &'static [&'static str] {
            &[]
        }

        fn genesis_alloc(&self, _n_tasks: u32) -> anyhow::Result<Vec<AllocEntry>> {
            Ok(Vec::new())
        }

        fn prepare(
            &self,
            _client: &HttpClient,
            n_tasks: u32,
            txs_per_task: u32,
        ) -> impl std::future::Future<Output = anyhow::Result<Prepared<Self::Item>>> + Send
        {
            std::future::ready(Ok(Prepared {
                warmup: Vec::new(),
                main: (0..n_tasks)
                    .map(|_| vec![(); txs_per_task as usize])
                    .collect(),
            }))
        }

        fn dispatch(
            &self,
            _client: &HttpClient,
            (): (),
        ) -> impl std::future::Future<Output = DispatchOutcome> + Send {
            std::future::ready(DispatchOutcome {
                method: "noop",
                success: true,
            })
        }
    }

    /// `Benchmark::prepare` passes `concurrency.get()` and
    /// `txs_per_task.get()` straight through to the workflow: one task
    /// per sender, `txs_per_task` items in each.
    #[tokio::test]
    async fn prepare_passes_nonzero_concurrency_and_txs_per_task_through() {
        let bench = Benchmark {
            workflow: NoopWorkflow,
            timeout: Duration::from_secs(1),
            concurrency: NonZeroU32::new(3).expect("3 != 0"),
            txs_per_task: NonZeroU32::new(5).expect("5 != 0"),
            max_in_flight: 8,
        };
        let client = jsonrpsee::http_client::HttpClientBuilder::default()
            .build("http://127.0.0.1:1")
            .expect("client builds without connecting");
        let prepared = bench.prepare(&client).await.expect("prepare");
        assert_eq!(prepared.main.len(), 3, "one work vector per task");
        for task in &prepared.main {
            assert_eq!(task.len(), 5, "txs_per_task items in each task");
        }
    }
}
