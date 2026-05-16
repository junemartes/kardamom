//! Metric names, labels, histogram buckets, and the `stage!` / `instrument_method!`
//! macros. One instrumentation site, two sinks: tracing-flame and Prometheus.

use std::time::Instant;

/// Counter: incremented once per RPC call.
pub const REQUESTS_TOTAL: &str = "kardamom_rpc_requests_total";

/// Histogram: handler-level duration (entry -> exit).
pub const RPC_DURATION_SECONDS: &str = "kardamom_rpc_duration_seconds";

/// Histogram: per-stage duration inside a handler.
pub const RPC_STAGE_DURATION_SECONDS: &str = "kardamom_rpc_stage_duration_seconds";

/// Gauge: the current block number.
pub const BLOCK_NUMBER: &str = "kardamom_block_number";

/// Gauge: build info, always 1, labeled with version + git_sha.
pub const BUILD_INFO: &str = "kardamom_build_info";

/// Shared bucket boundaries (seconds) for every duration histogram.
/// 100 µs → 5 s, exponential.
pub const DURATION_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
];

/// RAII guard that records a histogram observation for a single stage when
/// dropped. Used by the `stage!` macro so `?` inside the instrumented block
/// still records the elapsed time on early return.
pub struct StageGuard {
    start: Instant,
    method: &'static str,
    stage: &'static str,
}

impl StageGuard {
    #[inline]
    pub fn new(method: &'static str, stage: &'static str) -> Self {
        Self {
            start: Instant::now(),
            method,
            stage,
        }
    }
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        ::metrics::histogram!(
            RPC_STAGE_DURATION_SECONDS,
            "method" => self.method,
            "stage" => self.stage,
        )
        .record(elapsed);
    }
}

/// RAII guard for handler-level instrumentation. The outcome is recorded by
/// the macro after the body finishes, not by Drop — we need the result to
/// decide ok/err.
pub struct MethodTimer {
    pub start: Instant,
}

impl MethodTimer {
    #[inline]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    #[inline]
    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}

impl Default for MethodTimer {
    fn default() -> Self {
        Self::new()
    }
}

/// Record a single observation against the per-method handler-level histogram
/// and increment the request counter. Used by the `instrument_method!` macro.
#[inline]
pub fn record_method(method: &'static str, ok: bool, elapsed_secs: f64) {
    let outcome = if ok { "ok" } else { "err" };
    ::metrics::counter!(
        REQUESTS_TOTAL,
        "method" => method,
        "outcome" => outcome,
    )
    .increment(1);
    ::metrics::histogram!(
        RPC_DURATION_SECONDS,
        "method" => method,
        "outcome" => outcome,
    )
    .record(elapsed_secs);
}

/// Trait used by `instrument_method!` to classify the handler's result as
/// ok/err for the outcome label. Implemented for any `Result`.
pub trait IsOk {
    fn is_ok_outcome(&self) -> bool;
}

impl<T, E> IsOk for Result<T, E> {
    #[inline]
    fn is_ok_outcome(&self) -> bool {
        self.is_ok()
    }
}

/// Enter a tracing span and record per-stage histogram for the duration of a
/// sync block. Both `tracing-flame` (via the span) and Prometheus (via the
/// histogram) observe exactly the same boundaries.
///
/// Use `stage_await!` instead when the body must `.await` — `Entered` guards
/// cannot cross await points.
///
/// ```ignore
/// stage!("execute", method = "eth_sendRawTransaction", {
///     evm.transact_commit(tx)?
/// })
/// ```
#[macro_export]
macro_rules! stage {
    ($stage:expr, method = $method:expr, $body:block) => {{
        let _stage_span = ::tracing::info_span!($stage, method = $method).entered();
        let _stage_guard = $crate::metrics::StageGuard::new($method, $stage);
        $body
    }};
}

/// Async analogue of `stage!`. Instruments a single future with a span and
/// records the per-stage histogram for the duration of the `.await`. The
/// argument is the future to await, not a block — e.g.
/// `stage_await!("acquire_write_lock", method = METHOD, self.inner.write())`.
#[macro_export]
macro_rules! stage_await {
    ($stage:expr, method = $method:expr, $fut:expr) => {{
        let _stage_guard = $crate::metrics::StageGuard::new($method, $stage);
        ::tracing::Instrument::instrument($fut, ::tracing::info_span!($stage, method = $method))
            .await
    }};
}

/// Wrap an entire handler body, recording the request counter and handler
/// histogram with an `outcome` label. The body must evaluate to a `Result`.
///
/// Does not enter a tracing span: per-stage spans inside the body already
/// cover the flame view, and an entered span here would not be `Send` across
/// the body's `.await` points.
///
/// ```ignore
/// instrument_method!("eth_chainId", { Ok(U256::from(self.node.chain_id())) })
/// ```
#[macro_export]
macro_rules! instrument_method {
    ($method:expr, $body:block) => {{
        let _method_timer = $crate::metrics::MethodTimer::new();
        // Wrap the body in an `async {}` so `?` returns from the inner block
        // — not the enclosing handler — and the outcome is observable to the
        // metrics recorder.
        let result = async $body.await;
        let ok = $crate::metrics::IsOk::is_ok_outcome(&result);
        $crate::metrics::record_method($method, ok, _method_timer.elapsed_secs());
        result
    }};
}

/// Update the block-number gauge. Called from inside `submit_raw_transaction`
/// on each successful commit.
#[inline]
pub fn set_block_number(n: u64) {
    ::metrics::gauge!(BLOCK_NUMBER).set(n as f64);
}
