//! Knob-style defaults shared by both binaries' clap setup.
//!
//! Workload-shape config — mnemonic, contract bytecode, mix ratio — lives
//! inside the individual `BenchWorkflow` implementations in
//! `crate::workflows`. This module is now just constants.

use std::time::Duration;

/// Default `--timeout` value used by the CLI when the user doesn't set
/// one. Long enough to cover a meaningful warmup + measurement window
/// without leaving the bench running unattended for minutes.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default `--warmup` window between `prepare` and `dispatch`. Pure sleep
/// — no senders run. Lets OS / CPU caches settle before measurement.
pub const DEFAULT_WARMUP: Duration = Duration::from_secs(2);

/// Default number of sender tasks (= one signer per task).
pub const DEFAULT_CONCURRENCY: u32 = 16;

/// Default number of pre-signed transactions queued per sender task.
pub const DEFAULT_TXS_PER_TASK: u32 = 10_000;

/// Default cap on outstanding requests across all senders. Applied at the
/// HTTP client layer (`max_concurrent_requests = max_in_flight + MAX_IN_FLIGHT_SLACK`).
pub const DEFAULT_MAX_IN_FLIGHT: u32 = 5;

/// String form of [`DEFAULT_TIMEOUT`] for `clap`'s `default_value`
/// (clap needs a `&str` that's then parsed by `humantime::parse_duration`).
pub const DEFAULT_TIMEOUT_STR: &str = "10s";

/// String form of [`DEFAULT_WARMUP`] for `clap`'s `default_value`.
pub const DEFAULT_WARMUP_STR: &str = "2s";

/// jsonrpsee client request timeout. Long enough that the slowest
/// expected in-process call (a contended write-tx) doesn't trip it.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Slack between `Benchmark.max_in_flight` (the user-facing knob) and
/// the jsonrpsee client's `max_concurrent_requests`.
///
/// The +N covers the preflight + post-dispatch RPCs that aren't counted
/// against the bench budget.
pub const MAX_IN_FLIGHT_SLACK: usize = 16;

/// `pprof` sampling frequency in Hz. 999 (not 1000) avoids resonance
/// with kernel tick / scheduler quantum on most platforms.
pub const PPROF_HZ: i32 = 999;
