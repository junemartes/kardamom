//! This module has the default values for both binaries' clap setup.
//!
//! Workload-shape config, such as mnemonic, contract bytecode, and mix ratio,
//! lives in the `BenchWorkflow` implementations in `crate::workflows`. This
//! module holds only constants.

use std::time::Duration;

/// Default `--timeout` value for the CLI, when the user sets no value.
/// The CLI applies this to the warmup and dispatch phases on their own.
/// It gives a useful measurement window without a long unattended run.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default number of sender tasks. Each task uses one signer.
pub const DEFAULT_CONCURRENCY: u32 = 16;

/// Default number of pre-signed transactions in the queue of each sender task.
pub const DEFAULT_TXS_PER_TASK: u32 = 10_000;

/// Default limit on outstanding requests across all senders.
/// The HTTP client layer applies this as
/// `max_concurrent_requests = max_in_flight + MAX_IN_FLIGHT_SLACK`.
pub const DEFAULT_MAX_IN_FLIGHT: u32 = 5;

/// String form of [`DEFAULT_TIMEOUT`] for `clap`'s `default_value`.
/// Clap needs a `&str` value. `humantime::parse_duration` then parses it.
pub const DEFAULT_TIMEOUT_STR: &str = "10s";

/// Request timeout for the jsonrpsee client.
/// This is long enough that the slowest expected in-process call, a
/// contended write transaction, does not trip it.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Slack between `Benchmark.max_in_flight`, the user-facing setting, and
/// the jsonrpsee client's `max_concurrent_requests`.
///
/// The extra value covers preflight and post-dispatch RPCs. The bench
/// budget does not count these RPCs.
pub const MAX_IN_FLIGHT_SLACK: usize = 16;

/// `pprof` sampling frequency in Hz.
/// Use 999, not 1000, to avoid resonance with the kernel tick or
/// scheduler quantum on most platforms.
pub const PPROF_HZ: i32 = 999;
