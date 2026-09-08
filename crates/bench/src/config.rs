//! This module has the default values for both binaries' clap setup.
//!
//! Workload-shape config, such as mnemonic, contract bytecode, and mix ratio,
//! lives in the `BenchWorkflow` implementations in `crate::workflows`. This
//! module holds only constants.

use std::num::NonZeroU32;
use std::time::Duration;

use alloy_primitives::U256;
use hdrhistogram::Histogram;
use jsonrpsee::core::client::ClientT;
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::rpc_params;

/// Default `--timeout` value for the CLI, when the user sets no value.
/// The CLI applies this to the warmup and dispatch phases on their own.
/// It gives a useful measurement window without a long unattended run.
pub(crate) const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default number of sender tasks. Each task uses one signer.
pub const DEFAULT_CONCURRENCY: NonZeroU32 = NonZeroU32::new(16).unwrap();

/// Default number of pre-signed transactions in the queue of each sender task.
pub const DEFAULT_TXS_PER_TASK: NonZeroU32 = NonZeroU32::new(10_000).unwrap();

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

/// The shared latency-histogram bounds: 1 microsecond to 60 seconds,
/// 3 significant figures. `load::Tracker` and `Benchmark` both build a
/// histogram at these bounds.
pub const HIST_LOW_US: u64 = 1;
pub const HIST_HIGH_US: u64 = 60_000_000;
pub const HIST_SIGFIGS: u8 = 3;

/// A latency histogram at the shared bounds.
///
/// # Errors
///
/// Returns an error if the bounds are invalid (they are fixed
/// constants here, so this does not happen in practice).
pub fn new_latency_hist() -> anyhow::Result<Histogram<u64>> {
    Ok(Histogram::new_with_bounds(
        HIST_LOW_US,
        HIST_HIGH_US,
        HIST_SIGFIGS,
    )?)
}

/// Build the jsonrpsee HTTP client every binary uses: [`REQUEST_TIMEOUT`]
/// and a concurrency budget of `max_in_flight + MAX_IN_FLIGHT_SLACK`.
///
/// # Errors
///
/// Returns an error if `url` does not parse as a valid client endpoint.
pub fn rpc_client(url: &str, max_in_flight: u32) -> anyhow::Result<HttpClient> {
    Ok(HttpClientBuilder::default()
        .request_timeout(REQUEST_TIMEOUT)
        .max_concurrent_requests((max_in_flight as usize).saturating_add(MAX_IN_FLIGHT_SLACK))
        .build(url)?)
}

/// Read the chain ID with `eth_chainId`.
///
/// # Errors
///
/// Returns an error if the request fails, or if the result does not
/// fit in a `u64`.
pub async fn preflight_chain_id(client: &HttpClient) -> anyhow::Result<u64> {
    let v: U256 = client
        .request("eth_chainId", rpc_params![])
        .await
        .map_err(|e| {
            anyhow::anyhow!("eth_chainId failed (is the node running at the --rpc url?): {e}")
        })?;
    v.try_into()
        .map_err(|e| anyhow::anyhow!("chain_id overflow: {e}"))
}

/// The four settings every `Benchmark` needs, shared as one flattened
/// clap group by `kardamom-bench` and `kardamom-bench-harness`.
#[derive(clap::Args, Debug)]
pub struct BenchArgs {
    /// A safety timeout for each phase. Warmup and dispatch each get
    /// their own timeout. A sender also stops when its work vector is
    /// drained, whichever comes first.
    #[arg(long, value_parser = humantime::parse_duration, default_value = DEFAULT_TIMEOUT_STR)]
    pub timeout: Duration,

    /// The number of sender tasks. This equals the number of derived
    /// signers, one per task. Non-zero: the type rejects 0 at parse.
    #[arg(long, default_value_t = DEFAULT_CONCURRENCY)]
    pub concurrency: NonZeroU32,

    /// The number of pre-signed transactions in the queue of each
    /// sender task. Non-zero: the type rejects 0 at parse.
    #[arg(long = "txs-per-task", default_value_t = DEFAULT_TXS_PER_TASK)]
    pub txs_per_task: NonZeroU32,

    /// The limit on outstanding requests for each sender task.
    #[arg(long = "max-in-flight", default_value_t = DEFAULT_MAX_IN_FLIGHT)]
    pub max_in_flight: u32,
}
