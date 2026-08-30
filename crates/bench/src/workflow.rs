//! The `BenchWorkflow` trait is the plug-in point for this crate.
//!
//! A `BenchWorkflow` knows three things:
//! - the genesis state it expects. This is descriptive only: external
//!   harnesses provision it, and the in-process ingress stand-in ignores it.
//! - how to build its per-task work vectors against a live RPC client. This
//!   covers signer derivation, presigning, and preflight checks.
//! - how to dispatch one item and report the histogram bucket for the
//!   timing sample.
//!
//! `Benchmark<W: BenchWorkflow>` is generic over this trait. An external
//! crate can implement its own workflow and run a benchmark by
//! constructing a `Benchmark` and calling `.run(client)`.

use jsonrpsee::http_client::HttpClient;
use kardamom_types::AllocEntry;

use crate::benchmark::Prepared;

/// A workload definition: the genesis state to set up, work-vector
/// preparation against a live client, and per-item dispatch.
/// This is the generic plug-in point for [`crate::Benchmark`].
pub trait BenchWorkflow: Clone + Send + Sync + 'static {
    /// The unit of work each sender task loops over.
    type Item: Send + 'static;

    /// A human-readable label for the report. It does not have to be
    /// unique across crates. It only needs to be informative.
    fn name(&self) -> &'static str;

    /// The histogram bucket keys this workflow emits in `dispatch`. The
    /// dispatcher uses this slice to pre-allocate one `Histogram` per
    /// method per task, so recording in each iteration is a map lookup,
    /// not an insert. Keep the strings static, so callers can compare
    /// them by pointer.
    fn methods(&self) -> &'static [&'static str];

    /// The genesis allocations this workflow expects on the target chain:
    /// prefunded signer EOAs and deployed contracts.
    ///
    /// No runtime path consumes this value now, because `kardamom-node`
    /// is removed and the in-process ingress stand-in accepts submissions
    /// without balance checks. The value stays descriptive: an external
    /// harness, including a future full-pipeline harness, can use it to
    /// set up chain state. A workflow that leaves chain state to the
    /// caller returns `Ok(vec![])`.
    ///
    /// This method can fail, so a workflow that derives accounts from a
    /// mnemonic can report a bad-phrase error instead of a panic.
    ///
    /// # Errors
    ///
    /// The error depends on the workflow. Built-in workflows fail on a
    /// bad mnemonic phrase or an invalid BIP-32 derivation index.
    fn genesis_alloc(&self, n_tasks: u32) -> anyhow::Result<Vec<AllocEntry>>;

    /// Run preflight checks against a live client and build per-task work
    /// vectors. The harness calls this once, before warmup and dispatch.
    /// All cryptography (signing, key derivation), chain-state checks,
    /// and scheduling logic live here.
    ///
    /// Returns a [`Prepared`] value with two vectors, one entry per task:
    /// - `warmup`: work items the harness runs without metering, before
    ///   metering starts. This warms the JIT, caches, and one-shot
    ///   allocations.
    /// - `main`: the metered dispatch work, with `txs_per_task` items
    ///   per task.
    ///
    /// The workflow chooses its own warmup volume. This is usually a
    /// small fixed value per task. In a workload where warmup transaction
    /// state must match main transaction state, for example when
    /// transfers use up nonces, the workflow must order them correctly
    /// itself.
    fn prepare(
        &self,
        client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> impl std::future::Future<Output = anyhow::Result<Prepared<Self::Item>>> + Send;

    /// Dispatch one item against the RPC. Returns the histogram bucket key,
    /// which must be one of `self.methods()`, and whether the call
    /// succeeded. The dispatcher times the call outside this method.
    /// Do not measure or record timing inside this method.
    fn dispatch(
        &self,
        client: &HttpClient,
        item: Self::Item,
    ) -> impl std::future::Future<Output = (&'static str, bool)> + Send;
}

/// Helper for the built-in workflows: 1000 ETH in wei, as a
/// runtime-computed `U256`. Alloy's `pow` is not a const function.
///
/// This is a convenient default prefunding amount for derived signers.
#[must_use]
pub fn default_signer_balance() -> alloy_primitives::U256 {
    alloy_primitives::U256::from(10u64).pow(alloy_primitives::U256::from(21u64))
}
