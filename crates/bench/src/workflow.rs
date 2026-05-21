//! The `BenchWorkflow` trait — the unit of pluggability for this crate.
//!
//! A `BenchWorkflow` knows three things:
//! - what genesis state it needs (for the in-process harness),
//! - how to assemble its per-task work vecs against a live RPC client
//!   (signer derivation, presigning, preflight checks),
//! - how to dispatch one item and report which histogram bucket the timing
//!   sample belongs in.
//!
//! `Benchmark<W: BenchWorkflow>` is generic over this trait, so external
//! crates can implement their own workflow and run a benchmark by
//! constructing a `Benchmark` and calling `.run(client)`.

use jsonrpsee::http_client::HttpClient;
use kardamom_node::AllocEntry;

/// One workload definition: genesis state to set up, work-vec preparation
/// against a live client, and per-item dispatch. Generic plug point for
/// [`crate::Benchmark`].
pub trait BenchWorkflow: Clone + Send + Sync + 'static {
    /// The unit of work each sender task loops over.
    type Item: Send + 'static;

    /// Human-readable label for the report. Doesn't have to be unique across
    /// crates, just informative.
    fn name(&self) -> &'static str;

    /// Histogram bucket keys this workflow emits in `dispatch`. The
    /// dispatcher pre-allocates one `Histogram` per method per task using
    /// this slice, so per-iteration recording is a map lookup, not an
    /// insert. Keep the strings static so callers can compare by pointer.
    fn methods(&self) -> &'static [&'static str];

    /// Genesis allocs needed by this workflow on an in-process node:
    /// prefunded signer EOAs and deployed contracts. Workflows that target
    /// a remote node (caller is responsible for chain state) return
    /// `Ok(vec![])`. Fallible so workflows that derive accounts from a
    /// mnemonic can surface a bad-phrase error instead of panicking.
    ///
    /// # Errors
    ///
    /// Workflow-specific. Built-in workflows error on a bad mnemonic
    /// phrase or invalid BIP-32 derivation index.
    fn genesis_alloc(&self, n_tasks: u32) -> anyhow::Result<Vec<AllocEntry>>;

    /// Preflight against a live client and build per-task work vecs. Called
    /// once before dispatch — all crypto (signing, key derivation), all
    /// chain-state probes, and all scheduling logic live here. Returns one
    /// inner `Vec<Self::Item>` per sender task (`n_tasks` outer entries).
    fn prepare(
        &self,
        client: &HttpClient,
        n_tasks: u32,
        txs_per_task: u32,
    ) -> impl std::future::Future<Output = anyhow::Result<Vec<Vec<Self::Item>>>> + Send;

    /// Dispatch one item against the RPC. Returns the histogram bucket key
    /// (must be one of `self.methods()`) and whether the call succeeded.
    /// The dispatcher times the call externally; this method should NOT
    /// measure or record anything itself.
    fn dispatch(
        &self,
        client: &HttpClient,
        item: Self::Item,
    ) -> impl std::future::Future<Output = (&'static str, bool)> + Send;
}

/// Helper used by the built-in workflows: 1000 ETH in wei, as a
/// runtime-computed `U256` (alloy's `pow` isn't const).
///
/// Convenient default prefunding amount for derived signers.
#[must_use]
pub fn default_signer_balance() -> alloy_primitives::U256 {
    alloy_primitives::U256::from(10u64).pow(alloy_primitives::U256::from(21u64))
}
