//! Cross-component end-to-end test harness for the kardamom rollup pipeline.
//!
//! This crate hosts the **chain-semantics** e2e suite
//! (`docs/agents/chain-semantics-e2e-suite-spec.md`): target-agnostic
//! scenario drivers that prove the rollup's semantics through externally
//! observable seams (ingress JSON-RPC, per-service /metrics), plus the
//! Target-L local stack that runs the real service binaries against a real
//! Java Aeron Cluster sealer on one host.
//!
//! ## Layout
//!
//! - `src/harness/` — the Target-L local stack: ArchivingMediaDriver + a
//!   1-member `ClusterNode` JVM + `kardamom-{ingress,sequencer,executor}`
//!   child processes on per-test temp dirs, and the RPC/metrics clients.
//! - `src/scenarios/` — the scenario drivers (S3/S4/S5 today). They talk
//!   only to a [`scenarios::Target`], never to harness internals, so the
//!   Target-C (`ci-cluster.sh` DinD) runner can reuse them unchanged.
//! - `tests/chain_semantics.rs` — binds the scenarios to the local stack.
//!   Gated behind the `full-pipeline-e2e` feature AND `#[ignore]` so default
//!   `cargo test` skips them; `just test-e2e-local` / the chain-semantics
//!   CI job opt in.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod pipeline;

#[cfg(feature = "full-pipeline-e2e")]
pub mod harness;
#[cfg(feature = "full-pipeline-e2e")]
pub mod scenarios;
