//! Cross-component end-to-end test harness for the kardamom rollup pipeline.
//!
//! This crate hosts the chain-semantics e2e suite
//! (`docs/agents/chain-semantics-e2e-suite-spec.md`). It has target-agnostic
//! scenario drivers. The drivers prove the rollup's semantics through
//! external seams: the ingress JSON-RPC and the per-service `/metrics`.
//! The crate also has the Target-L local stack. This stack runs the real
//! service binaries against a real Java Aeron Cluster sealer on one host.
//!
//! ## Layout
//!
//! - `src/harness/` — the Target-L local stack. It has an
//!   ArchivingMediaDriver, a 1-member `ClusterNode` JVM, and
//!   `kardamom-{ingress,sequencer,executor}` child processes on per-test
//!   temp dirs. It also has the RPC and metrics clients.
//! - `src/scenarios/` — the scenario drivers (nonce ordering, nonce gaps,
//!   and RPC liveness today). They talk only to a [`scenarios::Target`],
//!   not to harness internals. This lets the Target-C (`ci-cluster.sh`
//!   DinD) runner reuse them unchanged.
//! - `tests/chain_semantics.rs` — binds the scenarios to the local stack.
//!   The `full-pipeline-e2e` feature and `#[ignore]` gate these tests, so
//!   default `cargo test` skips them. `just test-e2e-local` and the
//!   chain-semantics CI job opt in.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod pipeline;

#[cfg(feature = "full-pipeline-e2e")]
pub mod harness;
#[cfg(feature = "full-pipeline-e2e")]
pub mod scenarios;
