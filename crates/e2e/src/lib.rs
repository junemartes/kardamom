//! Cross-component end-to-end test harness for the kardamom rollup pipeline.
//!
//! This crate hosts the **proof-of-pipeline** test: a single integration test
//! that brings up real Aeron in Docker, wires every kardamom subsystem
//! against it, sends txs through the pipeline, and asserts the txs surface
//! correctly at every downstream subsystem (tx_ordering, tx_receipts,
//! libmdbx state DB, L1 blob payload).
//!
//! ## Layout
//!
//! - `src/lib.rs` — this file. Re-exports a small "pipeline builder"
//!   convenience for the test file.
//! - `tests/full_pipeline_e2e.rs` — the integration test. Gated behind the
//!   `full-pipeline-e2e` feature so default `cargo test` doesn't pull
//!   testcontainers, rusteron, libmdbx, etc.

#![deny(unsafe_op_in_unsafe_fn)]

pub mod pipeline;

/// RPC-driven scenarios for the `cluster-e2e` binary: signing helpers + the
/// transfer / deposit / contract-deploy flows that drive a *deployed* nomad
/// cluster over its ingress JSON-RPC (and the in-cluster anvil L1). Gated
/// behind `cluster-e2e` so the heavy alloy/jsonrpsee/da-watcher deps only
/// compile when the client binary is built.
#[cfg(feature = "cluster-e2e")]
pub mod cluster_client;
