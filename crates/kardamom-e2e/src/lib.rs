//! Cross-component end-to-end test harness for the kardamom rollup pipeline.
//!
//! This crate hosts the **proof-of-pipeline** test: a single integration test
//! that brings up real Aeron in Docker, wires every kardamom subsystem (S1–S7)
//! against it, sends txs through the proxy, and asserts the txs surface
//! correctly at every downstream subsystem (channel B, channel C / receipts,
//! libmdbx state DB, L1 blob payload).
//!
//! Per S0 D-Sh8 e2e tests **must** use real Aeron — mocks are not acceptable
//! at this layer. The mock-based unit and integration tests inside each
//! component crate are unaffected; this crate is **additional** coverage that
//! catches wire-format, IPC, back-pressure, and cross-component sequencing
//! bugs the mocks cannot surface.
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
