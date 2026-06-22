//! Kardamom S4 v0 sequential executor — the **sequencer-side role**.
//!
//! The execution core now lives in [`kardamom_engine`]; this crate is the
//! sequencer role (the `kardamom-executor` binary) plus a thin re-export of the
//! engine API so existing `kardamom_executor::...` paths keep resolving.
//!
//! ## Divergence detection
//!
//! Two replicas publishing a `Receipt` with the same `tx_idx` but different
//! `write_set_hash` must halt the chain. The executor cannot detect this from
//! its own output; the detection point is the **tx_receipts consumer** that
//! dedupes by `tx_idx` and panics on hash mismatch (lives in `kardamom-log`).
//! Independently, a `kardamom-validator` re-executes the canonical order and
//! cross-checks against the executor's receipts + BAL.

// Flat API (types, traits, structs) — same names the crate exposed before the
// engine extraction.
pub use kardamom_engine::*;

// Re-export the engine's modules so path-qualified references keep working
// (`kardamom_executor::metrics::describe()`, tests' `kardamom_executor::executor::*`,
// the throughput bench's `block_env` / `delta`, etc.).
pub use kardamom_engine::{
    actor, block_env, delta, error, exec_types, executor, metrics, persist, reader, state,
};
