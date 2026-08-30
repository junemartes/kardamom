//! Kardamom v0 sequential executor: the sequencer-side role.
//!
//! The execution core lives in [`kardamom_engine`]. This crate holds the
//! sequencer role (the `kardamom-executor` binary) and re-exports the engine
//! API, so existing `kardamom_executor::...` paths still resolve.
//!
//! ## Divergence detection
//!
//! Two replicas can publish a `Receipt` with the same `tx_idx` but a
//! different `write_set_hash`. This must halt the chain. The executor cannot
//! detect this from its own output. The tx_receipts consumer detects it: it
//! dedupes by `tx_idx` and panics on a hash mismatch (see `kardamom-log`).
//! Separately, a `kardamom-validator` re-executes the canonical order and
//! checks the result against the executor's receipts and BAL.

// Flat API (types, traits, structs). These are the same names the crate used
// before the engine extraction.
pub use kardamom_engine::*;

/// Executor binary file config (`executor.toml`). It holds role-specific
/// deploy settings (cluster egress endpoint, mdbx path, and more). It stays
/// in this crate because the engine is config-agnostic.
pub mod config;

// Re-export the engine's modules. This keeps path-qualified references
// working (for example `kardamom_executor::metrics::describe()`, and the
// throughput bench's `block_env` and `delta`).
pub use config::ExecutorFileConfig;
pub use kardamom_engine::{
    actor, block_env, delta, error, exec_types, executor, metrics, persist, reader, state,
};

/// Executor-side BAL publication. This is sequencer-role behavior layered on
/// the shared engine: it publishes each block's `BlockDelta` on `tx_bal`.
pub mod bal;
pub mod parallel;
