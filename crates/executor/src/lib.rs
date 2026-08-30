//! Kardamom v0 sequential executor: the sequencer-side role.
//!
//! Layering: `exec-core` (pure state transition), then [`kardamom_engine`]
//! (role-agnostic runtime), then this crate (the executor role). This
//! crate holds the `kardamom-executor` binary, the BAL publication
//! ([`bal`]), the Block-STM strategy ([`parallel`]), and the file config
//! ([`config`]). It does not re-export engine types. Use
//! `kardamom_engine::...` paths instead.
//!
//! ## Divergence detection
//!
//! Two replicas can publish a `Receipt` with the same `tx_idx` but a
//! different `write_set_hash`. This must halt the chain. The executor cannot
//! detect this from its own output. The tx_receipts consumer detects it: it
//! dedupes by `tx_idx` and panics on a hash mismatch (see `kardamom-log`).
//! Separately, a `kardamom-validator` re-executes the canonical order and
//! checks the result against the executor's receipts and BAL.

/// Executor binary file config (`executor.toml`). It holds role-specific
/// deploy settings (cluster egress endpoint, mdbx path, and more). It stays
/// in this crate because the engine is config-agnostic.
pub mod config;

pub use config::ExecutorFileConfig;

/// Executor-side BAL publication. This is sequencer-role behavior layered on
/// the shared engine: it publishes each block's `BlockDelta` on `tx_bal`.
pub mod bal;
pub mod parallel;
