//! Kardamom S4 v0 sequential executor — the **sequencer-side role**.
//!
//! Layering: `exec-core` (pure state transition) → [`kardamom_engine`]
//! (role-agnostic runtime) → this crate (the executor role). The crate
//! holds the `kardamom-executor` binary, the BAL publication ([`bal`]),
//! the Block-STM strategy ([`parallel`]), and the file config
//! ([`config`]). Engine types are NOT re-exported: use
//! `kardamom_engine::...` paths.
//!
//! ## Divergence detection
//!
//! Two replicas publishing a `Receipt` with the same `tx_idx` but different
//! `write_set_hash` must halt the chain. The executor cannot detect this from
//! its own output; the detection point is the **tx_receipts consumer** that
//! dedupes by `tx_idx` and panics on hash mismatch (lives in `kardamom-log`).
//! Independently, a `kardamom-validator` re-executes the canonical order and
//! cross-checks against the executor's receipts + BAL.

/// Executor **binary** file config (`executor.toml`): role-specific deploy
/// surface (cluster egress endpoint, mdbx path, …) — stays in this crate, the
/// engine is config-agnostic.
pub mod config;

pub use config::ExecutorFileConfig;

/// Executor-side BAL publication (the sequencer-role behaviour layered on the
/// shared engine: publish each block's `BlockDelta` on `tx_bal`).
pub mod bal;
pub mod parallel;
