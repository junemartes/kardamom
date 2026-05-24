//! Kardamom S4 v0 sequential executor — skeleton; populated by later tasks.
//!
//! Single-threaded revm executor that consumes Aeron channel B (txs +
//! BlockBoundaryStart) and publishes receipts + sealed BlockBoundaries to
//! channel C. Block-STM is explicitly out of scope for v0; S4 v1 will replace
//! the single execution thread with parallel workers behind the same channel
//! interface.
//!
//! See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.4 and
//! the V0 scope section. Shared types come from `kardamom-types` (S0 D-Sh1).

pub mod actor;
pub mod block_env;
pub mod delta;
pub mod error;
pub mod executor;
pub mod state;
pub mod types;
