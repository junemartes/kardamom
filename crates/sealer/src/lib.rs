//! Block sealer.
//!
//! Emits [`kardamom_types::BlockBoundaryStart`] markers every 250 ms wall-clock
//! onto tx_ordering. Single-instance for v1: there is no election, no
//! standby, no failover. If the sealer process dies the L2 stops producing
//! blocks until an operator restarts it — acceptable for v1 because the
//! sealer is off the transaction hot path.
//!
//! On restart, [`bootstrap`] reads the most recent `BlockBoundaryStart` from
//! tx_ordering's tail to resume `block_number` without gaps.
//!
//! ## Crate layout
//!
//! - [`config`] — `SealerConfig` (TOML loader + validation).
//! - [`clock`] — `WallClock` trait + `SystemClock` + `MockClock` for tests.
//! - [`tick`] — wall-clock tick alignment helpers (`floor_to_tick`, `next_tick`).
//! - [`bootstrap`] — read the most recent `BlockBoundaryStart` from tx_ordering's
//!   tail to seed `block_number`.
//! - [`emitter`] — publish loop.
//! - [`sealer`] — top-level supervisor wrapping the emitter on a tick loop.

pub mod bootstrap;
pub mod clock;
pub mod config;
pub mod emitter;
pub mod sealer;
pub mod tick;

pub use config::{ConfigError, SealerConfig};
pub use sealer::Sealer;
