//! S5 block sealer.
//!
//! Emits [`kardamom_types::BlockBoundaryStart`] markers every 250 ms wall-clock
//! onto channel B. Singleton with hot standbys; leader is the lowest-recorder-id
//! sealer whose recorder peer is caught up to the current B tail. Election is
//! driven by [`kardamom_leases::Lease`] — the same primitive the S2 sequencer
//! uses — so all three subsystems share one deterministic election rule.
//!
//! All sealer state is reconstructable from B's tail; failover is mechanical.
//! On takeover the new leader reads the most recent `BlockBoundaryStart` from
//! B and emits `block_number + 1` at the next aligned 250 ms tick.
//!
//! ## Crate layout
//!
//! - [`config`] — `SealerConfig` (TOML loader + validation).
//! - [`clock`] — `WallClock` trait + `SystemClock` + `MockClock` for tests.
//! - [`tick`] — wall-clock tick alignment helpers (`floor_to_tick`, `next_tick`).
//! - [`bootstrap`] — read the most recent `BlockBoundaryStart` from B's tail to
//!   seed `block_number`.
//! - [`emitter`] — leader-side publish loop.
//! - [`watermark_tracker`] — per-recorder freshness window feeding the lease.
//! - [`sealer`] — top-level supervisor: ties election + emitter together.
//!
//! The actual leader election lives in [`kardamom_leases::Lease`]. The
//! [`watermark_tracker`] module wraps the lease, feeds it `FsyncWatermark`
//! observations from each recorder, and synthesises a `QuorumWatermark` from
//! the current B publication position.

pub mod bootstrap;
pub mod clock;
pub mod config;
pub mod emitter;
pub mod sealer;
pub mod tick;
pub mod watermark_tracker;

pub use config::{ConfigError, SealerConfig};
// `pub use sealer::Sealer;` lands in Task 9.
