//! libmdbx-backed L2 state DB.
//!
//! See `docs/plans/2026-05-23-S6-state-writer.md` and the system spec §5 for
//! the protocol and crate-level invariants.
//!
//! Public surface (gated until each module fills in):
//! - [`StateEnv`] — owns the mdbx `Environment` and table handles.
//! - [`StateWriter`] — single-writer thread that drains a `BlockDelta` channel
//!   and commits one mdbx RW txn per block boundary.
//! - [`StateSnapshot`] — RO txn wrapper exposed to the executor via the
//!   `kardamom_types::StateDatabase` trait.
//! - [`SnapshotHandle`] — snapshot-swap channel published by the writer; the
//!   executor watches it to pick up post-N snapshots.

pub mod compaction;
pub mod env;
pub mod error;
pub mod geometry;
pub mod meta;
pub mod recovery;
pub mod schema;
pub mod snapshot;
pub mod swap;
pub mod writer;
