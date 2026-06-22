//! libmdbx-backed L2 state DB.
//!
//! See `` and the system for
//! the protocol and crate-level invariants.

pub mod compaction;
pub mod env;
pub mod error;
pub mod genesis;
pub mod geometry;
pub mod meta;
pub mod recovery;
pub mod schema;
pub mod snapshot;
pub mod swap;
pub mod trie;
pub mod writer;

pub use compaction::compact_to;
pub use env::{Durability, StateEnv, StateEnvBuilder};
pub use error::StateError;
pub use genesis::{genesis_applied, seed_genesis};
pub use recovery::{RecoveryPoint, read_recovery_point};
pub use snapshot::StateSnapshot;
pub use swap::{SnapshotHandle, SnapshotReceiver, channel as snapshot_channel};
pub use trie::{AccountTrieParts, empty_root, state_root, storage_root};
pub use writer::{StateWriter, WriteBatch, WriterHandle};
