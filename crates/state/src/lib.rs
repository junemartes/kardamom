//! Kardamom sequencer state: MDBX-backed persistent storage for the L2 latest world.
//!
//! See `docs/agents/sequencer-state-persistence-spec.md` for the full design.

pub mod codecs;
pub mod error;
pub mod revm_adapter;
pub mod schema;
pub mod state;
pub mod store;

pub use error::StateError;
pub use revm_adapter::MdbxBackend;
pub use state::{BlockCommit, State};
