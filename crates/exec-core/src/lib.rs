//! Kardamom `no_std` execution core.
//!
//! This is the pure state-transition part of the engine. It has per-tx revm
//! execution ([`executor`]), write-set and delta accumulation ([`delta`]),
//! deterministic block env construction ([`block_env`]), BAL quantization
//! ([`bal_ladder`]), and the shared error type ([`error`]). Every function
//! here depends only on the state snapshot and the canonical input. There is
//! no I/O, no Aeron, no libmdbx, no clock, and no entropy. This lets a
//! zk-guest build link the exact code that the live executor and validator
//! run.
//!
//! `kardamom-engine` re-exports these modules. Engine consumers keep their
//! `kardamom_engine::executor::…` paths. Guest builds depend on this crate
//! directly, with `default-features = false`.
//!
//! The `std` feature (default) adds operational side channels: tracing and
//! metrics on the invalid-tx skip path, and the [`state`] mock fixtures.
//! Execution semantics are the same with or without this feature.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod anchor;
pub mod bal_ladder;
pub mod block_env;
pub mod delta;
pub mod error;
pub mod exec_types;
pub mod executor;
pub mod features;
#[cfg(feature = "std")]
pub mod metrics;
#[cfg(feature = "std")]
pub mod state;
pub mod stateless;
pub mod witness;

pub use block_env::ExecEnv;
pub use delta::{PendingDelta, WriteSet};
pub use error::{EngineError, ExecutorError};
pub use exec_types::{CMessage, ReceiptStatus, TxIndex};
#[cfg(feature = "std")]
pub use state::{MockStateDatabase, MockStateError, MutatingSnapshotSource, StaticSnapshotSource};
pub use stateless::{
    BlockExecOutput, BufferedRecord, execute_block, execute_block_stateless, verify_record_identity,
};
#[cfg(feature = "std")]
pub use witness::WitnessRecorder;
pub use witness::{WitnessDb, WitnessError};
