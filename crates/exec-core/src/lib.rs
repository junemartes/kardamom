//! Kardamom `no_std` execution core.
//!
//! The pure state-transition slice of the engine: per-tx revm execution
//! ([`executor`]), write-set / delta accumulation ([`delta`]), deterministic
//! block env construction ([`block_env`]), BAL quantization ([`bal_ladder`]),
//! and the shared error type ([`error`]). Everything here is a pure function
//! of `(state snapshot, canonical input)` — no I/O, no Aeron, no libmdbx, no
//! clocks, no entropy — so a zk-guest build can link the exact code the live
//! executor and validator run.
//!
//! `kardamom-engine` re-exports these modules; engine consumers keep their
//! `kardamom_engine::executor::…` paths. Guest builds depend on this crate
//! directly with `default-features = false`.
//!
//! The `std` feature (default) adds the operational side-channels — the
//! tracing/metrics emission on the invalid-tx skip path and the [`state`]
//! mock fixtures. Execution SEMANTICS are identical with and without.

#![cfg_attr(not(feature = "std"), no_std)]

extern crate alloc;

pub mod anchor;
pub mod bal_ladder;
pub mod block_env;
pub mod delta;
pub mod error;
pub mod exec_types;
pub mod executor;
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
