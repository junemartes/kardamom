//! Kardamom role-agnostic execution engine.
//!
//! The execution core shared by the sequencer-side `kardamom-executor` and the
//! `kardamom-validator`: M per-sequencer **tx_data** readers + the canonical
//! **tx_ordering** reader joined by reference, per-tx revm execution, write-set
//! / `BlockDelta` accumulation, and the reader→exec→commit orchestration. The
//! commit stage is generalized over the existing [`StateWriterQueue`] and
//! [`TxReceiptsPublication`] seams, so each role decides what happens at each
//! tx and block close (the executor publishes receipts + a BAL; the validator
//! builds an MPT state root and cross-checks itself).
//!
//! Block-STM is out of scope for v0; v1 will replace the single execution
//! thread with parallel workers behind the same `TxDataSubscription` /
//! `TxOrderingSubscription` interfaces.

pub mod actor;
pub mod bin_support;
pub mod metrics;
pub mod persist;
pub mod reader;
pub mod replay;
pub mod state;

// The pure state-transition slice lives in `kardamom-exec-core` (`no_std`,
// zk-guest-linkable). Module re-exports keep every pre-split path working:
// `kardamom_engine::executor::…`, `crate::delta::…` etc. all still resolve.
pub use kardamom_exec_core::{
    anchor, bal_ladder, block_env, delta, error, exec_types, executor, stateless, witness,
};

pub use actor::{
    Executor, ExecutorConfig, ResumePoint, StateWriterQueue, StateWriterSignal,
    TxReceiptsPublication,
};
pub use block_env::ExecEnv;
pub use delta::{PendingDelta, WriteSet};
pub use error::{EngineError, ExecutorError};
pub use exec_types::{CMessage, ReceiptStatus, TxIndex};
pub use kardamom_exec_core::{
    MockStateDatabase, MockStateError, MutatingSnapshotSource, StaticSnapshotSource,
};
pub use persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
pub use reader::{
    EpochObserver, JoinBuffer, ReaderConfig, ReaderToExec, TxDataSubscription,
    TxOrderingSubscription,
};
pub use replay::{ReplayBlock, ReplayError, ReplayOutcome, replay_blocks};
pub use state::WriterApplyingQueue;
// Shared types re-exported from the `types` crate so callers can pull them via
// `kardamom_engine::*` without a separate dependency line.
pub use ::kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Receipt,
    SnapshotSource, StateDatabase, StorageChange, TxEnvelope, TxOrderingMessage, TxRef, WireLog,
};
