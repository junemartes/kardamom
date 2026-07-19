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
pub mod block_env;
pub mod delta;
pub mod error;
pub mod exec_types;
pub mod executor;
pub mod metrics;
pub mod persist;
pub mod reader;
pub mod replay;
pub mod state;

pub use actor::{
    Executor, ExecutorConfig, ResumePoint, StateWriterQueue, StateWriterSignal,
    TxReceiptsPublication,
};
pub use block_env::ExecEnv;
pub use delta::{PendingDelta, WriteSet};
pub use error::{EngineError, ExecutorError};
pub use exec_types::{CMessage, ReceiptStatus, TxIndex};
pub use persist::{MdbxSnapshotSource, MdbxWriterQueue, MdbxWriterSignal};
pub use reader::{
    DepositJoinBuffer, DepositSubscription, JoinBuffer, ReaderConfig, ReaderToExec,
    TxDataSubscription, TxOrderingSubscription,
};
pub use replay::{ReplayBlock, ReplayError, ReplayOutcome, replay_blocks};
pub use state::{
    MockStateDatabase, MockStateError, MutatingSnapshotSource, StaticSnapshotSource,
    WriterApplyingQueue,
};
// Shared types re-exported from the `types` crate so callers can pull them via
// `kardamom_engine::*` without a separate dependency line.
pub use ::kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Receipt,
    SnapshotSource, StateDatabase, StorageChange, TxEnvelope, TxOrderingMessage, TxRef, WireLog,
};
