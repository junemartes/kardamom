//! Kardamom role-agnostic execution engine.
//!
//! This is the execution core for `kardamom-executor` and
//! `kardamom-validator`. It has M per-sequencer **tx_data** readers and one
//! canonical **tx_ordering** reader. The readers join by reference. The
//! engine runs revm execution for each tx, and builds the write-set and
//! `BlockDelta`. It manages the reader-to-exec-to-commit flow.
//!
//! The commit stage works through the [`StateWriterQueue`] and
//! [`TxReceiptsPublication`] seams. Each role decides what to do at each tx
//! and block close. The executor publishes receipts and a BAL. The
//! validator builds an MPT state root and checks it.
//!
//! Block-STM is out of scope for v0. In v1, parallel workers will replace
//! the single execution thread. They will use the same
//! `TxDataSubscription` and `TxOrderingSubscription` interfaces.

pub mod actor;
pub mod bin_support;
pub mod metrics;
pub mod persist;
pub mod reader;
pub mod replay;
pub mod shadow;
pub mod state;

// The pure state-transition slice lives in `kardamom-exec-core` (a `no_std`,
// zk-guest-linkable crate). These re-exports keep old paths working, such as
// `kardamom_engine::executor::…` and `crate::delta::…`.
pub use kardamom_exec_core::{
    anchor, bal_ladder, block_env, delta, error, exec_types, executor, features, stateless, witness,
};

pub use actor::{
    EngineWiring, Executor, ExecutorConfig, Inbound, Outbound, ResumePoint, RoleHooks, SnapshotDb,
    StateWriterQueue, StateWriterSignal, TxReceiptsPublication,
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
    EpochObserver, JoinBuffer, NoEpochCheck, ReaderConfig, ReaderToExec, RemoteEpochObserver,
    TxDataSubscription, TxOrderingSubscription,
};
pub use replay::{ReplayBlock, ReplayError, ReplayOutcome, replay_blocks};
pub use state::WriterApplyingQueue;
// These types come from the `types` crate. Callers can use them through
// `kardamom_engine::*` without a separate dependency line.
pub use ::kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Receipt,
    SnapshotSource, StateDatabase, StorageChange, TxEnvelope, TxOrderingMessage, TxRef, WireLog,
};
