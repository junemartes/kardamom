//! Kardamom S4 v0 sequential executor.
//!
//! Single-threaded revm executor that subscribes to M per-sequencer
//! **tx_data**'s + the canonical **tx_ordering** orderer and joins them by
//! reference. Publishes receipts + sealed BlockBoundaries to tx_receipts.
//! Block-STM is explicitly out of scope for v0; S4 v1 will replace the
//! single execution thread with parallel workers behind the same
//! `TxDataSubscription` / `TxOrderingSubscription` / `TxReceiptsPublication`
//! interface.
//!
//! ## Wire topology ()
//!
//! - **TxData[i]** (per-sequencer, M of them) carries full `TxEnvelope`
//!   bytes. Executors run M subscriptions, one per sequencer partition.
//! - **TxOrdering** carries tiny `TxOrderingMessage` records (`TxRef |
//!   BoundaryStart`) — ~16-32 B each. This is the canonical orderer.
//! - **TxReceipts** carries `Receipt` and slim `BlockBoundary` records (the
//!   executor's output).
//!
//! See `` §2.3
//! (transport split) and §2.4 (executor). Shared types come from
//! `kardamom-types` (S0).
//!
//! ## Divergence detection
//!
//! Spec §4.4 mandates that two replicas publishing a `Receipt` with the same
//! `tx_idx` but different `write_set_hash` halt the chain. The executor
//! cannot detect this from its own output — it has no visibility into peer
//! replicas. The detection point is the **tx_receipts consumer** that dedupes
//! by `tx_idx`; that consumer panics on hash mismatch.
//!
//! That consumer lives in the `kardamom-log` crate (S3). The chaos test
//! `cargo test -p kardamom-log --test divergence_halt` (to be added in a
//! follow-up to the S3 plan) injects a "buggy replica" by hand-crafting a
//! `Receipt` with a tweaked `write_set_hash` and asserts the consumer
//! panics. Cross-reference when that lands.

pub mod actor;
pub mod block_env;
pub mod delta;
pub mod error;
pub mod exec_types;
pub mod executor;
pub mod reader;
pub mod state;

pub use actor::{
    Executor, ExecutorConfig, StateWriterQueue, StateWriterSignal, TxReceiptsPublication,
};
pub use block_env::ExecEnv;
pub use delta::{PendingDelta, WriteSet, apply_write_set};
pub use error::ExecutorError;
pub use reader::{
    JoinBuffer, ReaderConfig, ReaderToExec, TxDataSubscription, TxOrderingSubscription,
};
pub use state::{
    MockStateDatabase, MockStateError, MutatingSnapshotSource, StaticSnapshotSource,
    WriterApplyingQueue,
};
// Shared types re-exported from the `types` crate so external callers can
// pull them via `kardamom_executor::*` without a separate dependency line.
pub use ::kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Receipt,
    SnapshotSource, StateDatabase, StorageChange, TxEnvelope, TxOrderingMessage, TxRef, WireLog,
};
pub use exec_types::{CMessage, ReceiptStatus, TxIndex};
