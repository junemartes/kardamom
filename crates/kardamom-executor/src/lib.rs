//! Kardamom S4 v0 sequential executor.
//!
//! Single-threaded revm executor that subscribes to M per-sequencer
//! **channel A**'s + the canonical **channel B** orderer and joins them by
//! reference. Publishes receipts + sealed BlockBoundaries to channel C.
//! Block-STM is explicitly out of scope for v0; S4 v1 will replace the
//! single execution thread with parallel workers behind the same
//! `ChannelASubscription` / `ChannelBSubscription` / `ChannelCPublication`
//! interface.
//!
//! ## Wire topology (post-D-Sh12)
//!
//! - **Channel A[i]** (per-sequencer, M of them) carries full `TxEnvelope`
//!   bytes. Executors run M subscriptions, one per sequencer partition.
//! - **Channel B** carries tiny `ChannelBMessage` records (`TxRef |
//!   BoundaryStart`) — ~16-32 B each. This is the canonical orderer.
//! - **Channel C** carries `Receipt` and slim `BlockBoundary` records (the
//!   executor's output).
//!
//! See `docs/specs/2026-05-23-high-throughput-sequencer-design.md` §2.3
//! (transport split) and §2.4 (executor). Shared types come from
//! `kardamom-types` (S0 D-Sh1).
//!
//! ## Divergence detection
//!
//! Spec §4.4 mandates that two replicas publishing a `Receipt` with the same
//! `tx_idx` but different `write_set_hash` halt the chain. The executor
//! cannot detect this from its own output — it has no visibility into peer
//! replicas. The detection point is the **channel-C consumer** that dedupes
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
pub mod executor;
pub mod reader;
pub mod state;
pub mod types;

pub use actor::{
    ChannelCPublication, Executor, ExecutorConfig, StateWriterQueue, StateWriterSignal,
};
pub use block_env::ExecEnv;
pub use delta::{PendingDelta, WriteSet, apply_write_set};
pub use error::ExecutorError;
pub use reader::{
    ChannelASubscription, ChannelBSubscription, JoinBuffer, ReaderConfig, ReaderToExec,
};
pub use state::{
    MockStateDatabase, MockStateError, MutatingSnapshotSource, StaticSnapshotSource,
    WriterApplyingQueue,
};
// Shared types re-exported from kardamom-types so external callers can pull
// them via `kardamom_executor::*` without a separate dependency line.
pub use kardamom_types::{
    AccountChange, BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, ChannelBMessage,
    Receipt, SnapshotSource, StateDatabase, StorageChange, TxEnvelope, TxRef, WireLog,
};
pub use types::{CMessage, ReceiptStatus, TxIndex};
