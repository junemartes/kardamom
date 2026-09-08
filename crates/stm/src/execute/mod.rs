//! The parallel block executor: workers pull DAG-ready transactions,
//! execute each against a per-transaction multi-version view, and
//! publish versions. A canonical-order commit pass then computes the
//! exact sequential artifacts: receipts (cumulative gas,
//! accumulator-fixed write-set hashes) and the block `PendingDelta`.
//! These are byte-identical to `Executor` output by construction, and
//! validation re-checks this.
//!
//! There is no wound-wait runtime detection (ESTIMATE marks, child
//! self-abort): validation plus whole-block sequential fallback
//! (invariant #3) carries correctness alone.

mod acquire;
mod config;
mod graph;
mod handle;
mod hash_validate;
mod metrics;
mod predecessor;
mod prepare;
mod recycle;
mod sequential;
mod session;
mod tail;
mod touch;
mod view;
mod worker;
mod worker_execute;

/// A state backend usable by one worker pool: the pool's workers read
/// it from more than one thread, and it lives at least as long as the
/// pool.
pub trait StmBackend: kardamom_types::StateDatabase + Sync + 'static {}
impl<T: kardamom_types::StateDatabase + Sync + 'static> StmBackend for T {}

/// An [`StmBackend`] the pool can also clone: one clone per worker, so
/// each worker gets its own view of the same backend.
pub trait StmSnapshot: StmBackend + Clone {}
impl<T: StmBackend + Clone> StmSnapshot for T {}

pub use config::{FifoOptions, PARALLEL_WORTH_NS, PoolConfig, Scheduler};
pub use handle::{PoolHandle, PreparedTx, with_pool};
pub use metrics::StmOutcome;
pub use prepare::Prepared;
pub use sequential::{
    SeqTx, execute_block_sequential, execute_block_sequential_decoded, execute_block_stm,
};
pub use session::{BlockSession, BlockTicket, DeltaRelease, LayerBinder, MvRelease, ReadBase};
