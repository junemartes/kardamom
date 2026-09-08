//! Plain data types of the executor actor: the restart and resume cursor,
//! the run configuration, the BAL and whole-block-execution hand-off shapes,
//! and the internal exec-to-commit envelope.

use std::num::{NonZeroU64, NonZeroUsize};

use kardamom_types::BlockBoundary;

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::error::ExecutorError;
use crate::reader::ReaderConfig;

/// Where a run starts: the persisted state cursor
/// (`kardamom_state::RecoveryPoint`), or [`ResumePoint::GENESIS`] on a fresh
/// chain. A fresh start is a resume from the genesis cursor (all three
/// fields zero), so there is no separate fresh-start mode to wire.
///
/// The canonical stream source (the cluster's `REPLAY_FROM` on connect)
/// delivers records from this cursor onward. Records below the cursor are
/// deduped inside [`crate::reader::cluster`]. So the reader and exec threads
/// seed their counters from this cursor, instead of replaying from record 0
/// and counting the records to skip.
///
/// Fields:
/// - `block`: the last durably-committed block (`last_committed_block`).
///   The first boundary delivered after resume is `block + 1`. Execution
///   resumes against the state snapshot taken after `block`.
/// - `record_count`: the cumulative count of canonical records (`TxRef` and
///   `DepositRef`) applied through `block`
///   (`last_fsynced_b_position.as_index()`). The reader assigns this index
///   to the first delivered record, so the boundary alignment check
///   (absolute counts) still holds across the restart. `record_count` is
///   exactly the end count of `block`, so the resume boundary falls cleanly
///   between blocks. No partial block is ever half-replayed.
/// - `l2_timestamp`: the timestamp of boundary `block` (from the committed
///   header row). Block N+1's transactions execute with boundary N's
///   timestamp, and a resumed replica never sees boundary `block` again.
///   Without this seed, the first post-resume block would execute with
///   ts=0 and silently diverge from replicas that never restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumePoint {
    pub block: u64,
    pub record_count: u64,
    pub l2_timestamp: u64,
}

impl ResumePoint {
    /// The fresh-chain cursor: nothing committed, no records applied, and
    /// no boundary timestamp yet. Execution opens block 1 against the
    /// genesis snapshot, the same as a resume from block 0.
    pub const GENESIS: Self = Self {
        block: 0,
        record_count: 0,
        l2_timestamp: 0,
    };

    /// True when this cursor points mid-chain (a crash-recovery restart)
    /// rather than at genesis.
    #[must_use]
    pub fn is_resume(&self) -> bool {
        self.block > 0
    }
}

impl Default for ResumePoint {
    fn default() -> Self {
        Self::GENESIS
    }
}

impl From<&kardamom_state::RecoveryPoint> for ResumePoint {
    /// Build the resume cursor from the state writer's persisted recovery
    /// point. `record_count` reads `last_fsynced_b_position` as an absolute
    /// canonical record count, matching how the reader and exec threads
    /// key their counters.
    fn from(recovery: &kardamom_state::RecoveryPoint) -> Self {
        Self {
            block: recovery.last_committed_block,
            record_count: recovery.last_fsynced_b_position.as_index(),
            l2_timestamp: recovery.last_committed_l2_timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub chain_id: NonZeroU64,
    /// Bound on the receipt queue between the exec and commit threads. A
    /// larger bound gives more amortization, at the cost of more memory.
    pub receipt_queue_depth: NonZeroUsize,
    /// Reader-layer tunables (join buffer timeout, growth warning
    /// threshold). See [`ReaderConfig`].
    pub reader: ReaderConfig,
    /// Re-derive every tx record's identity on arrival: check that
    /// `tx_hash == keccak256(raw_tx)`, and that `sender` matches the
    /// signature's recovered signer. This is the same check as
    /// [`crate::stateless::verify_record_identity`], which the zk guest
    /// runs. On a mismatch, abort the pipeline with
    /// [`ExecutorError::RecordIdentity`].
    ///
    /// The stream carries both fields as proxy claims. A role that
    /// leaves this off executes
    /// whatever identity the proxy asserted.
    ///
    /// The validator enables this check unconditionally and treats a
    /// mismatch as an integrity halt. The executor keeps it off: with the
    /// validator checking, a forged envelope cannot commit unnoticed. So
    /// sequencer-side rejection is defense-in-depth, priced at one ecrecover
    /// per transaction on the hot path. That trade-off is a separate
    /// decision.
    ///
    /// Deposit records are out of scope. Their identity (`source_hash`)
    /// stays a trusted input until the witness is anchored on L1.
    pub verify_record_identity: bool,
}

/// Default [`ExecutorConfig::chain_id`]: chain id 1.
const DEFAULT_CHAIN_ID: NonZeroU64 = NonZeroU64::new(1).expect("1 is nonzero");

/// Default [`ExecutorConfig::receipt_queue_depth`].
const DEFAULT_RECEIPT_QUEUE_DEPTH: NonZeroUsize = NonZeroUsize::new(1024).expect("1024 is nonzero");

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            chain_id: DEFAULT_CHAIN_ID,
            receipt_queue_depth: DEFAULT_RECEIPT_QUEUE_DEPTH,
            reader: ReaderConfig::default(),
            verify_record_identity: false,
        }
    }
}

/// Per-block EIP-7928 hand-off to the executor's BAL publisher thread. Sent
/// at each boundary when capture is enabled.
pub struct BalHandoff {
    pub boundary: BlockBoundary,
    /// The receipts-free merged delta for the frame's V1 section.
    pub delta: kardamom_types::BlockDelta,
    pub bal: revm::state::bal::Bal,
}

// The buffered-record and block-output shapes live in the `no_std`
// `kardamom-exec-core` crate, since they are its stateless driver's input
// and output types. This re-export keeps the path resolving here too.
// They mirror the payload arms of `crate::reader::ReaderToExec`.
pub use kardamom_exec_core::stateless::{BlockExecOutput, BufferedRecord};

/// Optional whole-block execution strategy.
/// [`RoleHooks::block_exec`](super::wiring::RoleHooks::block_exec) carries
/// `None` (the executor) to keep the per-transaction streaming
/// path unchanged, or `Some` (the validator's parallel verifier) to make
/// the exec thread buffer a block's records and execute them together at
/// the boundary. This is what lets batches run concurrently, seeded from
/// BAL claims.
///
/// The implementation set is closed: the validator's parallel verifier is
/// one strategy, the executor's STM pool is another, and [`NoBlockExec`]
/// is the third. [`ExecPorts::BlockExec`](super::wiring::ExecPorts) names
/// one of them per wiring, so the exec thread calls it through a static
/// type, not a boxed trait object.
///
/// `parent` is the actor's merged, not-yet-durable writes. The depth-K
/// commit pipeline lets execution run up to K blocks ahead of fsync, so the
/// snapshot alone can be K blocks stale. A strategy that ignores the parent
/// layer executes against stale state: under load, it can skip a
/// transaction (nonce mismatch) that the executor already executed.
pub trait BlockExecStrategy<D>: Send {
    /// Execute every record buffered for one block against `snapshot` and
    /// `parent`, under `env`.
    ///
    /// # Errors
    ///
    /// Returns `Err` when execution fails.
    fn execute_block(
        &self,
        snapshot: &D,
        parent: Option<&PendingDelta>,
        records: &[BufferedRecord],
        env: ExecEnv,
        block_number: u64,
    ) -> Result<BlockExecOutput, ExecutorError>;
}

/// No whole-block execution strategy: the streaming per-transaction path
/// handles every record. This type has no values, so
/// `RoleHooks::block_exec` can only ever be `None` for a wiring that names
/// it, and `execute_block` never runs.
#[derive(Debug, Clone, Copy)]
pub enum NoBlockExec {}

impl<D> BlockExecStrategy<D> for NoBlockExec {
    fn execute_block(
        &self,
        _snapshot: &D,
        _parent: Option<&PendingDelta>,
        _records: &[BufferedRecord],
        _env: ExecEnv,
        _block_number: u64,
    ) -> Result<BlockExecOutput, ExecutorError> {
        match *self {}
    }
}

/// Internal envelope routed from the exec thread to the commit thread.
pub(crate) enum ExecToCommit {
    Receipt(kardamom_types::Receipt),
    Boundary(BlockBoundary),
}
