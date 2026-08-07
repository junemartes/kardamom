//! Plain data types of the executor actor: restart/resume cursor, run
//! configuration, the BAL and whole-block-execution hand-off shapes, and the
//! internal exec → commit envelope.

use kardamom_types::{BPosition, BlockBoundary, Receipt};

use crate::block_env::ExecEnv;
use crate::delta::PendingDelta;
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::ReaderConfig;

/// Where a restarted executor resumes from, derived from the persisted state
/// cursor (`kardamom_state::RecoveryPoint`). The canonical stream source (the
/// cluster's `REPLAY_FROM` on connect) delivers **from this cursor onward** —
/// records below it are deduped inside [`crate::reader::cluster`] — so the
/// reader and exec threads seed their counters here instead of replaying from
/// record 0 and skip-counting:
///
/// - `block` — the last durably-committed block (`last_committed_block`). The
///   first boundary delivered after resume is `block + 1`; the post-`block`
///   state snapshot is what execution resumes against.
/// - `record_count` — the cumulative count of canonical records (TxRef +
///   DepositRef) applied through `block` (`last_fsynced_b_position.as_index()`).
///   The reader assigns the first delivered record this index, so the boundary
///   alignment check (absolute counts) holds across the restart. Because
///   `record_count` is exactly the end count of `block`, the resume boundary
///   falls cleanly between blocks — no partial block is ever half-replayed.
/// - `l2_timestamp` — boundary `block`'s timestamp (from the committed header
///   row). Block N+1's txs execute with boundary N's timestamp, and a resumed
///   replica never sees boundary `block` again — without this seed its first
///   post-resume block would execute with ts=0 and silently diverge from the
///   replicas that never restarted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumePoint {
    pub block: u64,
    pub record_count: u64,
    pub l2_timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub chain_id: u64,
    /// Bound on the receipt queue between exec and commit threads. Larger =
    /// more amortization, more memory.
    pub receipt_queue_depth: usize,
    /// Reader-layer tunables (join buffer timeout, growth warning
    /// threshold). See [`ReaderConfig`].
    pub reader: ReaderConfig,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            chain_id: 1,
            receipt_queue_depth: 1024,
            reader: ReaderConfig::default(),
        }
    }
}

/// Per-block EIP-7928 handoff to the executor's BAL publisher thread:
/// (boundary, receipts-free merged delta for the frame's V1 section, the
/// captured Bal). Sent at each boundary when capture is enabled.
pub type BalHandoff = (
    BlockBoundary,
    kardamom_types::BlockDelta,
    revm::state::bal::Bal,
);

/// One canonical record buffered for whole-block execution (validator
/// parallel path). Mirrors [`crate::reader::ReaderToExec`]'s payload arms.
/// Clone is cheap: envelope/deposit byte payloads are refcounted `Bytes`
/// (the validator's flight ring keeps recent blocks' records for
/// receipt-divergence dumps).
#[derive(Clone)]
pub enum BufferedRecord {
    Tx {
        tx_idx: TxIndex,
        envelope: kardamom_types::TxEnvelope,
        position: BPosition,
    },
    Deposit {
        tx_idx: TxIndex,
        deposit: kardamom_types::Deposit,
        position: BPosition,
    },
}

/// What a block-execution strategy returns: the block's receipts in block
/// order (block-cumulative gas already correct) and its merged writes.
pub struct BlockExecOutput {
    pub receipts: Vec<Receipt>,
    pub delta: PendingDelta,
}

/// Optional whole-block execution strategy. `None` (the executor) keeps the
/// per-tx streaming path untouched. `Some` (the validator's parallel
/// verifier) makes the exec thread BUFFER a block's records and execute
/// them together at the boundary — which is what allows batches to run
/// concurrently, seeded from BAL claims.
/// (snapshot, PARENT LAYER, records, env, block_number). The parent layer
/// is the actor's merged not-yet-durable writes — the depth-K commit
/// pipeline lets execution run up to K blocks ahead of fsync, so the
/// snapshot alone can be K blocks STALE. Ignoring it executes against old
/// state: under load the validator skipped txs (nonce mismatch) the
/// executor had executed, a proven divergence in the first DeFi gate.
pub type BlockExec<D> = Box<
    dyn Fn(
            &D,
            Option<&PendingDelta>,
            &[BufferedRecord],
            ExecEnv,
            u64,
        ) -> Result<BlockExecOutput, ExecutorError>
        + Send,
>;

/// Internal envelope routed from exec → commit thread.
pub(crate) enum ExecToCommit {
    Receipt(kardamom_types::Receipt),
    Boundary(BlockBoundary),
}
