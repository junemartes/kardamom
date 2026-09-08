//! Type-level wiring of the executor actor.
//!
//! This module gives [`Executor::run`] two ideas in place of a long
//! positional argument list:
//!
//! - One [`EngineWiring`] impl per role names every port type the run is
//!   generic over (nine associated types, six of them on the [`ExecPorts`]
//!   supertrait the exec thread itself needs). So `run` carries a single
//!   type parameter instead of nine, and the whole pipeline uses static
//!   dispatch, with no boxed trait object in the hot path.
//! - Category structs group the run's inputs by what they do:
//!   [`Inbound`] (what the reader threads consume), [`Outbound`] (the
//!   receipts publication and the state-writer seams), [`ResumePoint`]
//!   (the cursor execution starts from, [`GENESIS`] on a fresh chain), and
//!   [`RoleHooks`] (the optional role-specific behavior).
//!
//! [`ResumePoint`]: super::ResumePoint
//! [`GENESIS`]: super::ResumePoint::GENESIS
//!
//! A caller that needs to pick between two shapes at runtime (for example,
//! the validator's optional attester tee around its receipts sink) names
//! [`super::ports::Either`] as that one associated type instead of boxing
//! a trait object: the choice is a value, not a vtable call, and nests for
//! more than one optional layer. The whole-block execution strategy
//! ([`BlockExecStrategy`](super::types::BlockExecStrategy)) uses the same
//! idea: the implementation set is closed (the validator's parallel
//! verifier, the executor's STM pool, or
//! [`NoBlockExec`](super::types::NoBlockExec)), so it is one more
//! associated type, not a boxed closure.
//!
//! [`Executor::run`]: super::Executor::run

use crossbeam_channel::Sender;

use kardamom_types::SnapshotSource;

use crate::reader::{
    EpochObserver, JoinRecoveryFactory, TxDataSubscription, TxOrderingSubscription,
};

use super::ports::{StateWriterQueue, StateWriterSignal, TxReceiptsPublication};
use super::types::{BalHandoff, BlockExecStrategy};

/// The port types the exec thread itself needs, independent of the reader
/// and commit threads. A supertrait, not folded into [`EngineWiring`]
/// directly, so a test fixture that only drives the exec thread (see
/// `test_support::ExecRig`) can implement this alone, with no `TxData`,
/// `TxOrdering`, or `TxReceipts` type to name.
pub trait ExecPorts {
    /// Post-block state snapshot source: the state writer's read side.
    type Snapshots: SnapshotSource + 'static;
    /// Durability signal from the state writer: block N is fsynced.
    type WriterSignal: StateWriterSignal + 'static;
    /// Delta hand-off queue into the state writer.
    type WriterQueue: StateWriterQueue + 'static;
    /// Role-specific epoch check. Use
    /// [`NoEpochCheck`](crate::reader::NoEpochCheck) for roles that trust
    /// the ordered stream. The validator's verifier re-derives each epoch
    /// from L1.
    type Epoch: EpochObserver + 'static;
    /// Role-specific remote-epoch (interop) check. Use
    /// [`NoRemoteEpochCheck`](crate::reader::NoRemoteEpochCheck) for roles
    /// that trust the pair's origin sequence as sent. The destination
    /// validator's verifier re-derives each pair from the origin chain.
    type RemoteEpoch: crate::reader::RemoteEpochObserver<SnapshotDb<Self>> + 'static;
    /// Whole-block execution strategy. Use
    /// [`NoBlockExec`](super::types::NoBlockExec) for the streaming
    /// per-transaction path. The validator's parallel verifier and the
    /// executor's STM pool each name their own type here.
    type BlockExec: BlockExecStrategy<SnapshotDb<Self>> + 'static;
}

/// The full set of port types one role plugs into [`Executor::run`]:
/// [`ExecPorts`] plus the reader and commit threads' subscription and
/// publication types.
///
/// Implementors are zero-sized marker types, one per call site:
///
/// ```ignore
/// struct ValidatorWiring;
/// impl ExecPorts for ValidatorWiring {
///     type Snapshots = MdbxSnapshotSource;
///     type WriterSignal = MdbxWriterSignal;
///     type WriterQueue = MdbxWriterQueue;
///     type Epoch = EpochVerifier;
///     type RemoteEpoch = RemoteEpochVerifier;
///     type BlockExec = ParallelBlockExec;
/// }
/// impl EngineWiring for ValidatorWiring {
///     type TxData = ClusterTxDataSubscription;
///     type TxOrdering = ClusterTxOrderingSubscription;
///     type TxReceipts = Either<AttestingReceiptSink<PlainSink>, PlainSink>; // attester tee
/// }
/// ```
///
/// [`Executor::run`]: super::Executor::run
pub trait EngineWiring: ExecPorts {
    /// Per-partition `tx_data` subscription. There are M of them (see
    /// [`Inbound`]).
    type TxData: TxDataSubscription + 'static;
    /// The canonical `tx_ordering` subscription.
    type TxOrdering: TxOrderingSubscription + 'static;
    /// The `tx_receipts` publication the commit thread drains into.
    type TxReceipts: TxReceiptsPublication + 'static;
}

/// The exec-thread's state database, as named by a wiring. Shorthand for
/// the double projection through [`SnapshotSource::Db`].
pub type SnapshotDb<W> = <<W as ExecPorts>::Snapshots as SnapshotSource>::Db;

/// What the reader threads consume: the M `tx_data` subscriptions, the
/// canonical `tx_ordering` subscription, and the optional archive-backed
/// join-miss recovery.
pub struct Inbound<W: EngineWiring> {
    /// One subscription per sequencer partition (M total). Callers may
    /// supply them in any order, since each subscription declares its own
    /// `sequencer_id`.
    pub tx_data: Vec<W::TxData>,
    /// The canonical orderer. Its clean close ends the run.
    pub tx_ordering: W::TxOrdering,
    /// Archive-backed join-miss refetch factory (see
    /// [`crate::reader::JoinRecovery`]). `None` keeps the plain bounded
    /// join.
    pub join_recovery: Option<JoinRecoveryFactory>,
}

/// The actor's outbound ports: the `tx_receipts` publication and the three
/// state-writer seams (snapshot source, durability signal, delta queue).
pub struct Outbound<W: EngineWiring> {
    pub tx_receipts: W::TxReceipts,
    pub snapshots: W::Snapshots,
    pub writer_signal: W::WriterSignal,
    pub writer_queue: W::WriterQueue,
}

/// Role-specific optional behavior. Everything here defaults to off (see
/// [`RoleHooks::none`]). The executor wires BAL capture. The validator
/// wires the whole-block strategy and the epoch verifier.
pub struct RoleHooks<W: EngineWiring> {
    /// EIP-7928 capture hand-off to the BAL publisher thread (executor
    /// role). `None` skips capture.
    pub bal_capture: Option<Sender<BalHandoff>>,
    /// Footprint shadow (`crate::shadow`): per-block capture hand-off to
    /// the grader thread (executor role, `KARDAMOM_FOOTPRINT_SHADOW=1`).
    /// `None` skips capture. Ignored on the whole-block (validator) path,
    /// since captures ride the streaming arm.
    pub footprint_shadow: Option<Sender<crate::shadow::ShadowBlock>>,
    /// Whole-block execution strategy (validator parallel path). `None`
    /// keeps the per-transaction streaming path unchanged.
    pub block_exec: Option<W::BlockExec>,
    /// Epoch check, run before an epoch's deposits apply. `None` trusts the
    /// ordered stream.
    pub epoch_observer: Option<W::Epoch>,
    /// Remote-epoch check (interop), run before a `RemoteEpochRecord`'s
    /// messages apply. `None` trusts the pair's origin sequence as sent.
    /// Wired by the destination validator only.
    pub remote_epoch_observer: Option<W::RemoteEpoch>,
}

impl<W: EngineWiring> RoleHooks<W> {
    /// No role-specific behavior: streaming execution, no BAL capture, and
    /// no epoch check. This is the shape the executor and most tests use.
    #[must_use]
    pub fn none() -> Self {
        Self {
            bal_capture: None,
            footprint_shadow: None,
            block_exec: None,
            epoch_observer: None,
            remote_epoch_observer: None,
        }
    }
}
