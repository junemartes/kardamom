//! Type-level wiring of the executor actor.
//!
//! [`Executor::run`] used to take 13 positional arguments, three of them
//! boxed trait objects. This module replaces that shape with two ideas:
//!
//! - **One [`EngineWiring`] impl per role** names every port type the run is
//!   generic over (7 associated types), so `run` carries a single type
//!   parameter instead of seven and the whole pipeline is statically
//!   dispatched — no `Box<dyn …>` anywhere in the hot path.
//! - **Category structs** group the run's inputs by what they do:
//!   [`Inbound`] (what the reader threads consume), [`Outbound`] (the
//!   receipts publication + state-writer seams), [`Start`] (where execution
//!   begins), [`RoleHooks`] (the optional role-specific behavior).
//!
//! A caller that genuinely needs to pick an implementation at RUNTIME (e.g.
//! the validator's optional attester tee around its receipts sink) still
//! can: every port trait has a forwarding impl for its boxed form, so the
//! wiring just names `Box<dyn …>` as that one associated type. The choice
//! of dynamic dispatch then lives with the caller that needs it instead of
//! being forced on every caller by the API.
//!
//! Two closure-shaped inputs intentionally stay boxed rather than becoming
//! associated types: [`BlockExec`] and
//! [`JoinRecoveryFactory`](crate::reader::JoinRecoveryFactory). Closure
//! types are unnameable, so a wiring impl could not write them out anyway —
//! callers would be forced to box exactly as they do today — and both run
//! far off the hot path (once per block / only on a join miss).
//!
//! [`Executor::run`]: super::Executor::run

use crossbeam_channel::Sender;

use kardamom_types::SnapshotSource;

use crate::reader::{
    EpochObserver, JoinRecoveryFactory, TxDataSubscription, TxOrderingSubscription,
};

use super::ports::{StateWriterQueue, StateWriterSignal, TxReceiptsPublication};
use super::types::{BalHandoff, BlockExec, ResumePoint};

/// The full set of port types one role plugs into [`Executor::run`] — the
/// "parent trait" that bundles what would otherwise be seven independent
/// type parameters.
///
/// Implementors are zero-sized marker types, one per call site:
///
/// ```ignore
/// struct ValidatorWiring;
/// impl EngineWiring for ValidatorWiring {
///     type TxData = Box<dyn TxDataSubscription>; // runtime transport choice
///     type TxOrdering = Box<dyn TxOrderingSubscription>;
///     type TxReceipts = Box<dyn TxReceiptsPublication>; // attester tee
///     type Snapshots = MdbxSnapshotSource;
///     type WriterSignal = MdbxWriterSignal;
///     type WriterQueue = MdbxWriterQueue;
///     type Epoch = EpochVerifier;
/// }
/// ```
///
/// [`Executor::run`]: super::Executor::run
pub trait EngineWiring {
    /// Per-partition tx_data subscription (M of them, see [`Inbound`]).
    type TxData: TxDataSubscription + 'static;
    /// The canonical tx_ordering subscription.
    type TxOrdering: TxOrderingSubscription + 'static;
    /// The tx_receipts publication the commit thread drains into.
    type TxReceipts: TxReceiptsPublication + 'static;
    /// Post-block state snapshot source (the state writer's read side).
    type Snapshots: SnapshotSource + 'static;
    /// Durability signal from the state writer ("block N is fsynced").
    type WriterSignal: StateWriterSignal + 'static;
    /// Delta hand-off queue into the state writer.
    type WriterQueue: StateWriterQueue + 'static;
    /// Role-specific epoch check ([`NoEpochCheck`](crate::reader::NoEpochCheck)
    /// for roles that trust the ordered stream; the validator's verifier
    /// re-derives each epoch from L1).
    type Epoch: EpochObserver + 'static;
}

/// The exec-thread's state database, as named by a wiring. Shorthand for
/// the double projection through [`SnapshotSource::Db`].
pub type SnapshotDb<W> = <<W as EngineWiring>::Snapshots as SnapshotSource>::Db;

/// What the reader threads consume: the M tx_data subscriptions, the
/// canonical tx_ordering subscription, and the optional archive-backed
/// join-miss recovery.
pub struct Inbound<W: EngineWiring> {
    /// One subscription per sequencer partition (M total). May be supplied
    /// in any order — each subscription declares its own `sequencer_id`.
    pub tx_data: Vec<W::TxData>,
    /// The canonical orderer. Its clean close is what ends the run.
    pub tx_ordering: W::TxOrdering,
    /// Archive-backed join-miss refetch factory (see
    /// [`crate::reader::JoinRecovery`]); `None` keeps the plain bounded
    /// join.
    pub join_recovery: Option<JoinRecoveryFactory>,
}

/// The actor's outbound ports: the tx_receipts publication and the three
/// state-writer seams (snapshot source, durability signal, delta queue).
pub struct Outbound<W: EngineWiring> {
    pub tx_receipts: W::TxReceipts,
    pub snapshots: W::Snapshots,
    pub writer_signal: W::WriterSignal,
    pub writer_queue: W::WriterQueue,
}

/// Where the run starts: block 0 on a fresh chain, or the persisted cursor
/// on crash recovery.
#[derive(Debug, Clone, Copy, Default)]
pub struct Start {
    /// The last durably-committed block; execution opens block
    /// `initial_block + 1`.
    pub initial_block: u64,
    /// The persisted resume cursor; `None` on a fresh start. See
    /// [`ResumePoint`] for how its fields seed the reader/exec counters.
    pub resume: Option<ResumePoint>,
}

/// Role-specific optional behavior. Everything here defaults to "off"
/// ([`RoleHooks::none`]); the executor wires BAL capture, the validator
/// wires the whole-block strategy and the epoch verifier.
pub struct RoleHooks<W: EngineWiring> {
    /// EIP-7928 capture hand-off to the BAL publisher thread (executor
    /// role); `None` skips capture entirely.
    pub bal_capture: Option<Sender<BalHandoff>>,
    /// Whole-block execution strategy (validator parallel path); `None`
    /// keeps the per-tx streaming path untouched.
    pub block_exec: Option<BlockExec<SnapshotDb<W>>>,
    /// Epoch check, run before an epoch's deposits apply; `None` trusts
    /// the ordered stream.
    pub epoch_observer: Option<W::Epoch>,
}

impl<W: EngineWiring> RoleHooks<W> {
    /// No role-specific behavior: streaming execution, no BAL capture, no
    /// epoch check (the executor's and most tests' shape).
    pub fn none() -> Self {
        Self {
            bal_capture: None,
            block_exec: None,
            epoch_observer: None,
        }
    }
}
