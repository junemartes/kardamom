//! Shared fixtures for the actor's test modules: canonical-position and
//! legacy-transaction builders (over [`super::fixtures::LegacyTx`]),
//! remote-epoch fixtures, writer-signal and writer-queue test doubles, and
//! the commit-channel drain helper.

use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, Sender};
use kardamom_types::xchain::{NonEmptyVec, RemoteEpochRecord, XChainMessage, remote_source_hash};
use kardamom_types::{
    BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, SnapshotSource,
    TxEnvelope as KtTxEnvelope,
};
use revm::primitives::KECCAK_EMPTY;

use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use crate::reader::{NoEpochCheck, NoRemoteEpochCheck, ReaderToExec, RemoteEpochObserver};
use crate::state::MockStateDatabase;

use super::{
    BalHandoff, BlockExecStrategy, ExecHooks, ExecInputs, ExecPorts, ExecState, ExecToCommit,
    ExecutorConfig, NoBlockExec, ResumePoint, StateWriterQueue, StateWriterSignal,
};

pub(super) fn pos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// A `MockStateDatabase` funding `signer`'s address with 1 ETH at `nonce`.
/// The common pre-block snapshot most streaming exec tests start from.
pub(super) fn funded(signer: &PrivateKeySigner, nonce: u64) -> MockStateDatabase {
    MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            nonce,
            KECCAK_EMPTY,
        )
        .build()
}

/// Build a `ReaderToExec::Boundary` message: `block_number` closes at
/// `end_count` canonical records (encoded as a `BPosition`), stamped
/// `l2_timestamp`. `l1_origin` is always 0 in this crate's fixtures; no
/// test here exercises a non-zero epoch.
pub(super) fn boundary_msg(block_number: u64, end_count: i32, l2_timestamp: u64) -> ReaderToExec {
    ReaderToExec::Boundary(BlockBoundaryStart {
        block_number,
        end_tx_idx: pos(end_count),
        l2_timestamp,
        l1_origin: 0,
    })
}

/// Build a `ReaderToExec::Tx` message: canonical index `idx` (used for
/// both `tx_idx` and `position` — the common case where a tx's wire
/// position matches its canonical count), a `legacy` transfer of `value`
/// to `to` at nonce `nonce`.
pub(super) fn tx_msg(
    signer: &PrivateKeySigner,
    to: Address,
    idx: u64,
    nonce: u64,
    value: u64,
) -> ReaderToExec {
    ReaderToExec::Tx {
        tx_idx: TxIndex(idx),
        envelope: legacy(signer, to, nonce, value),
        position: pos(i32::try_from(idx).expect("test fixture: idx fits in i32")),
    }
}

/// This crate's own fixtures' shape: chain id 1, a 21,000-gas transfer,
/// over [`super::fixtures::LegacyTx`]. `pub(crate)`: `reader::tests` and
/// `replay::tests` also build this fixture, and import this one copy
/// instead of their own.
pub(crate) fn legacy(
    signer: &PrivateKeySigner,
    to: Address,
    nonce: u64,
    value: u64,
) -> KtTxEnvelope {
    super::fixtures::LegacyTx {
        chain_id: 1,
        to,
        nonce,
        value,
        gas_limit: 21_000,
        gas_price: 0,
    }
    .sign(signer)
}

pub(super) struct ImmediateCommit;
impl StateWriterSignal for ImmediateCommit {
    fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
        Ok(at_least)
    }
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(u64::MAX)
    }
}

/// Writer signal whose durable level a test controls. `committed()` reads
/// the level. `wait_committed` (the depth-cap block) sets it to the target
/// value and counts the call.
pub(super) struct StagedCommit {
    pub(super) durable: Arc<std::sync::atomic::AtomicU64>,
    pub(super) blocking_waits: Arc<std::sync::atomic::AtomicU64>,
}
impl StateWriterSignal for StagedCommit {
    fn wait_committed(&mut self, at_least: u64) -> Result<u64, ExecutorError> {
        self.blocking_waits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.durable
            .fetch_max(at_least, std::sync::atomic::Ordering::SeqCst);
        Ok(self.durable.load(std::sync::atomic::Ordering::SeqCst))
    }
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(self.durable.load(std::sync::atomic::Ordering::SeqCst))
    }
}

/// Every `(BlockBoundary, BlockDelta)` a `RecordingQueue` or
/// `ApplyingRecordingQueue` has submitted, in submit order.
pub(super) type WriterLog = Arc<Mutex<Vec<(BlockBoundary, BlockDelta)>>>;

pub(super) struct RecordingQueue(pub(super) WriterLog);
impl StateWriterQueue for RecordingQueue {
    fn submit(&mut self, b: BlockBoundary, d: BlockDelta) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push((b, d));
        Ok(())
    }
}

/// Drain a closed `ExecToCommit` receiver. Return the block numbers of the
/// emitted receipts and boundaries, each in order.
pub(super) fn drain_commits(rx: &Receiver<ExecToCommit>) -> (Vec<u64>, Vec<u64>) {
    let mut receipts = Vec::new();
    let mut boundaries = Vec::new();
    while let Ok(m) = rx.recv() {
        push_commit(m, &mut receipts, &mut boundaries);
    }
    (receipts, boundaries)
}

/// Sort one drained message into `receipts` or `boundaries`. The `while`
/// loop in [`drain_commits`] stays free of a branch.
fn push_commit(m: ExecToCommit, receipts: &mut Vec<u64>, boundaries: &mut Vec<u64>) {
    match m {
        ExecToCommit::Receipt(r) => receipts.push(r.block_number),
        ExecToCommit::Boundary(b) => boundaries.push(b.block_number),
    }
}

/// Records every submitted block, and applies it to a shared
/// `MockStateDatabase`. This way, a later block's snapshot observes an
/// earlier block's writes.
///
/// Pair this with `MutatingSnapshotSource` over the same handle, for any
/// test that spans more than one block. A plain `RecordingQueue` with
/// `StaticSnapshotSource` silently loses committed state: the settle sweep
/// drops a settled block from the parent read layer, assuming the
/// refreshed snapshot now contains it. A static snapshot never does. So
/// multi-block state carry-over reads as zero, and a test can pass against
/// behavior production would never show.
pub(super) struct ApplyingRecordingQueue {
    pub(super) db: kardamom_exec_core::state::MockStateDatabase,
    pub(super) log: WriterLog,
}

impl StateWriterQueue for ApplyingRecordingQueue {
    fn submit(&mut self, b: BlockBoundary, d: BlockDelta) -> Result<(), ExecutorError> {
        self.db.apply_block_delta(&d);
        self.log.lock().unwrap().push((b, d));
        Ok(())
    }
}

/// Build a remote-epoch record with `n` messages, for the interop tests.
///
/// # Panics
///
/// Panics if `n == 0`: a remote epoch always carries at least one message.
pub(super) fn remote_epoch_fixture(origin: u64, n: NonZeroU64) -> RemoteEpochRecord {
    let message_at = |seq: u64| XChainMessage {
        source_hash: remote_source_hash(origin, seq),
        seq,
        origin_sender: Address::repeat_byte(0xA5),
        target: Address::repeat_byte(0xB6),
        value: 0,
        gas_limit: 100_000,
        input: bytes::Bytes::default(),
        callback: None,
    };
    RemoteEpochRecord {
        origin_chain_id: origin,
        anchor_number: 900,
        anchor_hash: alloy_primitives::B256::repeat_byte(0x0A),
        first_seq: 0,
        messages: NonEmptyVec::new(message_at(0), (1..n.get()).map(message_at).collect()),
    }
}

/// Build the marker + its messages, mirroring what the `tx_ordering` reader
/// dispatches for one record. The caller appends the closing boundary.
pub(super) fn remote_epoch_records(record: RemoteEpochRecord) -> Vec<ReaderToExec> {
    let origin = record.origin_chain_id;
    let messages: Vec<XChainMessage> = record.messages.iter().cloned().collect();
    let mut out = vec![ReaderToExec::RemoteEpoch {
        tx_idx: TxIndex(0),
        record: Box::new(record),
        position: pos(0),
    }];
    out.extend(
        messages
            .into_iter()
            .enumerate()
            .map(|(i, message)| ReaderToExec::XChain {
                tx_idx: TxIndex(1 + i as u64),
                origin_chain_id: origin,
                message: Box::new(message),
                position: pos(1 + i32::try_from(i).expect("small test fixture")),
            }),
    );
    out
}

/// Feed `records` into a fresh unbounded channel, then close the sender.
/// This is the reader-to-exec handoff shape every exec test drives: every
/// record is queued before `ExecState::spawn` starts, and the closed sender
/// signals end-of-stream once the exec thread drains them.
pub(super) fn feed(records: Vec<ReaderToExec>) -> Receiver<ReaderToExec> {
    let (tx, rx) = crossbeam_channel::unbounded();
    for record in records {
        tx.send(record).unwrap();
    }
    rx
}

/// Same as [`feed`], for the exec-to-commit channel: queue every message,
/// then close the sender, so `CommitLoop` sees a clean end of stream
/// after draining them.
pub(super) fn feed_commits(messages: Vec<ExecToCommit>) -> Receiver<ExecToCommit> {
    let (tx, rx) = crossbeam_channel::unbounded();
    for message in messages {
        tx.send(message).unwrap();
    }
    rx
}

/// Depth for the `ExecRig`-owned exec-to-commit channel. Nothing drains it
/// until after `.join()` (or, for the idle-probe test, until the test
/// reads it directly), so this only needs to comfortably exceed the
/// largest receipt+boundary count any current test produces (at most a
/// few dozen).
const RIG_COMMIT_CAPACITY: usize = 128;

/// The exec-only port bundle every `ExecRig` names as its `W: ExecPorts`.
/// A zero-sized marker: `ExecRig` owns the actual port values, this type
/// only carries their types through to `ExecState::spawn`. Epoch checking is
/// always [`NoEpochCheck`], since no test in this crate supplies a
/// non-trivial epoch observer.
struct TestPorts<S, Q, P, R, B>(std::marker::PhantomData<(S, Q, P, R, B)>);

impl<S, Q, P, R, B> ExecPorts for TestPorts<S, Q, P, R, B>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
    R: RemoteEpochObserver<S::Db> + 'static,
    B: BlockExecStrategy<S::Db> + 'static,
{
    type Snapshots = S;
    type WriterSignal = Q;
    type WriterQueue = P;
    type Epoch = NoEpochCheck;
    type RemoteEpoch = R;
    type BlockExec = B;
}

/// Builder for `ExecState::spawn`'s test fixtures. Every exec test wires the same
/// twelve-argument call, with nine of the twelve almost always `None`. This
/// collects them: `cfg` is always `ExecutorConfig::default()` and `start`
/// is always `ResumePoint::GENESIS` in every current test, so both start
/// there; the hook builders opt in only where a test needs one.
///
/// `R` defaults to [`NoRemoteEpochCheck`]; [`Self::remote`] swaps it for a
/// test's own observer type. `B` defaults to [`NoBlockExec`];
/// [`Self::block_exec`] swaps it for a test's own strategy type.
pub(super) struct ExecRig<S: SnapshotSource, Q, P, R = NoRemoteEpochCheck, B = NoBlockExec> {
    snapshots: S,
    sw_signal: Q,
    sw_queue: P,
    start: ResumePoint,
    bal_tx: Option<Sender<BalHandoff>>,
    shadow_tx: Option<Sender<crate::shadow::ShadowBlock>>,
    block_exec: Option<B>,
    remote_epoch_observer: Option<R>,
    tx_e2c: Sender<ExecToCommit>,
    rx_e2c: Receiver<ExecToCommit>,
}

impl<S, Q, P> ExecRig<S, Q, P, NoRemoteEpochCheck, NoBlockExec>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
{
    pub(super) fn new(snapshots: S, sw_signal: Q, sw_queue: P) -> Self {
        let (tx_e2c, rx_e2c) = crossbeam_channel::bounded(RIG_COMMIT_CAPACITY);
        Self {
            snapshots,
            sw_signal,
            sw_queue,
            start: ResumePoint::GENESIS,
            bal_tx: None,
            shadow_tx: None,
            block_exec: None,
            remote_epoch_observer: None,
            tx_e2c,
            rx_e2c,
        }
    }
}

impl<S, Q, P, B> ExecRig<S, Q, P, NoRemoteEpochCheck, B>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
    B: BlockExecStrategy<S::Db> + 'static,
{
    /// Swap in a test's own remote-epoch observer. Consumes the default
    /// [`NoRemoteEpochCheck`] rig and returns one typed for `R`, since a
    /// struct field cannot change type through `&mut self`.
    pub(super) fn remote<R: RemoteEpochObserver<S::Db> + 'static>(
        self,
        observer: R,
    ) -> ExecRig<S, Q, P, R, B> {
        ExecRig {
            snapshots: self.snapshots,
            sw_signal: self.sw_signal,
            sw_queue: self.sw_queue,
            start: self.start,
            bal_tx: self.bal_tx,
            shadow_tx: self.shadow_tx,
            block_exec: self.block_exec,
            remote_epoch_observer: Some(observer),
            tx_e2c: self.tx_e2c,
            rx_e2c: self.rx_e2c,
        }
    }
}

impl<S, Q, P, R, B> ExecRig<S, Q, P, R, B>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
    P: StateWriterQueue + 'static,
    R: RemoteEpochObserver<S::Db> + 'static,
    B: BlockExecStrategy<S::Db> + 'static,
{
    pub(super) fn start(mut self, start: ResumePoint) -> Self {
        self.start = start;
        self
    }

    pub(super) fn bal(mut self, tx: Sender<BalHandoff>) -> Self {
        self.bal_tx = Some(tx);
        self
    }

    pub(super) fn shadow(mut self, tx: Sender<crate::shadow::ShadowBlock>) -> Self {
        self.shadow_tx = Some(tx);
        self
    }

    /// Swap in a test's own whole-block strategy. Consumes the default
    /// [`NoBlockExec`] rig and returns one typed for `B2`, since a struct
    /// field cannot change type through `&mut self`.
    pub(super) fn block_exec<B2: BlockExecStrategy<S::Db> + 'static>(
        self,
        strategy: B2,
    ) -> ExecRig<S, Q, P, R, B2> {
        ExecRig {
            snapshots: self.snapshots,
            sw_signal: self.sw_signal,
            sw_queue: self.sw_queue,
            start: self.start,
            bal_tx: self.bal_tx,
            shadow_tx: self.shadow_tx,
            block_exec: Some(strategy),
            remote_epoch_observer: self.remote_epoch_observer,
            tx_e2c: self.tx_e2c,
            rx_e2c: self.rx_e2c,
        }
    }

    /// Spawns the exec thread. Returns its handle, and the exec-to-commit
    /// receiver.
    ///
    /// The receiver must come back to the caller, not stay a field this
    /// method drops: `self.rx_e2c` is the channel's only receiver, and a
    /// bounded `Sender::send` inside the exec thread turns into an
    /// immediate `Flow::Stop` the moment its last receiver disappears.
    /// Returning it here, for the caller to bind (even to a name the test
    /// never reads), keeps it alive until the test's own scope ends —
    /// normally past the `.join()` call this handle is for.
    pub(super) fn spawn(
        self,
        rx: Receiver<ReaderToExec>,
    ) -> (
        JoinHandle<Result<(), ExecutorError>>,
        Receiver<ExecToCommit>,
    ) {
        let rx_e2c = self.rx_e2c;
        let h = ExecState::<TestPorts<S, Q, P, R, B>>::spawn(ExecInputs {
            cfg: ExecutorConfig::default(),
            rx,
            tx: self.tx_e2c,
            snapshots: self.snapshots,
            sw_signal: self.sw_signal,
            sw_queue: self.sw_queue,
            start: self.start,
            hooks: ExecHooks {
                bal_tx: self.bal_tx,
                shadow_tx: self.shadow_tx,
                block_exec: self.block_exec,
                epoch_observer: None,
                remote_epoch_observer: self.remote_epoch_observer,
            },
        });
        (h, rx_e2c)
    }
}

impl<S, Q> ExecRig<S, Q, RecordingQueue>
where
    S: SnapshotSource + 'static,
    Q: StateWriterSignal + 'static,
{
    /// Convenience constructor for the common case: a `RecordingQueue`
    /// writer over a fresh writer log. Returns the rig and a clone of the
    /// log handle, since `RecordingQueue` owns the other clone.
    pub(super) fn recording(snapshots: S, sw_signal: Q) -> (Self, WriterLog) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let rig = Self::new(snapshots, sw_signal, RecordingQueue(log.clone()));
        (rig, log)
    }
}
