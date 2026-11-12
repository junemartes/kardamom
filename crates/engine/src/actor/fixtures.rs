//! The channel-backed [`ChannelHarness`] that runs the real
//! [`crate::Executor`] pipeline in-process over channels, the
//! [`TestEgress`] that serves the canonical stream with its bytes, the
//! [`TestStream`] that the `tx_ordering` reader reads from either source,
//! and a re-export of the signed-legacy-transaction fixture from
//! `kardamom-test-support`.
//!
//! [`LegacyTx`] and the signer helpers live in `kardamom-test-support`.
//! This module re-exports them, so a caller that already imports
//! `kardamom_engine::actor::fixtures` for the harness gets the fixture
//! from the same path. `kardamom-validator` and `kardamom-executor`
//! reach both through the `test-support` feature. This crate's own
//! `actor::test_support::legacy` builds on [`LegacyTx::sign`].

use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded, unbounded};
use kardamom_types::TxEnvelope as KtTxEnvelope;

pub use egress::{EgressReader, Retention, TestEgress};
pub use kardamom_test_support::{LegacyTx, anvil_signer_0, byte_signer, seeded_signer};
pub use stream::{TestEntry, TestStream};

mod egress;
mod stream;

// ---------------------------------------------------------------------------
// The chain-state predeploy and its upgrade transactions.
// ---------------------------------------------------------------------------

/// The chain-state predeploy of the dev genesis: its account and its code.
/// A test chain that runs `setFeature` deposits needs both.
pub struct ChainStatePredeploy {
    pub account: kardamom_types::AccountChange,
    pub code: kardamom_types::delta::CodeEntry,
}

impl ChainStatePredeploy {
    /// The predeploy of `chains/dev-withdrawals.toml`.
    ///
    /// # Panics
    ///
    /// Panics if the genesis does not parse or has no predeploy.
    #[must_use]
    pub fn dev() -> Self {
        let genesis: kardamom_types::Genesis =
            toml::from_str(include_str!("../../../../chains/dev-withdrawals.toml"))
                .expect("parse the genesis");
        let alloc = genesis.to_alloc();
        let account = alloc
            .accounts
            .into_iter()
            .find(|a| a.address == kardamom_types::upgrades::CHAIN_STATE)
            .expect("the genesis holds the chain-state predeploy");
        let code = alloc
            .code
            .into_iter()
            .find(|c| c.code_hash == account.code_hash)
            .expect("the predeploy code");
        Self { account, code }
    }

    /// The epoch of L1 block `l1_number`, whose upgrade transactions
    /// schedule `feature_id` at each of `activations_ms`, in log order.
    /// Each one derives into a `setFeature` system deposit.
    ///
    /// # Panics
    ///
    /// Panics if the logs do not derive into an epoch.
    #[must_use]
    pub fn upgrade_epoch(
        l1_number: u64,
        feature_id: u64,
        activations_ms: &[u64],
    ) -> kardamom_types::EpochRecord {
        use kardamom_types::epoch::{LockboxLog, UpgradeLog, derive_epoch};
        let l1_hash = alloy_primitives::keccak256(l1_number.to_be_bytes());
        let logs: Vec<LockboxLog> = activations_ms
            .iter()
            .zip(0u64..)
            .map(|(&activation_timestamp, log_index)| {
                LockboxLog::Upgrade(UpgradeLog {
                    block_number: l1_number,
                    block_hash: l1_hash,
                    log_index,
                    feature_id: alloy_primitives::U256::from(feature_id),
                    activation_timestamp,
                })
            })
            .collect();
        derive_epoch(l1_number, l1_hash, &logs).expect("one block's logs")
    }
}

/// Whether the genesis of a test chain holds the chain-state predeploy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChainStateGenesis {
    /// The predeploy of `chains/dev-withdrawals.toml`.
    Predeploy,
    /// No account at the chain-state address, as the staging genesis. The
    /// first `setFeature` deposit installs the code.
    Empty,
}

impl ChainStateGenesis {
    /// Both kinds of genesis.
    pub const ALL: [Self; 2] = [Self::Predeploy, Self::Empty];

    /// The predeploy that this genesis holds.
    ///
    /// # Panics
    ///
    /// Panics if the dev genesis does not parse or has no predeploy.
    #[must_use]
    pub fn predeploy(self) -> Option<ChainStatePredeploy> {
        match self {
            Self::Predeploy => Some(ChainStatePredeploy::dev()),
            Self::Empty => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Channel-backed engine harness.
// ---------------------------------------------------------------------------
//
// Every in-process test and bench of the executor pipeline uses these
// channel doubles and this one wiring. [`ChannelHarness`] owns the channels
// and the spawned threads. [`TestWiring`] and [`Imm`] stay public for a
// caller with its own subscription types, such as the wire-codec fakes of
// `kardamom_log::testing`.

/// A commit signal that reports every block as already durable. Every
/// in-process test and bench runs against an in-memory writer queue, so
/// there is nothing to wait on.
pub struct Imm;

impl crate::StateWriterSignal for Imm {
    fn committed(&mut self) -> Result<u64, crate::ExecutorError> {
        Ok(u64::MAX)
    }

    fn wait_committed(&mut self, b: u64) -> Result<u64, crate::ExecutorError> {
        Ok(b)
    }
}

/// The executor role's port types, over a caller-chosen `tx_data`,
/// `tx_ordering`, and `tx_receipts` triple. Every test and bench that
/// drives [`crate::Executor::run`] in-process uses this one wiring,
/// generic over its channel-backed fakes, instead of a fresh `ExecPorts`
/// and `EngineWiring` pair per file.
pub struct TestWiring<D, O, R>(std::marker::PhantomData<(D, O, R)>);

impl<D, O, R> crate::ExecPorts for TestWiring<D, O, R> {
    type Snapshots = crate::MutatingSnapshotSource;
    type WriterSignal = Imm;
    type WriterQueue = crate::WriterApplyingQueue;
    // No epoch verification: these doubles trust the ordered stream.
    type Epoch = crate::NoEpochCheck;
    type RemoteEpoch = crate::NoRemoteEpochCheck;
    type BlockExec = crate::NoBlockExec;
    // Off unless `HarnessSetup::tx_hook` turns it on.
    type TxHook = Option<crate::VerifyRecordIdentity>;
}

impl<D, O, R> crate::EngineWiring for TestWiring<D, O, R>
where
    D: crate::TxDataSubscription + 'static,
    O: crate::TxOrderingSubscription + 'static,
    R: crate::TxReceiptsPublication + 'static,
{
    type TxData = D;
    type TxOrdering = O;
    type TxReceipts = R;
    type ExecStream = crate::reader::NoExecStream;
}

/// A `tx_data` subscription backed by a crossbeam channel, standing in
/// for the wire.
pub struct ChanTxDataSub {
    pub sequencer_id: u8,
    pub rx: Receiver<(crate::BPosition, KtTxEnvelope)>,
}

impl crate::TxDataSubscription for ChanTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(kardamom_types::TxDataLoc, KtTxEnvelope), crate::ExecutorError> {
        self.rx
            .recv()
            .map(|(pos, env)| (kardamom_types::TxDataLoc::new(0, pos), env))
            .map_err(|_| crate::ExecutorError::TxDataClosed {
                sequencer_id: self.sequencer_id,
            })
    }
}

/// A `tx_ordering` subscription backed by a crossbeam channel, standing
/// in for the Aeron Cluster egress.
pub struct ChanTxOrderingSub(pub Receiver<(crate::BPosition, crate::TxOrderingMessage)>);

impl crate::TxOrderingSubscription for ChanTxOrderingSub {
    fn next(&mut self) -> Result<(crate::BPosition, crate::reader::Ordered), crate::ExecutorError> {
        self.0
            .recv()
            .map(|(pos, msg)| (pos, msg.into()))
            .map_err(|_| crate::ExecutorError::TxOrderingClosed)
    }
}

/// A `tx_receipts` publication backed by a crossbeam channel, standing in
/// for the wire.
pub struct ChanReceiptsPub(pub Sender<crate::CMessage>);

impl crate::TxReceiptsPublication for ChanReceiptsPub {
    fn publish_rollback(
        &mut self,
        _rollback: kardamom_types::ReceiptRollback,
    ) -> Result<(), crate::ExecutorError> {
        Ok(())
    }

    fn publish(&mut self, m: crate::CMessage) -> Result<(), crate::ExecutorError> {
        self.0
            .send(m)
            .map_err(|_| crate::ExecutorError::TxReceiptsClosed)
    }
}

/// The channel-backed wiring [`ChannelHarness`] drives, over the
/// `tx_ordering` source `O`.
type HarnessWiring<O> = TestWiring<ChanTxDataSub, O, ChanReceiptsPub>;

/// Depth of the `tx_receipts` channel. The test thread drains it while the
/// engine runs, so this only bounds how far commit runs ahead of the drain.
const RECEIPTS_DEPTH: usize = 64;

/// How long a drain waits for the next receipt before it stops.
const RECEIPT_WAIT: Duration = Duration::from_secs(5);

/// What a harness run starts from: the executor config, the resume cursor,
/// the starting state snapshot, and the optional identity hook. The
/// snapshot is also the shared post-run state.
pub struct HarnessSetup {
    pub cfg: crate::ExecutorConfig,
    pub start: crate::ResumePoint,
    pub snap: crate::MockStateDatabase,
    pub tx_hook: Option<crate::VerifyRecordIdentity>,
}

/// Input for [`ChannelHarness::run`]: one record list per `tx_data` lane
/// (lane `i` carries `sequencer_id` `i`), and the `tx_ordering` records in
/// the order the engine must see them.
pub struct HarnessInput {
    pub tx_data: Vec<Vec<(crate::BPosition, KtTxEnvelope)>>,
    pub tx_ordering: Vec<(crate::BPosition, crate::TxOrderingMessage)>,
}

/// [`ChannelHarness::finish`]'s result: the engine's outcome, every
/// `tx_receipts` message drained while it ran, and the shared state after
/// the run.
pub struct HarnessOutcome {
    pub result: Result<(), crate::ExecutorError>,
    pub receipts: Vec<crate::CMessage>,
    pub state: crate::MockStateDatabase,
}

/// A live run of the real [`crate::Executor`] pipeline over channel-backed
/// subscriptions. [`Self::spawn`] starts the threads with every input
/// channel open. A test feeds records at any time with
/// [`Self::send_tx_data`] and [`Self::send_tx_ordering`], reads
/// [`Self::tx_receipts`] while the engine runs, and ends the run with
/// [`Self::finish`]. [`Self::run`] does all of this for an input known
/// upfront.
pub struct ChannelHarness {
    tx_data: Vec<Sender<(crate::BPosition, KtTxEnvelope)>>,
    tx_ordering: Sender<(crate::BPosition, crate::TxOrderingMessage)>,
    pipeline: Pipeline,
}

impl ChannelHarness {
    /// Spawn the pipeline with `lanes` `tx_data` subscriptions (lane `i`
    /// carries `sequencer_id` `i`), one `tx_ordering`, and one
    /// `tx_receipts`. The input channels are unbounded, so a send never
    /// blocks the test thread.
    #[must_use]
    pub fn spawn(setup: HarnessSetup, lanes: u8) -> Self {
        let (tx_data, tx_data_subs): (Vec<_>, Vec<_>) = (0..lanes)
            .map(|sequencer_id| {
                let (tx, rx) = unbounded();
                (tx, ChanTxDataSub { sequencer_id, rx })
            })
            .unzip();
        let (tx_ordering, b_rx) = unbounded();
        Self {
            tx_data,
            tx_ordering,
            pipeline: Pipeline::spawn(setup, tx_data_subs, ChanTxOrderingSub(b_rx)),
        }
    }

    /// Run the pipeline over `reader`, with no `tx_data` lane, as a binary
    /// with `--egress` runs. The drain ends when the commit thread stops,
    /// or when no receipt comes within the drain wait. Then the live
    /// stream of the egress closes, the reader stops, and the threads join.
    #[must_use]
    pub fn run_on_egress(setup: HarnessSetup, reader: EgressReader) -> HarnessOutcome {
        Self::run_on_source(setup, reader.sub, reader.live)
    }

    /// Run the pipeline over the `tx_ordering` source `source`, with no
    /// `tx_data` lane. The drain ends as in [`Self::run_on_egress`]. Then
    /// `inputs` ends, which closes the inputs of the source, the reader
    /// stops, and the threads join.
    #[must_use]
    pub fn run_on_source<O, I>(setup: HarnessSetup, source: O, inputs: I) -> HarnessOutcome
    where
        O: crate::TxOrderingSubscription + 'static,
    {
        Pipeline::spawn(setup, Vec::new(), source).finish_closing(inputs)
    }

    /// Feed every record of `input`, close the inputs, drain
    /// `tx_receipts`, and join the pipeline. `input.tx_data` holds one list
    /// per lane.
    ///
    /// # Panics
    ///
    /// Panics if `input.tx_data` has more than 255 lanes, if a record send
    /// fails, or if a pipeline thread panics.
    #[must_use]
    pub fn run(setup: HarnessSetup, input: HarnessInput) -> HarnessOutcome {
        let HarnessInput {
            tx_data,
            tx_ordering,
        } = input;
        let lanes = u8::try_from(tx_data.len()).expect("test fixture: at most 255 lanes");
        let harness = Self::spawn(setup, lanes);
        tx_data
            .into_iter()
            .zip(0..lanes)
            .flat_map(|(records, lane)| records.into_iter().map(move |rec| (lane, rec)))
            .for_each(|(lane, rec)| harness.send_tx_data(lane, rec));
        for rec in tx_ordering {
            harness.send_tx_ordering(rec);
        }
        harness.finish()
    }

    /// Send one record onto `tx_data` lane `lane`.
    ///
    /// # Panics
    ///
    /// Panics if `lane` is not a spawned lane, or if its reader has exited.
    pub fn send_tx_data(&self, lane: u8, rec: (crate::BPosition, KtTxEnvelope)) {
        self.tx_data[usize::from(lane)]
            .send(rec)
            .expect("tx_data reader still running");
    }

    /// Send one record onto `tx_ordering`.
    ///
    /// # Panics
    ///
    /// Panics if the `tx_ordering` reader has exited.
    pub fn send_tx_ordering(&self, rec: (crate::BPosition, crate::TxOrderingMessage)) {
        self.tx_ordering
            .send(rec)
            .expect("tx_ordering reader still running");
    }

    /// The `tx_receipts` stream, for a test that reads receipts while the
    /// engine runs.
    #[must_use]
    pub fn tx_receipts(&self) -> &Receiver<crate::CMessage> {
        &self.pipeline.tx_receipts
    }

    /// Close every input, drain the rest of `tx_receipts`, and join the
    /// pipeline in its safe order.
    ///
    /// # Panics
    ///
    /// Panics if a pipeline thread panics.
    #[must_use]
    pub fn finish(self) -> HarnessOutcome {
        self.close_inputs().finish()
    }

    /// End every input sender, so each reader sees EOF once it drains what
    /// was sent. The senders are the only fields that do not move out.
    fn close_inputs(self) -> Pipeline {
        self.pipeline
    }
}

/// The running pipeline: its `tx_receipts` stream, its threads, and the
/// shared state.
struct Pipeline {
    tx_receipts: Receiver<crate::CMessage>,
    threads: crate::Threads,
    state: crate::MockStateDatabase,
}

impl Pipeline {
    /// Spawn the pipeline over the `tx_data` lanes `tx_data` and the
    /// `tx_ordering` source `tx_ordering`, with one `tx_receipts`.
    fn spawn<O>(setup: HarnessSetup, tx_data: Vec<ChanTxDataSub>, tx_ordering: O) -> Self
    where
        O: crate::TxOrderingSubscription + 'static,
    {
        let HarnessSetup {
            cfg,
            start,
            snap,
            tx_hook,
        } = setup;
        let (c_tx, tx_receipts) = bounded(RECEIPTS_DEPTH);
        let threads = crate::Executor::<HarnessWiring<O>>::new(
            cfg,
            crate::Inbound {
                tx_data,
                tx_ordering,
                join_recovery: None,
                exec_stream: crate::reader::NoExecStream,
            },
            crate::Outbound {
                tx_receipts: ChanReceiptsPub(c_tx),
                snapshots: crate::MutatingSnapshotSource(snap.clone()),
                writer_signal: Imm,
                writer_queue: crate::WriterApplyingQueue::new(snap.clone()),
            },
            start,
            crate::RoleHooks {
                tx_hook,
                ..crate::RoleHooks::none()
            },
        )
        .spawn();
        Self {
            tx_receipts,
            threads,
            state: snap,
        }
    }

    /// Drain `tx_receipts`, then join. The drain runs first: a commit
    /// thread blocked on a full channel would never exit.
    fn finish(self) -> HarnessOutcome {
        let receipts = self.drain();
        self.join(receipts)
    }

    /// Drain `tx_receipts`, end the source inputs `inputs`, then join.
    fn finish_closing<I>(self, inputs: I) -> HarnessOutcome {
        let receipts = self.drain_closing(inputs);
        self.join(receipts)
    }

    /// Every receipt until the commit thread closes `tx_receipts`, or until
    /// none comes within [`RECEIPT_WAIT`].
    fn drain(&self) -> Vec<crate::CMessage> {
        std::iter::from_fn(|| self.tx_receipts.recv_timeout(RECEIPT_WAIT).ok()).collect()
    }

    /// [`Self::drain`]. The source inputs `_inputs` end when this returns.
    fn drain_closing<I>(&self, _inputs: I) -> Vec<crate::CMessage> {
        self.drain()
    }

    fn join(self, receipts: Vec<crate::CMessage>) -> HarnessOutcome {
        HarnessOutcome {
            result: self.threads.join(),
            receipts,
            state: self.state,
        }
    }
}
