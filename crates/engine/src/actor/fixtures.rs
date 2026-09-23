//! The channel-backed [`ChannelHarness`] that runs the real
//! [`crate::Executor`] pipeline in-process over channels, and a re-export
//! of the signed-legacy-transaction fixture from `kardamom-test-support`.
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

pub use kardamom_test_support::{LegacyTx, anvil_signer_0, byte_signer, seeded_signer};

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
    fn next(
        &mut self,
    ) -> Result<(crate::BPosition, crate::TxOrderingMessage), crate::ExecutorError> {
        self.0
            .recv()
            .map_err(|_| crate::ExecutorError::TxOrderingClosed)
    }
}

/// A `tx_receipts` publication backed by a crossbeam channel, standing in
/// for the wire.
pub struct ChanReceiptsPub(pub Sender<crate::CMessage>);

impl crate::TxReceiptsPublication for ChanReceiptsPub {
    fn publish(&mut self, m: crate::CMessage) -> Result<(), crate::ExecutorError> {
        self.0
            .send(m)
            .map_err(|_| crate::ExecutorError::TxReceiptsClosed)
    }
}

/// The channel-backed wiring [`ChannelHarness`] drives.
type HarnessWiring = TestWiring<ChanTxDataSub, ChanTxOrderingSub, ChanReceiptsPub>;

/// Depth of the `tx_receipts` channel. The test thread drains it while the
/// engine runs, so this only bounds how far commit runs ahead of the drain.
const RECEIPTS_DEPTH: usize = 64;

/// How long a drain waits for the next receipt before it stops.
const RECEIPT_WAIT: Duration = Duration::from_secs(5);

/// What a harness run starts from: the executor config, the resume cursor,
/// and the starting state snapshot. The snapshot is also the shared
/// post-run state.
pub struct HarnessSetup {
    pub cfg: crate::ExecutorConfig,
    pub start: crate::ResumePoint,
    pub snap: crate::MockStateDatabase,
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
    tx_receipts: Receiver<crate::CMessage>,
    threads: crate::Threads,
    state: crate::MockStateDatabase,
}

impl ChannelHarness {
    /// Spawn the pipeline with `lanes` `tx_data` subscriptions (lane `i`
    /// carries `sequencer_id` `i`), one `tx_ordering`, and one
    /// `tx_receipts`. The input channels are unbounded, so a send never
    /// blocks the test thread.
    #[must_use]
    pub fn spawn(setup: HarnessSetup, lanes: u8) -> Self {
        let HarnessSetup { cfg, start, snap } = setup;
        let (tx_data, tx_data_subs): (Vec<_>, Vec<_>) = (0..lanes)
            .map(|sequencer_id| {
                let (tx, rx) = unbounded();
                (tx, ChanTxDataSub { sequencer_id, rx })
            })
            .unzip();
        let (tx_ordering, b_rx) = unbounded();
        let (c_tx, tx_receipts) = bounded(RECEIPTS_DEPTH);
        let threads = crate::Executor::<HarnessWiring>::new(
            cfg,
            crate::Inbound {
                tx_data: tx_data_subs,
                tx_ordering: ChanTxOrderingSub(b_rx),
                join_recovery: None,
            },
            crate::Outbound {
                tx_receipts: ChanReceiptsPub(c_tx),
                snapshots: crate::MutatingSnapshotSource(snap.clone()),
                writer_signal: Imm,
                writer_queue: crate::WriterApplyingQueue::new(snap.clone()),
            },
            start,
            crate::RoleHooks::none(),
        )
        .spawn();
        Self {
            tx_data,
            tx_ordering,
            tx_receipts,
            threads,
            state: snap,
        }
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
        &self.tx_receipts
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
    fn close_inputs(self) -> Draining {
        Draining {
            tx_receipts: self.tx_receipts,
            threads: self.threads,
            state: self.state,
        }
    }
}

/// A harness whose inputs are closed: what is left is draining the
/// receipts and joining the threads.
struct Draining {
    tx_receipts: Receiver<crate::CMessage>,
    threads: crate::Threads,
    state: crate::MockStateDatabase,
}

impl Draining {
    /// Drain `tx_receipts` until the commit thread closes it, then join.
    /// The drain runs first: a commit thread blocked on a full channel
    /// would never exit.
    fn finish(self) -> HarnessOutcome {
        let receipts: Vec<_> =
            std::iter::from_fn(|| self.tx_receipts.recv_timeout(RECEIPT_WAIT).ok()).collect();
        HarnessOutcome {
            result: self.threads.join(),
            receipts,
            state: self.state,
        }
    }
}
