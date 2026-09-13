//! The signed-legacy-transaction builder this crate's own fixtures use
//! ([`LegacyTx`]), and the channel-backed [`ChannelHarness`] that drives a
//! real [`crate::Executor::run`] pipeline in-process over channels
//! standing in for the wire.
//!
//! Most of `kardamom-validator` and `kardamom-executor`'s integration
//! tests still carry their own copy of the sign-and-wrap shape (sign a
//! `TxLegacy`, encode 2718, keccak the bytes, wrap in a `TxEnvelope`).
//! The `test-support` feature exists so those crates can depend on this
//! module instead; migrating each remaining copy is a follow-up, not
//! done yet. `kardamom-validator`'s `forged_envelope_chaos` test already
//! reaches [`ChannelHarness`] this way. This crate's own
//! `actor::test_support::legacy` already builds on [`LegacyTx::sign`].

use std::thread;
use std::time::Duration;

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, bounded};
use kardamom_types::TxEnvelope as KtTxEnvelope;

/// A legacy transfer's fixed fields, ready to sign.
pub struct LegacyTx {
    pub chain_id: u64,
    pub to: Address,
    pub nonce: u64,
    pub value: u64,
    pub gas_limit: u64,
    pub gas_price: u128,
}

impl LegacyTx {
    /// Sign this transaction with `signer`, wrapped as a
    /// `kardamom_types::TxEnvelope`. This matches what the proxy hands
    /// downstream, with `sender` and `tx_hash` stamped.
    ///
    /// # Panics
    ///
    /// Panics if `signer` cannot sign the built transaction. A
    /// `PrivateKeySigner` over a well-formed `TxLegacy` cannot fail this in
    /// practice; a fixture builder is the right place for this to stay a
    /// panic instead of a `Result` every caller must thread through.
    #[must_use]
    pub fn sign(self, signer: &PrivateKeySigner) -> KtTxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce: self.nonce,
            gas_price: self.gas_price,
            gas_limit: self.gas_limit,
            to: APTxKind::Call(self.to),
            value: U256::from(self.value),
            input: AlloyBytes::new(),
        };
        let sig = signer
            .sign_transaction_sync(&mut tx)
            .expect("a PrivateKeySigner signs a well-formed TxLegacy");
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        KtTxEnvelope {
            correlation_id: 0,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }
}

// ---------------------------------------------------------------------------
// Channel-backed engine harness.
// ---------------------------------------------------------------------------
//
// `kardamom-validator`'s `forged_envelope_chaos.rs`, and
// `kardamom-executor`'s `determinism.rs`, `diff_reference.rs`,
// `replay_integration.rs`, `m_plus_one_join.rs`, and the
// `sequential_throughput` bench, each carry their own copy of
// `ChanTxDataSub`, `ChanTxOrderingSub`, `ChanReceiptsPub`, a commit signal
// that reports every block as already durable, and the generic
// `EngineWiring` over those three channel doubles. This module is the one
// copy: [`Imm`] and [`TestWiring`] for a caller that drives the channels
// itself (a multi-block test that streams input while the engine runs),
// and [`ChannelHarness::run`] for a caller whose whole input is known
// upfront (build the `tx_data` and `tx_ordering` records, get back the
// engine result, the drained `tx_receipts` stream, and the shared state
// DB).

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

/// The channel-backed wiring [`ChannelHarness::run`] drives.
type HarnessWiring = TestWiring<ChanTxDataSub, ChanTxOrderingSub, ChanReceiptsPub>;

/// Send every item onto a fresh bounded channel, then close the sender so
/// the reader sees EOF. The sender ends with this function.
fn feed<T>(items: Vec<T>, what: &str) -> Receiver<T> {
    let (tx, rx) = bounded(items.len() + 1);
    for rec in items {
        tx.send(rec)
            .unwrap_or_else(|_| panic!("{what} receiver still open"));
    }
    rx
}

/// Grouped input for [`ChannelHarness::run`]: the executor config, the two
/// input streams in the order the engine must see them, and the starting
/// state snapshot.
pub struct HarnessInput {
    pub cfg: crate::ExecutorConfig,
    pub tx_data: Vec<(crate::BPosition, KtTxEnvelope)>,
    pub tx_ordering: Vec<(crate::BPosition, crate::TxOrderingMessage)>,
    pub snap: crate::MockStateDatabase,
}

/// [`ChannelHarness::run`]'s result: the engine loop's outcome, every
/// `tx_receipts` message drained while it ran, and the shared state after
/// the run.
pub struct HarnessOutcome {
    pub result: Result<(), crate::ExecutorError>,
    pub receipts: Vec<crate::CMessage>,
    pub state: crate::MockStateDatabase,
}

/// A whole-input-known-upfront run of the real [`crate::Executor::run`]
/// pipeline over channel-backed subscriptions: one `tx_data` sequencer,
/// one `tx_ordering`, one `tx_receipts`.
pub struct ChannelHarness;

impl ChannelHarness {
    /// Feed `input.tx_data` and `input.tx_ordering` onto their channels,
    /// close both, run the pipeline on its own thread with `input.snap` as
    /// the starting and shared post-run state, and drain `tx_receipts`
    /// until the engine loop returns.
    ///
    /// `tx_ordering` carries every `TxRef` and `BoundaryStart` the run
    /// needs, in the order the engine must see them; a caller that wants
    /// a resumed start, or more than one `tx_data` sequencer, drives
    /// [`ChanTxDataSub`]/[`ChanTxOrderingSub`]/[`ChanReceiptsPub`] and
    /// [`TestWiring`] directly instead.
    ///
    /// # Panics
    ///
    /// Panics if the engine loop's thread panics, or if the channel send
    /// of a `tx_data`/`tx_ordering` record fails (the receiver dropping
    /// before every record is sent would itself be a test bug).
    #[must_use]
    pub fn run(input: HarnessInput) -> HarnessOutcome {
        let HarnessInput {
            cfg,
            tx_data,
            tx_ordering,
            snap,
        } = input;
        let writer_queue = crate::WriterApplyingQueue::new(snap.clone());
        let snapshots = crate::MutatingSnapshotSource(snap.clone());

        let a_rx = feed(tx_data, "tx_data");
        let b_rx = feed(tx_ordering, "tx_ordering");
        let (c_tx, c_rx) = bounded(64);

        let tx_data_subs = vec![ChanTxDataSub {
            sequencer_id: 0,
            rx: a_rx,
        }];
        let h = thread::spawn(move || -> Result<(), crate::ExecutorError> {
            crate::Executor::<HarnessWiring>::new(
                cfg,
                crate::Inbound {
                    tx_data: tx_data_subs,
                    tx_ordering: ChanTxOrderingSub(b_rx),
                    join_recovery: None,
                },
                crate::Outbound {
                    tx_receipts: ChanReceiptsPub(c_tx),
                    snapshots,
                    writer_signal: Imm,
                    writer_queue,
                },
                crate::ResumePoint::GENESIS,
                crate::RoleHooks::none(),
            )
            .run()
        });

        let receipts: Vec<_> =
            std::iter::from_fn(|| c_rx.recv_timeout(Duration::from_secs(5)).ok()).collect();
        let result = h.join().expect("engine thread did not panic");
        HarnessOutcome {
            result,
            receipts,
            state: snap,
        }
    }
}
