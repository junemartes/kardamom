//! A carry executor that stops while the window of its member moves past
//! its cursor. When it starts again, the member refuses the cursor below
//! its window. The reader then reads the range from the egress, which
//! keeps every block above the posted head. The executor executes each
//! missing block once, with the receipts and the state of a run that never
//! stopped, and asks for no peer checkpoint. Once its cursor is in the
//! window of the member, the reader goes back to the feed.

use std::thread::JoinHandle;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::{Receiver, Sender};
use kardamom_cluster_adapter::wire::{CanonicalTx, encode_egress_boundary};
use revm::primitives::KECCAK_EMPTY;

use super::CarriedSubscription;
use super::memory::{MemoryRemote, MemoryTransport};
use super::tests::{TIMING, done, tx_frame, unavailable};
use super::transport::Request;
use crate::actor::fixtures::{ChannelHarness, HarnessOutcome, HarnessSetup, TestEgress};
use crate::reader::cluster::ReplayCursor;
use crate::{
    CMessage, ExecutorConfig, ExecutorError, MockStateDatabase, ResumePoint, StateDatabase,
};

/// The blocks of the chain. Each holds one transfer.
const BLOCKS: u64 = 12;
/// The last block that the executor committed before it stopped.
const STOPPED_AT: u64 = 4;
/// The first block of the window of the member when the executor starts
/// again: above the cursor of the executor.
const WINDOW_FROM: u64 = 8;
const FIRST_TIMESTAMP: u64 = 1_700_000_000;
const RECIPIENT: Address = Address::repeat_byte(0x22);

/// The canonical stream: block `b` holds the transfer at index `b - 1`.
struct Chain {
    signer: PrivateKeySigner,
    /// Each frame with the cursor at which a replay starts with it.
    frames: Vec<((u64, u64), Vec<u8>)>,
}

impl Chain {
    fn new() -> Self {
        let signer = PrivateKeySigner::from_bytes(&[0x11; 32].into()).unwrap();
        let frames = (1..=BLOCKS)
            .flat_map(|block| {
                let index = block - 1;
                let envelope = crate::actor::test_support::legacy(&signer, RECIPIENT, index, 1);
                let tx = CanonicalTx {
                    canonical_id: envelope.tx_hash,
                    lane: 0,
                    envelope,
                };
                let end = encode_egress_boundary(block, block, FIRST_TIMESTAMP + block, 0);
                [
                    ((index, block), tx_frame(index, &tx)),
                    ((block, block), end),
                ]
            })
            .collect();
        Self { signer, frames }
    }

    /// The frames of the blocks through `last`.
    fn through(&self, last: u64) -> Vec<Vec<u8>> {
        self.frames
            .iter()
            .filter(|((_, block), _)| *block <= last)
            .map(|(_, frame)| frame.clone())
            .collect()
    }

    /// The genesis state: the signer has funds.
    fn genesis(&self) -> MockStateDatabase {
        MockStateDatabase::builder()
            .account(
                self.signer.address(),
                U256::from(10u128.pow(18)),
                0,
                KECCAK_EMPTY,
            )
            .build()
    }

    /// The accounts that the chain changes, as `(nonce, balance)`.
    fn accounts(&self, state: &MockStateDatabase) -> Vec<Option<(u64, U256)>> {
        [self.signer.address(), RECIPIENT]
            .iter()
            .map(|address| {
                state
                    .basic(*address)
                    .unwrap()
                    .map(|(nonce, balance, _)| (nonce, balance))
            })
            .collect()
    }
}

/// The start of a run of the executor.
fn setup(start: ResumePoint, snap: MockStateDatabase) -> HarnessSetup {
    HarnessSetup {
        cfg: ExecutorConfig::default(),
        start,
        snap,
        tx_hook: None,
    }
}

/// Each published message, as text: `CMessage` has no equality.
fn published(outcome: &HarnessOutcome) -> Vec<String> {
    outcome.receipts.iter().map(|m| format!("{m:?}")).collect()
}

/// Whether a published message belongs to a block after `block`.
fn after(message: &CMessage, block: u64) -> bool {
    match message {
        CMessage::Receipt(receipt) => receipt.tx_idx.as_index() >= block,
        CMessage::BlockBoundary(boundary) => boundary.block_number > block,
    }
}

/// The member feed of the executor. Its replay window starts at
/// `window_from`. It refuses a cursor below the window, and replays a
/// cursor in the window to its head.
struct Member {
    remote: MemoryRemote,
    frames: Vec<((u64, u64), Vec<u8>)>,
    window_from: (u64, u64),
    head: (u64, u64),
    stop: Receiver<()>,
    log: MemberLog,
}

/// The requests that the member refused and served.
#[derive(Default)]
struct MemberLog {
    refused: Vec<Request>,
    served: Vec<Request>,
}

impl Member {
    /// Spawn the member over the chain. It answers until `stop` ends.
    fn spawn(chain: &Chain, remote: MemoryRemote) -> (Sender<()>, JoinHandle<MemberLog>) {
        let (stop_tx, stop) = crossbeam_channel::bounded(0);
        let member = Self {
            remote,
            frames: chain.frames.clone(),
            window_from: (WINDOW_FROM - 1, WINDOW_FROM),
            head: (BLOCKS, BLOCKS + 1),
            stop,
            log: MemberLog::default(),
        };
        (stop_tx, std::thread::spawn(move || member.run()))
    }

    fn run(mut self) -> MemberLog {
        while self.answer_next() {}
        self.log
    }

    /// Answer the next request. Returns false once the test ends the
    /// member.
    fn answer_next(&mut self) -> bool {
        if let Err(crossbeam_channel::TryRecvError::Disconnected) = self.stop.try_recv() {
            return false;
        }
        if let Some(request) = self.remote.request(Duration::from_millis(5)) {
            self.answer(request);
        }
        true
    }

    fn answer(&mut self, request: Request) {
        let (index, block) = self.window_from;
        if request.from_block < block {
            self.remote
                .send(unavailable(request.request_id, index, block));
            self.log.refused.push(request);
            return;
        }
        self.frames
            .iter()
            .filter(|((at_index, at_block), _)| {
                *at_index >= request.from_index && *at_block >= request.from_block
            })
            .for_each(|(_, frame)| self.remote.send(frame.clone()));
        let (head_index, head_block) = self.head;
        self.remote
            .send(done(request.request_id, head_index, head_block));
        self.log.served.push(request);
    }
}

#[test]
fn a_cursor_below_the_window_of_the_member_resumes_from_the_egress_with_exact_output() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let chain = Chain::new();

    let whole_dir = tempfile::tempdir().unwrap();
    let whole = TestEgress::serve(
        &rt,
        whole_dir.path(),
        TestEgress::GENESIS,
        &chain.through(BLOCKS),
    );
    let reference = ChannelHarness::run_on_egress(
        setup(ResumePoint::GENESIS, chain.genesis()),
        whole.reader(&rt, ReplayCursor::new(0, 1)),
    );
    if let Err(e) = &reference.result {
        assert!(matches!(e, ExecutorError::TxOrderingClosed), "{e:?}");
    }

    // The executor commits the blocks through `STOPPED_AT`, then stops.
    let early_dir = tempfile::tempdir().unwrap();
    let early = TestEgress::serve(
        &rt,
        early_dir.path(),
        TestEgress::GENESIS,
        &chain.through(STOPPED_AT),
    );
    let stopped = ChannelHarness::run_on_egress(
        setup(ResumePoint::GENESIS, chain.genesis()),
        early.reader(&rt, ReplayCursor::new(0, 1)),
    );
    let (before, rest): (Vec<_>, Vec<_>) = reference
        .receipts
        .iter()
        .partition(|m| !after(m, STOPPED_AT));
    assert_eq!(
        published(&stopped),
        before.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>()
    );

    // While it is stopped, the window of the member moves past its cursor.
    // The egress keeps every block: no post covers them.
    let egress_dir = tempfile::tempdir().unwrap();
    let egress = TestEgress::serve(
        &rt,
        egress_dir.path(),
        TestEgress::GENESIS,
        &chain.through(BLOCKS),
    );
    let (feed, remote) = MemoryTransport::pair();
    let (stop, member) = Member::spawn(&chain, remote);
    let (service, live) = egress.transport(&rt);
    let cursor = ReplayCursor::new(STOPPED_AT, STOPPED_AT + 1);
    let resumed = ChannelHarness::run_on_source(
        setup(
            ResumePoint {
                block: STOPPED_AT,
                record_count: STOPPED_AT,
                l2_timestamp: FIRST_TIMESTAMP + STOPPED_AT,
                base_fee: 0,
                gas_used: 21_000,
            },
            stopped.state.clone(),
        ),
        CarriedSubscription::with_timing(feed, service, cursor, TIMING),
        (live, stop),
    );
    let log = member.join().unwrap();

    assert!(
        log.refused
            .iter()
            .any(|r| (r.from_index, r.from_block) == (STOPPED_AT, STOPPED_AT + 1)),
        "the member refuses the cursor of the executor below its window"
    );
    match &resumed.result {
        Ok(()) | Err(ExecutorError::TxOrderingClosed) => {}
        Err(e) => panic!("the executor resumes from the egress, not a peer checkpoint: {e:?}"),
    }
    assert_eq!(
        published(&resumed),
        rest.iter().map(|m| format!("{m:?}")).collect::<Vec<_>>(),
        "every missing block executes once, with the receipts of a run that never stopped"
    );
    assert_eq!(
        chain.accounts(&resumed.state),
        chain.accounts(&reference.state),
        "the state equals the state of a run that never stopped"
    );
    assert!(
        log.served.iter().any(|r| r.from_block >= WINDOW_FROM),
        "with its cursor in the window, the reader goes back to the member feed"
    );
}
