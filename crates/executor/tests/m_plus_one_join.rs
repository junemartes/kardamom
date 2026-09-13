//! Integration tests for the M+1 `tx_data` and `tx_ordering` reader topology,
//! and join-by-ref semantics.
//!
//! Uses the `kardamom_log::testing::Fake*` in-memory pub/sub fakes, so
//! this exercises the exact wire types (`TxEnvelope`, `TxOrderingMessage`,
//! `TxRef`) and rkyv codec that the production Aeron path uses, without
//! the testcontainers dependency. Real-Aeron coverage of the same
//! topology lives in `tests/docker_aeron_e2e.rs`.
//!
//! Tests:
//! - `m4_canonical_b_order_drives_receipts`: four sequencer fakes each
//!   publish 50 envelopes onto their `tx_data`. `tx_ordering` publishes the
//!   refs in an arbitrary interleaving. The test checks that the
//!   executor processes all 200 txs in B's canonical order, and emits
//!   200 receipts in the same order.
//! - `tx_ref_arriving_before_envelope_still_joins`: a race test. It
//!   simulates about 30 ms of A-publisher lag. The `tx_ordering` reader
//!   must spin and pick up the envelope once it lands.
//!
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    reason = "indices here are bounded by the small fixed M and transaction counts this test uses, never near a truncation boundary"
)]

use std::num::NonZeroU64;
use std::num::NonZeroUsize;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256, address};
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::bounded;
use rand::SeedableRng;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use revm::primitives::KECCAK_EMPTY;

use kardamom_engine::actor::fixtures::{ChanReceiptsPub, Imm, LegacyTx, TestWiring};
use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, Executor, ExecutorConfig, ExecutorError, Inbound,
    MockStateDatabase, MutatingSnapshotSource, Outbound, ReaderConfig, ResumePoint, RoleHooks,
    TxDataSubscription, TxEnvelope, TxOrderingMessage, TxOrderingSubscription, TxRef,
    WriterApplyingQueue,
};
use kardamom_log::testing::{
    FakeBus, FakeTxDataPublication, FakeTxDataSubscription, FakeTxOrderingPublication,
    FakeTxOrderingSubscription,
};

const QUEUE_DEPTH_8: NonZeroUsize = NonZeroUsize::new(8).unwrap();
const QUEUE_DEPTH_512: NonZeroUsize = NonZeroUsize::new(512).unwrap();

/// Bridge a `FakeTxDataSubscription` into a `TxDataSubscription`. The
/// real-Aeron equivalent is `kardamom_log::TxDataSubscriber`, opened
/// directly on a dedicated OS thread (one Aeron client per thread,
/// because the client is `!Send + !Sync`).
struct FakeTxDataSubAdapter {
    sequencer_id: u8,
    sub: FakeTxDataSubscription,
    /// Set by the test driver once it has finished publishing. The
    /// subscription becomes "closed" when the bus is drained and this
    /// flag is set, mimicking Aeron's notion of an EOS.
    closed: Arc<AtomicBool>,
}

impl TxDataSubscription for FakeTxDataSubAdapter {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(kardamom_types::TxDataLoc, TxEnvelope), ExecutorError> {
        loop {
            let ControlFlow::Break(result) = self.poll_once() else {
                continue;
            };
            return result;
        }
    }
}

impl FakeTxDataSubAdapter {
    /// One poll attempt: if an item arrived, or the bus has closed,
    /// [`ControlFlow::Break`] with the result to return. Otherwise
    /// sleeps briefly and returns [`ControlFlow::Continue`], so the
    /// caller's loop polls again.
    fn poll_once(
        &mut self,
    ) -> ControlFlow<Result<(kardamom_types::TxDataLoc, TxEnvelope), ExecutorError>> {
        let mut out: Option<(kardamom_types::TxDataLoc, TxEnvelope)> = None;
        self.sub.poll(
            |loc, env| {
                if out.is_none() {
                    out = Some((loc, env));
                }
            },
            1,
        );
        if let Some(p) = out {
            return ControlFlow::Break(Ok(p));
        }
        if self.closed.load(Ordering::Acquire) {
            return ControlFlow::Break(Err(ExecutorError::TxDataClosed {
                sequencer_id: self.sequencer_id,
            }));
        }
        thread::sleep(Duration::from_micros(50));
        ControlFlow::Continue(())
    }
}

struct FakeTxOrderingSubAdapter {
    sub: FakeTxOrderingSubscription,
    closed: Arc<AtomicBool>,
}
impl TxOrderingSubscription for FakeTxOrderingSubAdapter {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        loop {
            let ControlFlow::Break(result) = self.poll_once() else {
                continue;
            };
            return result;
        }
    }
}

impl FakeTxOrderingSubAdapter {
    /// One poll attempt: if an item arrived, or the bus has closed,
    /// [`ControlFlow::Break`] with the result to return. Otherwise
    /// sleeps briefly and returns [`ControlFlow::Continue`], so the
    /// caller's loop polls again.
    fn poll_once(&mut self) -> ControlFlow<Result<(BPosition, TxOrderingMessage), ExecutorError>> {
        let mut out: Option<(BPosition, TxOrderingMessage)> = None;
        self.sub.poll(
            |pos, msg| {
                if out.is_none() {
                    out = Some((pos, msg));
                }
            },
            1,
        );
        if let Some(p) = out {
            return ControlFlow::Break(Ok(p));
        }
        if self.closed.load(Ordering::Acquire) {
            return ControlFlow::Break(Err(ExecutorError::TxOrderingClosed));
        }
        thread::sleep(Duration::from_micros(50));
        ControlFlow::Continue(())
    }
}

/// These tests' fake-bus wiring: the two fake-bus adapters, and one
/// `tx_receipts`.
type Wiring = TestWiring<FakeTxDataSubAdapter, FakeTxOrderingSubAdapter, ChanReceiptsPub>;

fn transfer(signer: &PrivateKeySigner, nonce: u64, to: Address) -> TxEnvelope {
    LegacyTx {
        chain_id: 1,
        to,
        nonce,
        value: 1,
        gas_limit: 21_000,
        gas_price: 0,
    }
    .sign(signer)
}

/// Build `m` signers and a snapshot that pre-funds every one of them,
/// so transfers do not underflow.
fn fund_m_signers(m: u8) -> (Vec<PrivateKeySigner>, MockStateDatabase) {
    let signers: Vec<PrivateKeySigner> = (0..m)
        .map(|i| {
            PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xA0 + i)).unwrap()
        })
        .collect();
    let mut snap_builder = MockStateDatabase::builder();
    for s in &signers {
        snap_builder =
            snap_builder.account(s.address(), U256::from(10u128.pow(18)), 0, KECCAK_EMPTY);
    }
    (signers, snap_builder.build())
}

/// The M+1 topology's pub/sub handles: `m` per-sequencer `tx_data`
/// pairs, plus the single shared `tx_ordering` pair.
struct M1Bus {
    a_pubs: Vec<FakeTxDataPublication>,
    tx_data_subs: Vec<FakeTxDataSubscription>,
    b_pub: FakeTxOrderingPublication,
    tx_ordering_sub: FakeTxOrderingSubscription,
}

/// Open `m` per-sequencer `tx_data` pub/sub pairs on `bus`, plus the
/// single shared `tx_ordering` pub/sub pair. The channel URI and
/// stream ID match the `ChannelsConfig::tx_data_channel_template`
/// convention.
fn open_m_plus_one_bus(bus: &FakeBus, m: u8) -> M1Bus {
    let (a_pubs, tx_data_subs): (Vec<FakeTxDataPublication>, Vec<FakeTxDataSubscription>) = (0..m)
        .map(|sid| {
            let chan = format!("aeron:ipc?alias=a-{sid}");
            let stream_id = 2000 + i32::from(sid);
            (
                FakeTxDataPublication::open(bus, sid, &chan, stream_id),
                FakeTxDataSubscription::open(bus, &chan, stream_id),
            )
        })
        .unzip();
    let b_pub = FakeTxOrderingPublication::open(bus, "aeron:ipc?alias=b", 1001);
    let tx_ordering_sub = FakeTxOrderingSubscription::open(bus, "aeron:ipc?alias=b", 1001);
    M1Bus {
        a_pubs,
        tx_data_subs,
        b_pub,
        tx_ordering_sub,
    }
}

/// One published transfer's ref: the sequencer it came from, its
/// `tx_data` position, and its `tx_hash`.
#[derive(Clone, Copy)]
struct PlannedRef {
    sid: u8,
    pos_a: BPosition,
    hash: alloy_primitives::B256,
}

/// Publish `txs_per_seq` transfers for each of `m` sequencers onto
/// `a_pubs`, in nonce order per sequencer. Returns
/// `(sid, tx_data_position, tx_hash)` for every published transfer, in
/// sequencer-major order.
fn publish_tx_data_plan(
    a_pubs: &[FakeTxDataPublication],
    signers: &[PrivateKeySigner],
    to: Address,
    m: u8,
    txs_per_seq: u64,
) -> Vec<PlannedRef> {
    let mut by_sid_nonce: Vec<u64> = vec![0; m as usize];
    (0..m)
        .flat_map(|sid| (0..txs_per_seq).map(move |_| sid))
        .map(|sid| {
            let nonce = by_sid_nonce[sid as usize];
            by_sid_nonce[sid as usize] += 1;
            let env = transfer(&signers[sid as usize], nonce, to);
            let h = env.tx_hash;
            let pos_a = a_pubs[sid as usize].publish(&env).expect("publish A");
            PlannedRef {
                sid,
                pos_a,
                hash: h,
            }
        })
        .collect()
}

/// Interleave `plan`'s per-sequencer entries into an arbitrary
/// canonical order. Per-sequencer FIFO order stays intact (`tx_data`
/// is exclusive per publisher, and refs land on B in A-publish order).
/// So this merges `m` ordered queues; only the interleaving between
/// queues is random.
fn shuffle_canonical_order(plan: &[PlannedRef], m: u8, seed: u64) -> Vec<PlannedRef> {
    let mut per_sid: Vec<std::collections::VecDeque<PlannedRef>> =
        vec![std::collections::VecDeque::new(); m as usize];
    for entry in plan {
        per_sid[entry.sid as usize].push_back(*entry);
    }
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut shuffled: Vec<PlannedRef> = Vec::with_capacity(plan.len());
    while shuffled.len() < plan.len() {
        shuffled.push(take_one(&mut per_sid, &mut rng));
    }
    shuffled
}

/// Pick one non-empty per-sequencer queue at random, and pop its front
/// entry.
fn take_one(
    per_sid: &mut [std::collections::VecDeque<PlannedRef>],
    rng: &mut ChaCha8Rng,
) -> PlannedRef {
    let live: Vec<usize> = per_sid
        .iter()
        .enumerate()
        .filter(|(_, q)| !q.is_empty())
        .map(|(i, _)| i)
        .collect();
    let pick = live[rng.random_range(0..live.len())];
    per_sid[pick].pop_front().unwrap()
}

/// Publish `shuffled`'s refs onto `b_pub`, then the sealer-emitted
/// boundary that closes block 1. `end_tx_idx` is the cumulative count
/// of canonical records through this block: the publisher- and
/// position-independent alignment key the executor matches against
/// its own processed-record count. Every plan entry is a distinct tx
/// (a unique sender and nonce give a unique `tx_hash`). So no dedup
/// drops happen, and the count is exactly `plan_len`.
fn publish_ordering_and_boundary(
    b_pub: &FakeTxOrderingPublication,
    shuffled: &[PlannedRef],
    plan_len: usize,
) {
    for r in shuffled {
        b_pub
            .publish_ref(&TxRef::new(r.hash, r.sid, r.pos_a, 0))
            .expect("publish ref");
    }
    let end_tx_idx = BPosition::from_index(plan_len as u64);
    b_pub
        .publish_boundary(&BlockBoundaryStart {
            block_number: 1,
            end_tx_idx,
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        })
        .expect("publish boundary");
}

/// The executor's join handle and receipt channel, from
/// [`spawn_m_plus_one_executor`].
struct RunHandles {
    c_rx: crossbeam_channel::Receiver<CMessage>,
    join: thread::JoinHandle<Result<(), ExecutorError>>,
}

/// Run the executor on its own thread, reading `tx_data_subs` and
/// `tx_ordering_sub` through the M+1 join, and writing into `snap`.
/// Also spawns the signaler thread that flips every closed flag after
/// a short drain delay. This lets the fake subscriptions see EOF once
/// the bus is fully drained, instead of blocking forever.
fn spawn_m_plus_one_executor(
    m: u8,
    snap: MockStateDatabase,
    tx_data_sub_handles: Vec<FakeTxDataSubscription>,
    tx_ordering_sub_handle: FakeTxOrderingSubscription,
) -> RunHandles {
    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);

    let a_closed: Vec<Arc<AtomicBool>> = (0..m).map(|_| Arc::new(AtomicBool::new(false))).collect();
    let b_closed = Arc::new(AtomicBool::new(false));

    let tx_data_subs: Vec<FakeTxDataSubAdapter> = tx_data_sub_handles
        .into_iter()
        .zip(a_closed.iter().cloned())
        .enumerate()
        .map(|(sid, (sub, closed))| FakeTxDataSubAdapter {
            sequencer_id: sid as u8,
            sub,
            closed,
        })
        .collect();
    let tx_ordering_sub = FakeTxOrderingSubAdapter {
        sub: tx_ordering_sub_handle,
        closed: b_closed.clone(),
    };

    let (c_tx, c_rx) = bounded::<CMessage>(512);
    let cfg = ExecutorConfig {
        chain_id: NonZeroU64::MIN,
        receipt_queue_depth: QUEUE_DEPTH_512,
        ..Default::default()
    };

    let a_closed_for_signaler = a_closed.clone();
    let b_closed_for_signaler = b_closed.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        for f in &a_closed_for_signaler {
            f.store(true, Ordering::Release);
        }
        b_closed_for_signaler.store(true, Ordering::Release);
    });

    let join = thread::spawn(move || {
        Executor::<Wiring>::new(
            cfg,
            Inbound {
                tx_data: tx_data_subs,
                tx_ordering: tx_ordering_sub,
                join_recovery: None,
            },
            Outbound {
                tx_receipts: ChanReceiptsPub(c_tx),
                snapshots,
                writer_signal: Imm,
                writer_queue: writer_q,
            },
            ResumePoint::GENESIS,
            RoleHooks::none(),
        )
        .run()
    });

    RunHandles { c_rx, join }
}

/// One [`collect_until_boundary`] poll's outcome.
enum PollOutcome {
    /// A receipt landed; keep polling.
    Continue,
    /// The block boundary landed, or the channel closed or timed out;
    /// stop polling.
    Stop,
}

/// Poll one message from `c_rx`. A receipt appends its hash to
/// `got_hashes`; a boundary increments `boundaries`.
fn poll_one_c_message(
    c_rx: &crossbeam_channel::Receiver<CMessage>,
    got_hashes: &mut Vec<alloy_primitives::B256>,
    boundaries: &mut usize,
) -> PollOutcome {
    match c_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(CMessage::Receipt(r)) => {
            assert!(r.status, "tx should succeed (idx={})", got_hashes.len());
            got_hashes.push(r.tx_hash);
            PollOutcome::Continue
        }
        Ok(CMessage::BlockBoundary(_)) => {
            *boundaries += 1;
            PollOutcome::Stop // one block, done
        }
        Err(_) => PollOutcome::Stop,
    }
}

/// Collect receipts from `c_rx` until the block boundary lands, the
/// channel closes, or `deadline` passes. Returns the received hashes,
/// in arrival order, and the boundary count.
fn collect_until_boundary(
    c_rx: &crossbeam_channel::Receiver<CMessage>,
    deadline: Instant,
) -> (Vec<alloy_primitives::B256>, usize) {
    let mut got_hashes = Vec::new();
    let mut boundaries = 0usize;
    while Instant::now() < deadline {
        let PollOutcome::Continue = poll_one_c_message(c_rx, &mut got_hashes, &mut boundaries)
        else {
            break;
        };
    }
    (got_hashes, boundaries)
}

#[path = "m_plus_one_join/tests.rs"]
mod tests;
