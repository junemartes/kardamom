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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{
    Address, Bytes as AlloyBytes, TxKind as APTxKind, U256, address, keccak256,
};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use crossbeam_channel::{Sender, bounded};
use rand::SeedableRng;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use revm::primitives::KECCAK_EMPTY;

use kardamom_engine::{
    BPosition, BlockBoundaryStart, CMessage, EngineWiring, Executor, ExecutorConfig, ExecutorError,
    Inbound, MockStateDatabase, MutatingSnapshotSource, NoEpochCheck, Outbound, ReaderConfig,
    ResumePoint, RoleHooks, StateWriterSignal, TxDataSubscription, TxEnvelope as KtTxEnvelope,
    TxOrderingMessage, TxOrderingSubscription, TxReceiptsPublication, TxRef, WriterApplyingQueue,
};
use kardamom_log::testing::{
    FakeBus, FakeTxDataPublication, FakeTxDataSubscription, FakeTxOrderingPublication,
    FakeTxOrderingSubscription,
};

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

    fn next(&mut self) -> Result<(kardamom_types::TxDataLoc, KtTxEnvelope), ExecutorError> {
        loop {
            let mut out: Option<(kardamom_types::TxDataLoc, KtTxEnvelope)> = None;
            self.sub.poll(
                |loc, env| {
                    if out.is_none() {
                        out = Some((loc, env));
                    }
                },
                1,
            );
            if let Some(p) = out {
                return Ok(p);
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(ExecutorError::TxDataClosed {
                    sequencer_id: self.sequencer_id,
                });
            }
            thread::sleep(Duration::from_micros(50));
        }
    }
}

struct FakeTxOrderingSubAdapter {
    sub: FakeTxOrderingSubscription,
    closed: Arc<AtomicBool>,
}
impl TxOrderingSubscription for FakeTxOrderingSubAdapter {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        loop {
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
                return Ok(p);
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(ExecutorError::TxOrderingClosed);
            }
            thread::sleep(Duration::from_micros(50));
        }
    }
}

struct ChanReceiptsPub(Sender<CMessage>);
impl TxReceiptsPublication for ChanReceiptsPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        self.0
            .send(msg)
            .map_err(|_| ExecutorError::TxReceiptsClosed)
    }
}

struct Imm;
impl StateWriterSignal for Imm {
    fn committed(&mut self) -> Result<u64, ExecutorError> {
        Ok(u64::MAX)
    }
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
}

/// Port types for these tests' fake-bus adapters.
struct TestWiring;
impl EngineWiring for TestWiring {
    type TxData = FakeTxDataSubAdapter;
    type TxOrdering = FakeTxOrderingSubAdapter;
    type TxReceipts = ChanReceiptsPub;
    type Snapshots = MutatingSnapshotSource;
    type WriterSignal = Imm;
    type WriterQueue = WriterApplyingQueue;
    type Epoch = NoEpochCheck;
}

fn transfer(signer: &PrivateKeySigner, nonce: u64, to: Address) -> KtTxEnvelope {
    let mut tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 0,
        gas_limit: 21_000,
        to: APTxKind::Call(to),
        value: U256::from(1u64),
        input: AlloyBytes::new(),
    };
    let sig = signer.sign_transaction_sync(&mut tx).unwrap();
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

/// Open `m` per-sequencer `tx_data` pub/sub pairs on `bus`, plus the
/// single shared `tx_ordering` pub/sub pair. The channel URI and
/// stream ID match the `ChannelsConfig::tx_data_channel_template`
/// convention.
fn open_m_plus_one_bus(
    bus: &FakeBus,
    m: u8,
) -> (
    Vec<FakeTxDataPublication>,
    Vec<FakeTxDataSubscription>,
    FakeTxOrderingPublication,
    FakeTxOrderingSubscription,
) {
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
    (a_pubs, tx_data_subs, b_pub, tx_ordering_sub)
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
) -> Vec<(u8, BPosition, alloy_primitives::B256)> {
    let mut by_sid_nonce: Vec<u64> = vec![0; m as usize];
    (0..m)
        .flat_map(|sid| (0..txs_per_seq).map(move |_| sid))
        .map(|sid| {
            let nonce = by_sid_nonce[sid as usize];
            by_sid_nonce[sid as usize] += 1;
            let env = transfer(&signers[sid as usize], nonce, to);
            let h = env.tx_hash;
            let pos_a = a_pubs[sid as usize].publish(&env).expect("publish A");
            (sid, pos_a, h)
        })
        .collect()
}

/// Interleave `plan`'s per-sequencer entries into an arbitrary
/// canonical order. Per-sequencer FIFO order stays intact (`tx_data`
/// is exclusive per publisher, and refs land on B in A-publish order).
/// So this merges `m` ordered queues; only the interleaving between
/// queues is random.
fn shuffle_canonical_order(
    plan: &[(u8, BPosition, alloy_primitives::B256)],
    m: u8,
    seed: u64,
) -> Vec<(u8, BPosition, alloy_primitives::B256)> {
    let mut per_sid: Vec<std::collections::VecDeque<(u8, BPosition, alloy_primitives::B256)>> =
        vec![std::collections::VecDeque::new(); m as usize];
    for entry in plan {
        per_sid[entry.0 as usize].push_back(*entry);
    }
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let mut shuffled: Vec<(u8, BPosition, alloy_primitives::B256)> = Vec::with_capacity(plan.len());
    while shuffled.len() < plan.len() {
        // Build the list of non-empty queues, pick one at random.
        let live: Vec<usize> = per_sid
            .iter()
            .enumerate()
            .filter_map(|(i, q)| if q.is_empty() { None } else { Some(i) })
            .collect();
        let pick = live[rng.random_range(0..live.len())];
        shuffled.push(per_sid[pick].pop_front().unwrap());
    }
    shuffled
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
    shuffled: &[(u8, BPosition, alloy_primitives::B256)],
    plan_len: usize,
) {
    for (sid, pos_a, h) in shuffled {
        b_pub
            .publish_ref(&TxRef::new(*h, *sid, *pos_a, 0))
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
        chain_id: 1,
        receipt_queue_depth: 512,
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
        Executor::run::<TestWiring>(
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

#[test]
fn m4_canonical_b_order_drives_receipts() {
    const M: u8 = 4;
    const TXS_PER_SEQ: u64 = 50;
    const TOTAL: u64 = (M as u64) * TXS_PER_SEQ;

    // M signers, one per sequencer. Each publishes its own nonce-stream of
    // transfers. There are no inter-signer dependencies, so the executor's
    // sequential revm path can reorder them freely. The test checks one
    // constraint: receipts come out in tx_ordering canonical order.
    let (signers, snap) = fund_m_signers(M);
    let to = address!("00000000000000000000000000000000DEADBEEF");

    let bus = FakeBus::new();
    let (a_pubs, tx_data_sub_handles, b_pub, tx_ordering_sub_handle) = open_m_plus_one_bus(&bus, M);

    // Phase 1: every sequencer publishes its envelopes onto tx_data.
    let plan = publish_tx_data_plan(&a_pubs, &signers, to, M, TXS_PER_SEQ);
    assert_eq!(plan.len(), TOTAL as usize);

    // Phase 2: interleave the per-A plan into an arbitrary canonical
    // order, and publish refs and the closing boundary onto tx_ordering.
    let shuffled = shuffle_canonical_order(&plan, M, 0xD15C0_u64);
    publish_ordering_and_boundary(&b_pub, &shuffled, plan.len());

    // Phase 3: run the executor. The subscription adapters spin until
    // the test sets `closed=true` after the bus is fully drained.
    let RunHandles { c_rx, join } =
        spawn_m_plus_one_executor(M, snap, tx_data_sub_handles, tx_ordering_sub_handle);

    let deadline = Instant::now() + Duration::from_secs(30);
    let (got_hashes, boundaries) = collect_until_boundary(&c_rx, deadline);

    join.join().expect("no panic").expect("executor ok");

    assert_eq!(
        got_hashes.len() as u64,
        TOTAL,
        "expected {TOTAL} receipts, got {}",
        got_hashes.len()
    );
    assert_eq!(boundaries, 1);

    let expected: Vec<alloy_primitives::B256> = shuffled.iter().map(|(_, _, h)| *h).collect();
    assert_eq!(
        got_hashes, expected,
        "receipts must be in tx_ordering canonical order"
    );
}

#[test]
fn tx_ref_arriving_before_envelope_still_joins() {
    // Single-sequencer mini-scenario: publish the ref onto tx_ordering
    // immediately, and delay the envelope on tx_data by about 30 ms. The
    // join buffer's bounded wait should pick it up well within the
    // default 100 ms timeout.

    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let snap = MockStateDatabase::builder()
        .account(
            signer.address(),
            U256::from(10u128.pow(18)),
            0,
            KECCAK_EMPTY,
        )
        .build();

    let bus = FakeBus::new();
    let tx_data_sub_handle = FakeTxDataSubscription::open(&bus, "aeron:ipc?alias=a-0", 2000);
    let b_pub = FakeTxOrderingPublication::open(&bus, "aeron:ipc?alias=b", 1001);
    let tx_ordering_sub_handle = FakeTxOrderingSubscription::open(&bus, "aeron:ipc?alias=b", 1001);

    let env = transfer(&signer, 0, to);
    let expected_hash = env.tx_hash;

    // Schedule the tx_data publish on a background thread so the
    // ref-then-envelope order is genuine.
    let bus_clone = bus.clone();
    let env_clone = env.clone();
    let a_inserter = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        let a_pub_late = FakeTxDataPublication::open(&bus_clone, 0, "aeron:ipc?alias=a-0", 2000);
        let _ = a_pub_late.publish(&env_clone).expect("publish A late");
    });

    // Meanwhile, stake out immediately the tx_data_position that the ref
    // will claim. The fake's `publish` advances `next_offset` by the
    // payload length, so the test needs to know what `BPosition` the A
    // publish will land at. The fake bus is fresh, so the first
    // envelope's start position is 0: `BPosition { term_id: 0,
    // term_offset: 0 }`.
    //
    // (If the fake's offset convention changes, the test could instead
    // check what the A publish returned. This test needs a
    // deterministic position to reference before the publish happens,
    // so it relies on the fake's well-defined zero-init.)

    let tx_data_position = BPosition {
        term_id: 0,
        term_offset: 0,
    };
    b_pub
        .publish_ref(&TxRef::new(
            alloy_primitives::B256::ZERO,
            0,
            tx_data_position,
            0,
        ))
        .expect("publish ref");
    b_pub
        .publish_boundary(&BlockBoundaryStart {
            block_number: 1,
            // One canonical record applied, so the cumulative count is 1.
            end_tx_idx: BPosition::from_index(1),
            l2_timestamp: 1_700_000_000,
            l1_origin: 0,
        })
        .expect("publish boundary");

    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);

    let a_closed = Arc::new(AtomicBool::new(false));
    let b_closed = Arc::new(AtomicBool::new(false));
    let tx_data_subs = vec![FakeTxDataSubAdapter {
        sequencer_id: 0,
        sub: tx_data_sub_handle,
        closed: a_closed.clone(),
    }];
    let tx_ordering_sub = FakeTxOrderingSubAdapter {
        sub: tx_ordering_sub_handle,
        closed: b_closed.clone(),
    };
    let (c_tx, c_rx) = bounded::<CMessage>(8);

    // After the inserter has had time to fire, and the executor has had
    // time to consume, signal "EOF" on both subscriptions, so the
    // executor returns. This gives a generous 500 ms.
    let a_closed_signaler = a_closed.clone();
    let b_closed_signaler = b_closed.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        a_closed_signaler.store(true, Ordering::Release);
        b_closed_signaler.store(true, Ordering::Release);
    });

    // Give the tx_ordering reader's join wait enough headroom even on slow CI.
    let cfg = ExecutorConfig {
        chain_id: 1,
        receipt_queue_depth: 8,
        reader: ReaderConfig {
            join_timeout: Duration::from_millis(500),
            join_poll_interval: Duration::from_micros(100),
            ..ReaderConfig::default()
        },
        ..ExecutorConfig::default()
    };

    let join = thread::spawn(move || {
        Executor::run::<TestWiring>(
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
    });

    let (got_hashes, boundaries) = collect_single_seq_until_boundary(&c_rx);

    a_inserter.join().unwrap();
    join.join().expect("no panic").expect("executor ok");
    assert_eq!(got_hashes, vec![expected_hash]);
    assert_eq!(boundaries, 1);
}

/// One [`collect_single_seq_until_boundary`] poll's outcome: keep
/// polling, or stop, either because the boundary landed or because the
/// channel timed out or closed.
enum SinglePoll {
    Continue,
    Stop,
}

/// Poll one message from `c_rx`. A receipt appends its hash to
/// `got_hashes`; a boundary increments `boundaries`.
fn poll_one_single_seq(
    c_rx: &crossbeam_channel::Receiver<CMessage>,
    got_hashes: &mut Vec<alloy_primitives::B256>,
    boundaries: &mut u32,
) -> SinglePoll {
    let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) else {
        return SinglePoll::Stop;
    };
    match m {
        CMessage::Receipt(r) => {
            got_hashes.push(r.tx_hash);
            SinglePoll::Continue
        }
        CMessage::BlockBoundary(_) => {
            *boundaries += 1;
            SinglePoll::Stop
        }
    }
}

/// Collect receipts from `c_rx` until the block boundary lands, or the
/// channel times out or closes. Returns the received hashes, in
/// arrival order, and the boundary count.
fn collect_single_seq_until_boundary(
    c_rx: &crossbeam_channel::Receiver<CMessage>,
) -> (Vec<alloy_primitives::B256>, u32) {
    let mut got_hashes = Vec::new();
    let mut boundaries = 0u32;
    while let SinglePoll::Continue = poll_one_single_seq(c_rx, &mut got_hashes, &mut boundaries) {}
    (got_hashes, boundaries)
}
