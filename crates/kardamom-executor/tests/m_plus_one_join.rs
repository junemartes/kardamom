//! S4-arch-update (D-Sh12 / spec §2.4) integration tests for the M+1
//! channel-A/channel-B reader topology + join-by-ref semantics.
//!
//! Uses the `kardamom-log::testing::Fake*` in-memory pub/sub fakes so we
//! exercise the exact wire types (`TxEnvelope`, `TxOrderingMessage`,
//! `TxRef`) and rkyv codec the production Aeron path uses, without the
//! testcontainers dependency. Real-Aeron coverage of the same topology
//! lives in `tests/docker_aeron_e2e.rs`.
//!
//! Tests:
//! - `m4_canonical_b_order_drives_receipts`: M=4 sequencer fakes each
//!   publish 50 envelopes onto their channel A; channel B publishes the
//!   refs in an arbitrary interleaving. Assert the executor processes
//!   all 200 txs in B's canonical order and emits 200 receipts in the
//!   same order.
//! - `tx_ref_arriving_before_envelope_still_joins`: race test —
//!   simulates A-publisher lag of ~30 ms; the channel-B reader must
//!   spin and pick up the envelope once it lands.

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

use kardamom_executor::{
    BPosition, BlockBoundaryStart, CMessage, ChannelCPublication, Executor, ExecutorConfig,
    ExecutorError, MockStateDatabase, MutatingSnapshotSource, ReaderConfig, StateWriterSignal,
    TxDataSubscription, TxEnvelope as KtTxEnvelope, TxOrderingMessage, TxOrderingSubscription,
    TxRef, WriterApplyingQueue,
};
use kardamom_log::testing::{
    FakeBus, FakeChannelAPublication, FakeChannelBPublication, FakeTxDataSubscription,
    FakeTxOrderingSubscription,
};

/// Bridge a `FakeTxDataSubscription` into a `TxDataSubscription`. The
/// real-Aeron equivalent is `kardamom_log::TxDataSubscriber` driven by
/// `AeronRuntime::tx_data_subscriber(i)` on a dedicated OS thread.
struct FakeASubAdapter {
    sequencer_id: u8,
    sub: FakeTxDataSubscription,
    /// Set by the test driver once it has finished publishing; the
    /// subscription becomes "closed" when both the bus is drained AND this
    /// flag is set, mimicking Aeron's notion of an EOS.
    closed: Arc<AtomicBool>,
}

impl TxDataSubscription for FakeASubAdapter {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }

    fn next(&mut self) -> Result<(BPosition, KtTxEnvelope), ExecutorError> {
        loop {
            let mut out: Option<(BPosition, KtTxEnvelope)> = None;
            self.sub.poll(
                |pos, env| {
                    if out.is_none() {
                        out = Some((pos, env));
                    }
                },
                1,
            );
            if let Some(p) = out {
                return Ok(p);
            }
            if self.closed.load(Ordering::Acquire) {
                return Err(ExecutorError::ChannelAClosed {
                    sequencer_id: self.sequencer_id,
                });
            }
            thread::sleep(Duration::from_micros(50));
        }
    }
}

struct FakeBSubAdapter {
    sub: FakeTxOrderingSubscription,
    closed: Arc<AtomicBool>,
}
impl TxOrderingSubscription for FakeBSubAdapter {
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
                return Err(ExecutorError::ChannelBClosed);
            }
            thread::sleep(Duration::from_micros(50));
        }
    }
}

struct ChanCPub(Sender<CMessage>);
impl ChannelCPublication for ChanCPub {
    fn publish(&mut self, msg: CMessage) -> Result<(), ExecutorError> {
        self.0.send(msg).map_err(|_| ExecutorError::ChannelCClosed)
    }
}

struct Imm;
impl StateWriterSignal for Imm {
    fn wait_committed(&mut self, b: u64) -> Result<u64, ExecutorError> {
        Ok(b)
    }
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

#[test]
fn m4_canonical_b_order_drives_receipts() {
    const M: u8 = 4;
    const TXS_PER_SEQ: u64 = 50;
    const TOTAL: u64 = (M as u64) * TXS_PER_SEQ;

    // M signers, one per sequencer. Each publishes its own nonce-stream
    // of transfers — no inter-signer dependencies, so the executor's
    // sequential revm path can re-order them freely. The constraint we
    // assert is: receipts come out in **channel-B canonical order**.
    let signers: Vec<PrivateKeySigner> = (0..M)
        .map(|i| {
            PrivateKeySigner::from_bytes(&alloy_primitives::B256::repeat_byte(0xA0 + i)).unwrap()
        })
        .collect();
    let to = address!("00000000000000000000000000000000DEADBEEF");

    // Pre-fund every sender so transfers don't underflow.
    let mut snap_builder = MockStateDatabase::builder();
    for s in &signers {
        snap_builder =
            snap_builder.account(s.address(), U256::from(10u128.pow(18)), 0, KECCAK_EMPTY);
    }
    let snap = snap_builder.build();

    let bus = FakeBus::new();
    // Per-sequencer channel-A pub/sub pairs. Channel URI / stream-id match
    // the `ChannelsConfig::tx_dattx_data_channel_template` convention.
    let mut a_pubs: Vec<FakeChannelAPublication> = Vec::with_capacity(M as usize);
    let mut a_sub_handles: Vec<FakeTxDataSubscription> = Vec::with_capacity(M as usize);
    for sid in 0..M {
        let chan = format!("aeron:ipc?alias=a-{sid}");
        let stream_id = 2000 + (sid as i32);
        a_pubs.push(FakeChannelAPublication::open(&bus, sid, &chan, stream_id));
        a_sub_handles.push(FakeTxDataSubscription::open(&bus, &chan, stream_id));
    }
    let b_pub = FakeChannelBPublication::open(&bus, "aeron:ipc?alias=b", 1001);
    let b_sub_handle = FakeTxOrderingSubscription::open(&bus, "aeron:ipc?alias=b", 1001);

    // Phase 1: every sequencer publishes its envelopes onto channel A.
    // Record (sid, tx_data_position, tx_hash) so we can assert canonical order.
    let mut plan: Vec<(u8, BPosition, alloy_primitives::B256)> = Vec::new();
    let mut by_sid_nonce: Vec<u64> = vec![0; M as usize];
    // We want 50 txs per sequencer. Publish them sequencer-by-sequencer
    // for simplicity (the fake bus is single-publisher per stream).
    for sid in 0..M {
        for _ in 0..TXS_PER_SEQ {
            let nonce = by_sid_nonce[sid as usize];
            by_sid_nonce[sid as usize] += 1;
            let env = transfer(&signers[sid as usize], nonce, to);
            let h = env.tx_hash;
            let pos_a = a_pubs[sid as usize].publish(&env).expect("publish A");
            plan.push((sid, pos_a, h));
        }
    }
    assert_eq!(plan.len(), TOTAL as usize);

    // Phase 2: interleave the per-A plan into an arbitrary canonical
    // order and publish refs onto channel B. Per-sequencer FIFO must be
    // preserved (channel A is exclusive per publisher, refs land on B
    // in A-publish order — see spec §2.3), so the interleaving is a
    // **merge** of M ordered queues, not a global shuffle. Inter-stream
    // interleaving is chosen at random.
    let mut per_sid: Vec<std::collections::VecDeque<(u8, BPosition, alloy_primitives::B256)>> =
        vec![std::collections::VecDeque::new(); M as usize];
    for entry in &plan {
        per_sid[entry.0 as usize].push_back(*entry);
    }
    let mut rng = ChaCha8Rng::seed_from_u64(0xD15C0_u64);
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
    for (sid, pos_a, h) in &shuffled {
        b_pub
            .publish_ref(&TxRef::new(*h, *sid, *pos_a))
            .expect("publish ref");
    }
    // Sealer-emitted boundary: closes block 1 at the last B position.
    // The fake bus serializes appends, so the boundary's BPosition will
    // be after every TxRef; we synthesize end_tx_idx to match the LAST
    // ref's actual BPosition by snooping at the bus state via the
    // subscription poll. Simpler: open a peek subscription, drain to
    // last position. Even simpler: we don't have to assert end_tx_idx in
    // this test since the executor's misalignment check fires only if it
    // doesn't match; here we want it to match.
    //
    // Workaround: drain a temporary subscription to find the last
    // position the bus assigned to a TxRef.
    let mut peek = FakeTxOrderingSubscription::open(&bus, "aeron:ipc?alias=b", 1001);
    let mut last_pos: Option<BPosition> = None;
    peek.poll(
        |pos, _msg| {
            last_pos = Some(pos);
        },
        4096,
    );
    let end_tx_idx = last_pos.expect("at least one TxRef");
    b_pub
        .publish_boundary(&BlockBoundaryStart {
            block_number: 1,
            end_tx_idx,
            l2_timestamp: 1_700_000_000,
        })
        .expect("publish boundary");

    // Phase 3: run the executor. The subscription adapters spin until
    // the test sets `closed=true` after the bus is fully drained.
    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);

    let a_closed: Vec<Arc<AtomicBool>> = (0..M).map(|_| Arc::new(AtomicBool::new(false))).collect();
    let b_closed = Arc::new(AtomicBool::new(false));

    let mut a_subs: Vec<Box<dyn TxDataSubscription>> = Vec::with_capacity(M as usize);
    for (sid, (sub, closed)) in a_sub_handles
        .into_iter()
        .zip(a_closed.iter().cloned())
        .enumerate()
    {
        a_subs.push(Box::new(FakeASubAdapter {
            sequencer_id: sid as u8,
            sub,
            closed,
        }));
    }
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(FakeBSubAdapter {
        sub: b_sub_handle,
        closed: b_closed.clone(),
    });

    let (c_tx, c_rx) = bounded::<CMessage>(512);
    let cfg = ExecutorConfig {
        chain_id: 1,
        receipt_queue_depth: 512,
        ..Default::default()
    };

    // Mark each fake subscription "closed" after a small drain delay —
    // the entire bus is already populated so this just lets the
    // subscribers see EOF instead of blocking forever once they've
    // consumed everything.
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
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            ChanCPub(c_tx),
            snapshots,
            Imm,
            writer_q,
            0,
        )
    });

    let mut got_hashes: Vec<alloy_primitives::B256> = Vec::new();
    let mut boundaries = 0usize;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        match c_rx.recv_timeout(Duration::from_secs(5)) {
            Ok(CMessage::Receipt(r)) => {
                assert!(r.status, "tx should succeed (idx={})", got_hashes.len());
                got_hashes.push(r.tx_hash);
            }
            Ok(CMessage::BlockBoundary(_)) => {
                boundaries += 1;
                break; // single block — done
            }
            Err(_) => break,
        }
    }

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
        "receipts must be in channel-B canonical order"
    );
}

#[test]
fn tx_ref_arriving_before_envelope_still_joins() {
    // Single-sequencer mini-scenario: publish the ref onto channel B
    // immediately; delay the envelope on channel A by ~30 ms. The join
    // buffer's bounded wait should pick it up well within the default
    // 100 ms timeout.

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
    let a_pub = FakeChannelAPublication::open(&bus, 0, "aeron:ipc?alias=a-0", 2000);
    let a_sub_handle = FakeTxDataSubscription::open(&bus, "aeron:ipc?alias=a-0", 2000);
    let b_pub = FakeChannelBPublication::open(&bus, "aeron:ipc?alias=b", 1001);
    let b_sub_handle = FakeTxOrderingSubscription::open(&bus, "aeron:ipc?alias=b", 1001);

    let env = transfer(&signer, 0, to);
    let expected_hash = env.tx_hash;

    // Schedule the channel-A publish on a background thread so the
    // ref-then-envelope order is genuine.
    let bus_clone = bus.clone();
    let env_clone = env.clone();
    let a_inserter = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        let _a_pub_late = FakeChannelAPublication::open(&bus_clone, 0, "aeron:ipc?alias=a-0", 2000);
        // Use the original pub handle captured before; reopening would
        // grab the same underlying stream. Either works.
        let _ = _a_pub_late.publish(&env_clone).expect("publish A late");
    });

    // Meanwhile, immediately stake out the tx_data_position we'll claim in
    // the ref. The fake's `publish` advances `next_offset` by the
    // payload length, so we need to know what BPosition the A publish
    // will land at. The fake bus is fresh, so the first envelope's
    // start position is 0 → BPosition { term_id: 0, term_offset: 0 }.
    //
    // (If the fake's offset convention changes, we can also assert this
    // by checking what `a_pub.publish(...)` returned. For this test we
    // need a deterministic position to ref *before* the publish, so we
    // rely on the fake's well-defined zero-init.)
    drop(a_pub); // we won't use this handle; the inserter has its own.

    let tx_data_position = BPosition {
        term_id: 0,
        term_offset: 0,
    };
    b_pub
        .publish_ref(&TxRef::new(
            alloy_primitives::B256::ZERO,
            0,
            tx_data_position,
        ))
        .expect("publish ref");
    b_pub
        .publish_boundary(&BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: BPosition {
                term_id: 0,
                term_offset: 0,
            },
            l2_timestamp: 1_700_000_000,
        })
        .expect("publish boundary");

    let writer_q = WriterApplyingQueue::new(snap.clone());
    let snapshots = MutatingSnapshotSource(snap);

    let a_closed = Arc::new(AtomicBool::new(false));
    let b_closed = Arc::new(AtomicBool::new(false));
    let a_subs: Vec<Box<dyn TxDataSubscription>> = vec![Box::new(FakeASubAdapter {
        sequencer_id: 0,
        sub: a_sub_handle,
        closed: a_closed.clone(),
    })];
    let b_sub: Box<dyn TxOrderingSubscription> = Box::new(FakeBSubAdapter {
        sub: b_sub_handle,
        closed: b_closed.clone(),
    });
    let (c_tx, c_rx) = bounded::<CMessage>(8);

    // After the inserter has had time to fire and the executor has had
    // time to consume, signal "EOF" on both subscriptions so the
    // executor returns. We give a generous 500 ms.
    let a_closed_signaler = a_closed.clone();
    let b_closed_signaler = b_closed.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(500));
        a_closed_signaler.store(true, Ordering::Release);
        b_closed_signaler.store(true, Ordering::Release);
    });

    // Give the channel-B reader's join wait enough headroom even on slow CI.
    let cfg = ExecutorConfig {
        chain_id: 1,
        receipt_queue_depth: 8,
        reader: ReaderConfig {
            join_timeout: Duration::from_millis(500),
            join_poll_interval: Duration::from_micros(100),
            buffer_warn_threshold: 10_000,
        },
    };

    let join = thread::spawn(move || {
        Executor::run(
            cfg,
            a_subs,
            b_sub,
            ChanCPub(c_tx),
            snapshots,
            Imm,
            writer_q,
            0,
        )
    });

    let mut got_hashes = Vec::new();
    let mut boundaries = 0;
    while let Ok(m) = c_rx.recv_timeout(Duration::from_secs(5)) {
        match m {
            CMessage::Receipt(r) => got_hashes.push(r.tx_hash),
            CMessage::BlockBoundary(_) => {
                boundaries += 1;
                break;
            }
        }
    }

    a_inserter.join().unwrap();
    join.join().expect("no panic").expect("executor ok");
    assert_eq!(got_hashes, vec![expected_hash]);
    assert_eq!(boundaries, 1);
}
