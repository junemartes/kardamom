//! Tests for the reader / join module.

use std::num::NonZeroU64;
use std::thread;
use std::time::Duration;

use super::join::{DedupWindow, TxDataKey};
use super::*;
use crate::error::ExecutorError;
use crate::exec_types::TxIndex;
use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use crossbeam_channel::bounded;
use kardamom_types::xchain::{RemoteEpochRecord, XChainMessage};
use kardamom_types::{
    BPosition, BlockBoundaryStart, Deposit, EpochRecord, TxDataLoc, TxEnvelope, TxOrderingMessage,
    TxRef,
};
use std::collections::VecDeque;

/// A fixed-destination, fixed-value transfer. Thin wrapper over
/// `actor::test_support::legacy`, the one signed-legacy-transfer fixture
/// this crate's tests share.
fn envelope(signer: &PrivateKeySigner, nonce: u64) -> TxEnvelope {
    crate::actor::test_support::legacy(signer, Address::from([0x22u8; 20]), nonce, 1)
}

fn pos(off: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: off,
    }
}

/// Drain a closed `ReaderToExec` receiver into a `Vec`, in arrival order.
fn drain(rx: &crossbeam_channel::Receiver<ReaderToExec>) -> Vec<ReaderToExec> {
    rx.iter().collect()
}

/// Assert that `got` is the `i`th deposit of an expanded epoch: it must
/// carry slot `1 + i` (deposits occupy slots 1..=N, in L1 log order) and
/// `expected`'s source hash.
fn assert_deposit_at(i: usize, expected: &Deposit, got: &ReaderToExec) {
    match got {
        ReaderToExec::Deposit {
            tx_idx, deposit, ..
        } => {
            assert_eq!(*tx_idx, TxIndex(1 + i as u64));
            assert_eq!(deposit.source_hash, expected.source_hash);
        }
        other => panic!("expected Deposit at {i}, got {other:?}"),
    }
}

/// Assert that `got` is the `i`th message of an expanded remote-epoch
/// record for `origin`: it must carry slot `1 + i` (its own slot
/// position, not the record's shared position — every position-keyed
/// receipt consumer needs a distinct position per message) and
/// `expected`'s source hash.
fn assert_xchain_at(i: usize, origin: u64, expected: &XChainMessage, got: &ReaderToExec) {
    match got {
        ReaderToExec::XChain {
            tx_idx,
            origin_chain_id,
            message,
            position,
        } => {
            assert_eq!(*tx_idx, TxIndex(1 + i as u64));
            assert_eq!(*origin_chain_id, origin);
            assert_eq!(message.source_hash, expected.source_hash);
            assert_eq!(
                *position,
                BPosition::from_index(1 + i as u64),
                "message {i} must carry its own slot position"
            );
        }
        other => panic!("expected XChain at {i}, got {other:?}"),
    }
}

/// In-memory `tx_data` subscription: a `VecDeque` of pre-baked
/// `(TxDataLoc, TxEnvelope)` records.
struct VecTxDataSub {
    sequencer_id: u8,
    queue: VecDeque<Result<(TxDataLoc, TxEnvelope), ExecutorError>>,
}
impl TxDataSubscription for VecTxDataSub {
    fn sequencer_id(&self) -> u8 {
        self.sequencer_id
    }
    fn next(&mut self) -> Result<(TxDataLoc, TxEnvelope), ExecutorError> {
        self.queue
            .pop_front()
            .unwrap_or(Err(ExecutorError::TxDataClosed {
                sequencer_id: self.sequencer_id,
            }))
    }
}

/// Build a `TxDataLoc` with session `0`. This is the single-publisher
/// default for tests that do not model concurrent ingress.
fn loc(off: i32) -> TxDataLoc {
    TxDataLoc::new(0, pos(off))
}

struct VecTxOrderingSub {
    queue: VecDeque<Result<(BPosition, TxOrderingMessage), ExecutorError>>,
}
impl TxOrderingSubscription for VecTxOrderingSub {
    fn next(&mut self) -> Result<(BPosition, TxOrderingMessage), ExecutorError> {
        self.queue
            .pop_front()
            .unwrap_or(Err(ExecutorError::TxOrderingClosed))
    }
}

/// Spawn the `tx_ordering` reader over `queue`, join it, and drain the
/// exec sink into a `Vec`, in arrival order. This is the spawn-join-drain
/// shape every `tx_ordering` unit test below drives, for a fixed,
/// literal message queue and a `buf` with no concurrent inserter.
///
/// # Errors
///
/// Returns the reader thread's error, if any (for example a
/// `JoinTimeout`, when `queue` names an envelope `buf` never receives).
fn run_ordering(
    queue: Vec<Result<(BPosition, TxOrderingMessage), ExecutorError>>,
    buf: JoinBuffer,
    cfg: ReaderConfig,
) -> Result<Vec<ReaderToExec>, ExecutorError> {
    let b = VecTxOrderingSub {
        queue: VecDeque::from(queue),
    };
    let (tx, rx) = bounded::<ReaderToExec>(8);
    let h = spawn_tx_ordering_reader(b, buf, cfg, tx, TxIndex::ZERO, None);
    h.join().expect("no panic")?;
    Ok(drain(&rx))
}

#[test]
fn channel_a_reader_drains_into_buffer() {
    let signer = PrivateKeySigner::random();
    let buf = JoinBuffer::new();
    let a = VecTxDataSub {
        sequencer_id: 3,
        queue: VecDeque::from(vec![
            Ok((loc(0), envelope(&signer, 0))),
            Ok((loc(100), envelope(&signer, 1))),
        ]),
    };
    let h = spawn_tx_data_reader(a, buf.clone());
    h.join().expect("no panic").expect("ok");
    assert_eq!(buf.len(), 2);
    assert!(buf.take(TxDataKey::new(3, 0, pos(0))).is_some());
    assert!(buf.take(TxDataKey::new(3, 0, pos(100))).is_some());
    assert_eq!(buf.len(), 0);
}

#[test]
fn channel_b_reader_emits_tx_and_boundary_in_canonical_order() {
    let signer = PrivateKeySigner::random();
    let buf = JoinBuffer::new();
    buf.insert(TxDataKey::new(0, 0, pos(0)), envelope(&signer, 0));
    buf.insert(TxDataKey::new(1, 0, pos(50)), envelope(&signer, 1));

    let out = run_ordering(
        vec![
            Ok((
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(
                    alloy_primitives::B256::repeat_byte(0xA1),
                    0,
                    pos(0),
                    0,
                )),
            )),
            Ok((
                pos(16),
                TxOrderingMessage::TxRef(TxRef::new(
                    alloy_primitives::B256::repeat_byte(0xA2),
                    1,
                    pos(50),
                    0,
                )),
            )),
            Ok((
                pos(32),
                TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                    block_number: 1,
                    end_tx_idx: pos(16),
                    l2_timestamp: 1_700_000_000,
                    l1_origin: 0,
                }),
            )),
        ],
        buf,
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 3);
    match &out[0] {
        ReaderToExec::Tx {
            tx_idx, position, ..
        } => {
            assert_eq!(*tx_idx, TxIndex(0));
            assert_eq!(*position, pos(0));
        }
        _ => panic!("expected Tx"),
    }
    match &out[1] {
        ReaderToExec::Tx {
            tx_idx, position, ..
        } => {
            assert_eq!(*tx_idx, TxIndex(1));
            assert_eq!(*position, pos(16));
        }
        _ => panic!("expected Tx"),
    }
    match &out[2] {
        ReaderToExec::Boundary(b) => {
            assert_eq!(b.block_number, 1);
            assert_eq!(b.end_tx_idx, pos(16));
        }
        _ => panic!("expected Boundary"),
    }
}

/// An epoch expands to the marker plus one dispatch per deposit. `tx_idx`
/// runs consecutively across the whole range. The exec side's boundary
/// alignment depends on that contiguity.
#[test]
fn channel_b_reader_expands_an_epoch_into_marker_plus_deposits() {
    let deposits: Vec<Deposit> = (0..3)
        .map(|i| Deposit {
            source_hash: alloy_primitives::B256::repeat_byte(0xD0 + i),
            mint: 1_000 + u128::from(i),
            ..Default::default()
        })
        .collect();
    let epoch = EpochRecord {
        l1_number: 4_242,
        l1_hash: alloy_primitives::B256::repeat_byte(0xE1),
        deposits: deposits.clone(),
    };

    let out = run_ordering(
        vec![
            Ok((pos(0), TxOrderingMessage::Epoch(epoch.clone()))),
            Ok((
                pos(4),
                TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                    block_number: 1,
                    // Marker + 3 deposits = 4 slots consumed.
                    end_tx_idx: pos(4),
                    l2_timestamp: 1_700_000_000,
                    l1_origin: 4_242,
                }),
            )),
        ],
        JoinBuffer::new(),
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 5, "marker + 3 deposits + boundary");
    match &out[0] {
        ReaderToExec::Epoch {
            tx_idx, epoch: e, ..
        } => {
            assert_eq!(*tx_idx, TxIndex(0));
            assert_eq!(e.l1_number, 4_242);
        }
        other => panic!("expected Epoch marker, got {other:?}"),
    }
    for (i, expected) in deposits.iter().enumerate() {
        assert_deposit_at(i, expected, &out[1 + i]);
    }
    match &out[4] {
        ReaderToExec::Boundary(b) => assert_eq!(b.end_tx_idx, pos(4)),
        other => panic!("expected Boundary, got {other:?}"),
    }
}

/// A duplicate epoch from a racing sequencer must dispatch nothing. A
/// second expansion would double-apply every deposit in it.
#[test]
fn channel_b_reader_drops_a_duplicate_epoch() {
    let epoch = EpochRecord {
        l1_number: 7,
        l1_hash: alloy_primitives::B256::repeat_byte(0xE2),
        deposits: vec![Deposit {
            source_hash: alloy_primitives::B256::repeat_byte(0xD9),
            mint: 5,
            ..Default::default()
        }],
    };
    let out = run_ordering(
        vec![
            Ok((pos(0), TxOrderingMessage::Epoch(epoch.clone()))),
            Ok((pos(2), TxOrderingMessage::Epoch(epoch))),
        ],
        JoinBuffer::new(),
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 2, "one marker + one deposit, not two of each");
}

fn remote_record(origin: u64, first_seq: u64, n: NonZeroU64) -> RemoteEpochRecord {
    let message_at = |seq: u64| XChainMessage {
        source_hash: kardamom_types::xchain::remote_source_hash(origin, seq),
        seq,
        gas_limit: 100_000,
        ..Default::default()
    };
    RemoteEpochRecord {
        origin_chain_id: origin,
        anchor_number: 40,
        anchor_hash: alloy_primitives::B256::repeat_byte(0xAB),
        first_seq,
        messages: kardamom_types::xchain::NonEmptyVec::new(
            message_at(first_seq),
            (first_seq + 1..first_seq + n.get())
                .map(message_at)
                .collect(),
        ),
    }
}

/// A remote epoch expands exactly like an L1 epoch: the marker plus one
/// dispatch per message, `tx_idx` contiguous across the whole range.
#[test]
fn channel_b_reader_expands_a_remote_epoch_into_marker_plus_messages() {
    let origin = 412_346u64;
    let rec = remote_record(origin, 5, NonZeroU64::new(2).expect("2 is nonzero"));
    let out = run_ordering(
        vec![
            Ok((pos(0), TxOrderingMessage::RemoteEpoch(rec.clone()))),
            Ok((
                pos(3),
                TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                    block_number: 1,
                    // Marker + 2 messages = 3 slots consumed.
                    end_tx_idx: pos(3),
                    l2_timestamp: 1_700_000_000,
                    l1_origin: 0,
                }),
            )),
        ],
        JoinBuffer::new(),
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 4, "marker + 2 messages + boundary");
    match &out[0] {
        ReaderToExec::RemoteEpoch { tx_idx, record, .. } => {
            assert_eq!(*tx_idx, TxIndex(0));
            assert_eq!(record.origin_chain_id, origin);
            assert_eq!(record.first_seq, 5);
        }
        other => panic!("expected RemoteEpoch marker, got {other:?}"),
    }
    for (i, expected) in rec.messages.iter().enumerate() {
        assert_xchain_at(i, origin, expected, &out[1 + i]);
    }
    match &out[3] {
        ReaderToExec::Boundary(b) => assert_eq!(b.end_tx_idx, pos(3)),
        other => panic!("expected Boundary, got {other:?}"),
    }
}

/// A duplicate remote epoch from a racing sequencer must dispatch
/// nothing. A second expansion would double-deliver every message.
#[test]
fn channel_b_reader_drops_a_duplicate_remote_epoch() {
    let rec = remote_record(412_346, 0, NonZeroU64::new(1).expect("1 is nonzero"));
    let out = run_ordering(
        vec![
            Ok((pos(0), TxOrderingMessage::RemoteEpoch(rec.clone()))),
            Ok((pos(2), TxOrderingMessage::RemoteEpoch(rec))),
        ],
        JoinBuffer::new(),
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 2, "one marker + one message, not two of each");
}

/// Race test: `TxRef` arrives before its envelope. The B reader spins,
/// and picks it up once the A reader inserts.
#[test]
fn channel_b_reader_tolerates_a_publisher_lag() {
    let signer = PrivateKeySigner::random();
    let buf = JoinBuffer::new();
    let env = envelope(&signer, 0);

    // Configure a generous timeout so the test passes even on slow CI.
    let cfg = ReaderConfig {
        join_timeout: Duration::from_millis(500),
        join_poll_interval: Duration::from_micros(100),
        ..ReaderConfig::default()
    };

    // TxOrdering has the ref ready immediately. TxData's insert is
    // delayed by a background thread.
    let buf_for_a = buf.clone();
    let env_clone = env.clone();
    let a_inserter = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        buf_for_a.insert(TxDataKey::new(2, 0, pos(0)), env_clone);
    });

    let b = VecTxOrderingSub {
        queue: VecDeque::from(vec![Ok((
            pos(0),
            TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 2, pos(0), 0)),
        ))]),
    };
    let (tx, rx) = bounded::<ReaderToExec>(2);
    let h = spawn_tx_ordering_reader(b, buf, cfg, tx, TxIndex::ZERO, None);
    h.join().expect("no panic").expect("ok");
    a_inserter.join().unwrap();

    let out = drain(&rx);
    assert_eq!(out.len(), 1);
    match &out[0] {
        ReaderToExec::Tx { envelope: e, .. } => assert_eq!(e.tx_hash, env.tx_hash),
        _ => panic!("expected Tx"),
    }
}

/// If the envelope never arrives, the `tx_ordering` reader propagates
/// `JoinTimeout`.
#[test]
fn channel_b_reader_join_timeout_aborts() {
    let buf = JoinBuffer::new();
    let cfg = ReaderConfig {
        join_timeout: Duration::from_millis(50),
        join_poll_interval: Duration::from_millis(5),
        ..ReaderConfig::default()
    };
    let res = run_ordering(
        vec![Ok((
            pos(0),
            TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 7, pos(0), 0)),
        ))],
        buf,
        cfg,
    );
    assert!(matches!(
        res,
        Err(ExecutorError::JoinTimeout {
            sequencer_id: 7,
            ..
        })
    ));
}

/// Duplicate `TxRef`s, from MDS racing-sequencer republications, collapse
/// to a single exec dispatch. The join-buffer entry is taken only once.
#[test]
fn channel_b_reader_dedups_racing_sequencer_txrefs() {
    let signer = PrivateKeySigner::random();
    let buf = JoinBuffer::new();
    let env = envelope(&signer, 0);
    buf.insert(TxDataKey::new(2, 0, pos(0)), env.clone());

    let dup = TxOrderingMessage::TxRef(TxRef::new(env.tx_hash, 2, pos(0), 0));
    let out = run_ordering(
        vec![
            Ok((pos(0), dup.clone())),
            Ok((pos(16), dup.clone())),
            Ok((pos(32), dup)),
        ],
        buf,
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 1, "P duplicates must collapse to one dispatch");
    match &out[0] {
        ReaderToExec::Tx { envelope: e, .. } => assert_eq!(e.tx_hash, env.tx_hash),
        _ => panic!("expected Tx"),
    }
}

#[test]
fn dedup_window_rejects_known_ids_and_evicts_fifo() {
    let id = |b: u8| alloy_primitives::B256::repeat_byte(b);
    let mut w = DedupWindow::new(std::num::NonZeroUsize::new(2).expect("2 is nonzero"));

    assert!(w.first_seen(id(1)));
    assert!(!w.first_seen(id(1)), "second sighting is a duplicate");
    assert!(w.first_seen(id(2)));
    // Window is [1, 2]; inserting 3 evicts 1 (oldest first).
    assert!(w.first_seen(id(3)));
    assert!(!w.first_seen(id(2)), "2 still inside the window");
    assert!(!w.first_seen(id(3)), "3 still inside the window");
    // 1 was evicted above, so it counts as fresh again (and its
    // insertion evicts 2, keeping the window at capacity).
    assert!(w.first_seen(id(1)), "evicted id is fresh again");
    assert_eq!(w.seen.len(), 2);
    assert_eq!(w.fifo.len(), 2);
}

/// Core proof for the I-A invariant. Under active/active ingress, two
/// publishers on one shard have independent Aeron term spaces. So they can
/// emit fragments at the same `(term_id, term_offset)`. The join key
/// carries `session_id`, so each `TxRef` still resolves to its own
/// envelope: no overwrite, no cross-wire. A key of only `(shard, position)`
/// would let the second insert overwrite the first, and the executor would
/// then join the wrong bytes.
#[test]
fn join_buffer_distinguishes_colliding_positions() {
    let signer = PrivateKeySigner::random();
    let buf = JoinBuffer::new();
    let env_a = envelope(&signer, 0);
    let env_b = envelope(&signer, 1);
    // Same shard, same BPosition, different publisher sessions.
    let p = pos(0);
    buf.insert(TxDataKey::new(3, 100, p), env_a.clone());
    buf.insert(TxDataKey::new(3, 200, p), env_b.clone());
    assert_eq!(buf.len(), 2, "distinct sessions must not collide");

    // Each session's take returns its own envelope.
    let got_a = buf
        .take(TxDataKey::new(3, 100, p))
        .expect("session 100 present");
    let got_b = buf
        .take(TxDataKey::new(3, 200, p))
        .expect("session 200 present");
    assert_eq!(got_a.tx_hash, env_a.tx_hash);
    assert_eq!(got_b.tx_hash, env_b.tx_hash);
    assert_eq!(buf.len(), 0);

    // A wrong-session lookup misses. A key of only `(shard, position)`
    // would silently return the wrong envelope instead.
    buf.insert(TxDataKey::new(3, 100, p), env_a.clone());
    assert!(
        buf.take(TxDataKey::new(3, 999, p)).is_none(),
        "wrong session must miss"
    );
    assert!(buf.take(TxDataKey::new(3, 100, p)).is_some());
}

/// I-A integration test, through the real reader threads. Two `tx_data`
/// fragments on one shard share the same `BPosition`, but have different
/// publisher sessions: the active/active collision. Two `TxRef`s, each
/// carrying its publisher's session, must each join the correct envelope,
/// in canonical order.
#[test]
fn reader_joins_two_sessions_at_same_position() {
    let signer = PrivateKeySigner::random();
    let env_a = envelope(&signer, 0);
    let env_b = envelope(&signer, 1);
    let buf = JoinBuffer::new();

    // One tx_data reader for shard 5, fed two colliding-position fragments
    // from two distinct sessions. This is what two active/active
    // ingresses produce.
    let a = VecTxDataSub {
        sequencer_id: 5,
        queue: VecDeque::from(vec![
            Ok((TxDataLoc::new(100, pos(0)), env_a.clone())),
            Ok((TxDataLoc::new(200, pos(0)), env_b.clone())),
        ]),
    };
    spawn_tx_data_reader(a, buf.clone())
        .join()
        .expect("no panic")
        .expect("ok");
    assert_eq!(buf.len(), 2, "distinct sessions must both be buffered");

    // Canonical order interleaves them: env_b's ref first, then env_a's.
    // Each ref carries its session, so the join keys on session, not
    // position.
    let out = run_ordering(
        vec![
            Ok((
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(env_b.tx_hash, 5, pos(0), 200)),
            )),
            Ok((
                pos(16),
                TxOrderingMessage::TxRef(TxRef::new(env_a.tx_hash, 5, pos(0), 100)),
            )),
        ],
        buf,
        ReaderConfig::default(),
    )
    .expect("ok");
    assert_eq!(out.len(), 2);
    match &out[0] {
        ReaderToExec::Tx { envelope: e, .. } => {
            assert_eq!(e.tx_hash, env_b.tx_hash, "first ref → session 200 envelope");
        }
        _ => panic!("expected Tx"),
    }
    match &out[1] {
        ReaderToExec::Tx { envelope: e, .. } => {
            assert_eq!(
                e.tx_hash, env_a.tx_hash,
                "second ref → session 100 envelope"
            );
        }
        _ => panic!("expected Tx"),
    }
}
