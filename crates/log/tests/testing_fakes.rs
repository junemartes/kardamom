#![cfg(feature = "testing")]

use std::collections::HashMap;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::testing::{
    FakeBus, FakeFsyncWatermarkStream, FakePublication, FakeTxDataPublication,
    FakeTxDataSubscription, FakeTxOrderingPublication, FakeTxOrderingSubscription,
    FakeTypedSubscription,
};
use kardamom_types::{
    BPosition, BlockBoundaryStart, FsyncWatermark, TxEnvelope, TxOrderingMessage, TxRef,
};

#[test]
fn fake_pub_sub_roundtrip() {
    let bus = FakeBus::new();
    let pubr = FakePublication::open(&bus, "test", 1);
    let mut sub = FakeTypedSubscription::<TxEnvelope>::open(&bus, "test", 1);

    let env = TxEnvelope {
        correlation_id: 42,
        raw_tx: Bytes::from_static(b"abc"),
        sender: Address::repeat_byte(0x11),
        tx_hash: B256::repeat_byte(0x22),
    };
    pubr.publish(&env).unwrap();

    let mut received: Vec<TxEnvelope> = Vec::new();
    sub.poll(|t, _| received.push(t), 16);
    assert_eq!(received, vec![env]);
}

#[test]
fn fake_fsync_watermark_stream_per_recorder() {
    let stream = FakeFsyncWatermarkStream::new();
    stream.publish(FsyncWatermark {
        recorder_id: 0,
        position: BPosition {
            term_id: 1,
            term_offset: 100,
        },
    });
    stream.publish(FsyncWatermark {
        recorder_id: 0,
        position: BPosition {
            term_id: 1,
            term_offset: 200,
        },
    });
    stream.publish(FsyncWatermark {
        recorder_id: 1,
        position: BPosition {
            term_id: 1,
            term_offset: 50,
        },
    });

    assert_eq!(stream.drain(0).len(), 2);
    assert_eq!(stream.drain(1).len(), 1);
    assert!(stream.drain(2).is_empty());
}

// ---------------------------------------------------------------------------
// TxData / TxOrdering fakes.
// ---------------------------------------------------------------------------

fn env(corr: u64, byte: u8) -> TxEnvelope {
    TxEnvelope {
        correlation_id: corr,
        raw_tx: Bytes::from(vec![byte; 32]),
        sender: Address::repeat_byte(byte),
        tx_hash: B256::repeat_byte(byte),
    }
}

#[test]
fn channel_a_publish_returns_tx_data_positionnd_subscription_yields_same_position() {
    let bus = FakeBus::new();
    let pubr = FakeTxDataPublication::open(&bus, /*seq=*/ 2, "aeron:ipc?alias=a-2", 2001);
    let mut sub = FakeTxDataSubscription::open(&bus, "aeron:ipc?alias=a-2", 2001);

    let p0 = pubr.publish(&env(1, 0x10)).unwrap();
    let p1 = pubr.publish(&env(2, 0x11)).unwrap();
    let p2 = pubr.publish(&env(3, 0x12)).unwrap();
    assert!(p0 < p1, "positions monotone p0={p0:?} p1={p1:?}");
    assert!(p1 < p2);

    let mut got: Vec<(BPosition, u64)> = Vec::new();
    sub.poll(|loc, e| got.push((loc.position, e.correlation_id)), 16);
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].0, p0);
    assert_eq!(got[1].0, p1);
    assert_eq!(got[2].0, p2);
    assert_eq!(got.iter().map(|x| x.1).collect::<Vec<_>>(), vec![1, 2, 3]);
}

#[test]
fn channel_b_carries_tx_refs_and_boundaries_in_publish_order() {
    let bus = FakeBus::new();
    let pubr = FakeTxOrderingPublication::open(&bus, "aeron:ipc?alias=b", 1001);
    let mut sub = FakeTxOrderingSubscription::open(&bus, "aeron:ipc?alias=b", 1001);

    let r1 = TxRef {
        tx_hash: alloy_primitives::B256::ZERO,
        shard_id: 0,
        tx_data_position: BPosition {
            term_id: 0,
            term_offset: 0,
        },
        tx_data_session_id: 0,
    };
    let r2 = TxRef {
        tx_hash: alloy_primitives::B256::ZERO,
        shard_id: 1,
        tx_data_position: BPosition {
            term_id: 0,
            term_offset: 64,
        },
        tx_data_session_id: 0,
    };
    let b = BlockBoundaryStart {
        block_number: 1,
        end_tx_idx: BPosition {
            term_id: 0,
            term_offset: 256,
        },
        l2_timestamp: 1_700_000_000,
        l1_origin: 0,
    };

    let pp1 = pubr.publish_ref(&r1).unwrap();
    let pp2 = pubr.publish_ref(&r2).unwrap();
    let pb = pubr.publish_boundary(&b).unwrap();
    assert!(pp1 < pp2);
    assert!(pp2 < pb);

    let mut got: Vec<TxOrderingMessage> = Vec::new();
    sub.poll(|_pos, m| got.push(m), 16);
    assert_eq!(got.len(), 3);
    assert_eq!(got[0], TxOrderingMessage::TxRef(r1));
    assert_eq!(got[1], TxOrderingMessage::TxRef(r2));
    assert_eq!(got[2], TxOrderingMessage::BoundaryStart(b));
}

/// Mini end-to-end of the executor's B-to-A join: A-readers buffer
/// envelopes keyed by `(sequencer_id, tx_data_position)`. The B-reader
/// walks the canonical order, looking up the envelope on each `TxRef`.
#[test]
fn b_reader_joins_against_a_buffer_in_canonical_order() {
    let bus = FakeBus::new();

    let a0_pub = FakeTxDataPublication::open(&bus, 0, "aeron:ipc?alias=a-0", 2001);
    let a1_pub = FakeTxDataPublication::open(&bus, 1, "aeron:ipc?alias=a-1", 2002);
    let b_pub = FakeTxOrderingPublication::open(&bus, "aeron:ipc?alias=b", 1001);

    // Two sequencers each publish two txs.
    let p_0a = a0_pub.publish(&env(100, 0x01)).unwrap();
    let p_1a = a1_pub.publish(&env(101, 0x02)).unwrap();
    let p_0b = a0_pub.publish(&env(102, 0x03)).unwrap();
    let p_1b = a1_pub.publish(&env(103, 0x04)).unwrap();

    // Canonical-orderer interleaving: 0a, 1a, 1b, 0b. Sequencer order is
    // not canonical; the B-stream is.
    let _ = b_pub
        .publish_ref(&TxRef {
            tx_hash: alloy_primitives::B256::ZERO,
            shard_id: 0,
            tx_data_position: p_0a,
            tx_data_session_id: 0,
        })
        .unwrap();
    let _ = b_pub
        .publish_ref(&TxRef {
            tx_hash: alloy_primitives::B256::ZERO,
            shard_id: 1,
            tx_data_position: p_1a,
            tx_data_session_id: 0,
        })
        .unwrap();
    let _ = b_pub
        .publish_ref(&TxRef {
            tx_hash: alloy_primitives::B256::ZERO,
            shard_id: 1,
            tx_data_position: p_1b,
            tx_data_session_id: 0,
        })
        .unwrap();
    let _ = b_pub
        .publish_ref(&TxRef {
            tx_hash: alloy_primitives::B256::ZERO,
            shard_id: 0,
            tx_data_position: p_0b,
            tx_data_session_id: 0,
        })
        .unwrap();

    // Drain both A-streams into the executor's per-A buffer.
    let mut a_buffer: HashMap<(u8, BPosition), TxEnvelope> = HashMap::new();
    let mut a0_sub = FakeTxDataSubscription::open(&bus, "aeron:ipc?alias=a-0", 2001);
    let mut a1_sub = FakeTxDataSubscription::open(&bus, "aeron:ipc?alias=a-1", 2002);
    a0_sub.poll(
        |loc, env| {
            a_buffer.insert((0, loc.position), env);
        },
        16,
    );
    a1_sub.poll(
        |loc, env| {
            a_buffer.insert((1, loc.position), env);
        },
        16,
    );

    // Walk B in canonical order, and check that this recovers the
    // canonical sequence of (sender, correlation_id).
    let mut tx_ordering_sub = FakeTxOrderingSubscription::open(&bus, "aeron:ipc?alias=b", 1001);
    let mut canonical: Vec<u64> = Vec::new();
    tx_ordering_sub.poll(
        |_b_pos, msg| {
            if let TxOrderingMessage::TxRef(r) = msg {
                let env = a_buffer
                    .remove(&(r.shard_id, r.tx_data_position))
                    .expect("ref must hit A-buffer");
                canonical.push(env.correlation_id);
            }
        },
        16,
    );

    assert_eq!(canonical, vec![100, 101, 103, 102]);
    assert!(
        a_buffer.is_empty(),
        "executor must evict referenced envelopes"
    );
}
