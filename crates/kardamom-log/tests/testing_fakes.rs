#![cfg(feature = "testing")]

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_log::testing::{
    FakeBus, FakeFsyncWatermarkStream, FakePublication, FakeTypedSubscription,
};
use kardamom_types::{BPosition, FsyncWatermark, TxEnvelope};

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
