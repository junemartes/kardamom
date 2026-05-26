//! Offline segment-file reader tests (D-Sh12 typed-segment form).

use std::io::Write;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::archive_reader::{
    ChannelASegmentReader, ChannelBSegmentReader, TypedSegmentReader, append_frame,
};
use kardamom_types::{BPosition, BlockBoundaryStart, ChannelBMessage, TxEnvelope, TxRef};
use tempfile::NamedTempFile;

fn pos(o: i32) -> BPosition {
    BPosition {
        term_id: 0,
        term_offset: o,
    }
}

fn tx(correlation: u64) -> TxEnvelope {
    TxEnvelope {
        correlation_id: correlation,
        raw_tx: Bytes::from_static(b"raw-payload"),
        sender: Address::repeat_byte(0xAA),
        tx_hash: B256::repeat_byte(0xBB),
    }
}

#[test]
fn reads_two_interleaved_b_records() {
    // Channel B carries `ChannelBMessage::TxRef` and
    // `ChannelBMessage::BoundaryStart` in canonical order.
    let mut buf = Vec::new();
    append_frame(
        &mut buf,
        pos(0),
        &ChannelBMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 3, pos(128))),
    );
    append_frame(
        &mut buf,
        pos(64),
        &ChannelBMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(64),
            l2_timestamp: 1234,
        }),
    );

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = ChannelBSegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 2);

    let ChannelBMessage::TxRef(r) = &records[0].value else {
        panic!("expected ref first");
    };
    assert_eq!(r.shard_id, 3);
    assert_eq!(r.position_a, pos(128));

    let ChannelBMessage::BoundaryStart(b) = &records[1].value else {
        panic!("expected boundary second");
    };
    assert_eq!(b.block_number, 1);
}

#[test]
fn reads_per_sequencer_a_records() {
    // Channel A carries raw `TxEnvelope` records — no enum wrapper.
    let mut buf = Vec::new();
    append_frame(&mut buf, pos(0), &tx(1));
    append_frame(&mut buf, pos(128), &tx(2));

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = ChannelASegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].value.correlation_id, 1);
    assert_eq!(records[0].position, pos(0));
    assert_eq!(records[1].value.correlation_id, 2);
    assert_eq!(records[1].position, pos(128));
}

#[test]
fn truncated_active_segment_stops_cleanly() {
    let mut buf = Vec::new();
    append_frame(&mut buf, pos(0), &tx(99));
    buf.extend_from_slice(&[0xFFu8; 4]); // partial frame header — too short

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = ChannelASegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn segment_path_uses_canonical_layout() {
    let dir = std::path::Path::new("/tmp/archive");
    let p = TypedSegmentReader::<ChannelBMessage>::segment_path(dir, 5, 16777216);
    assert_eq!(p, std::path::Path::new("/tmp/archive/5-16777216.rec"));
}
