//! Offline segment-file reader tests.

use std::io::Write;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::archive_reader::{
    TxDataSegmentReader, TxOrderingSegmentReader, TypedSegmentReader, append_frame,
};
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage, TxRef};
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
    // TxOrdering carries `TxOrderingMessage::TxRef` and
    // `TxOrderingMessage::BoundaryStart` in canonical order.
    let mut buf = Vec::new();
    append_frame(
        &mut buf,
        pos(0),
        &TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 3, pos(128), 0)),
    );
    append_frame(
        &mut buf,
        pos(64),
        &TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
            block_number: 1,
            end_tx_idx: pos(64),
            l2_timestamp: 1234,
            l1_origin: 0,
        }),
    );

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = TxOrderingSegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 2);

    let TxOrderingMessage::TxRef(r) = &records[0].value else {
        panic!("expected ref first");
    };
    assert_eq!(r.shard_id, 3);
    assert_eq!(r.tx_data_position, pos(128));

    let TxOrderingMessage::BoundaryStart(b) = &records[1].value else {
        panic!("expected boundary second");
    };
    assert_eq!(b.block_number, 1);
}

#[test]
fn reads_per_sequencer_a_records() {
    // TxData carries raw `TxEnvelope` records — no enum wrapper.
    let mut buf = Vec::new();
    append_frame(&mut buf, pos(0), &tx(1));
    append_frame(&mut buf, pos(128), &tx(2));

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = TxDataSegmentReader::open(f.path()).unwrap();
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

    let reader = TxDataSegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn segment_path_uses_canonical_layout() {
    let dir = std::path::Path::new("/tmp/archive");
    let p = TypedSegmentReader::<TxOrderingMessage>::segment_path(dir, 5, 16777216);
    assert_eq!(p, std::path::Path::new("/tmp/archive/5-16777216.rec"));
}

#[test]
fn zeroed_header_with_data_behind_is_corruption() {
    // A wiped length field mid-file previously read as a clean live tail —
    // silent data loss. With real frames behind the zeroed header it must
    // surface as Corruption.
    let mut buf = Vec::new();
    append_frame(&mut buf, pos(0), &tx(1));
    let wipe_at = buf.len();
    append_frame(&mut buf, pos(128), &tx(2));
    buf[wipe_at..wipe_at + 4].fill(0); // zero the second frame's length

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = TxDataSegmentReader::open(f.path()).unwrap();
    let results: Vec<_> = reader.collect();
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    assert!(matches!(
        results[1],
        Err(kardamom_batcher::BatcherError::Corruption(_))
    ));
}

#[test]
fn zero_filled_tail_still_stops_cleanly() {
    // The legitimate case the corruption check must NOT flag: a pre-allocated
    // zero-filled tail after the last real frame.
    let mut buf = Vec::new();
    append_frame(&mut buf, pos(0), &tx(1));
    buf.extend_from_slice(&[0u8; 256]);

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = TxDataSegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 1);
}

#[test]
fn undersized_frame_length_is_corruption() {
    let mut buf = Vec::new();
    append_frame(&mut buf, pos(0), &tx(1));
    let at = buf.len();
    buf.extend_from_slice(&8u32.to_le_bytes()); // len 8 < header size 16
    buf.extend_from_slice(&[0xAB; 12]);
    let _ = at;

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&buf).unwrap();

    let reader = TxDataSegmentReader::open(f.path()).unwrap();
    let results: Vec<_> = reader.collect();
    assert!(matches!(
        results.last().unwrap(),
        Err(kardamom_batcher::BatcherError::Corruption(_))
    ));
}
