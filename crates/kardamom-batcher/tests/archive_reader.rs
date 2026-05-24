//! Offline segment-file reader tests.

use std::io::Write;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::archive_reader::{
    STREAM_KIND_BOUNDARY, STREAM_KIND_TX, SegmentReader, SegmentRecord, append_frame,
};
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope};
use tempfile::NamedTempFile;

fn build_segment(frames: &[(u8, BPosition, Either)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (kind, pos, value) in frames {
        match value {
            Either::Tx(env) => append_frame(&mut buf, *kind, *pos, env),
            Either::Boundary(b) => append_frame(&mut buf, *kind, *pos, b),
        }
    }
    buf
}

enum Either {
    Tx(TxEnvelope),
    Boundary(BlockBoundaryStart),
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
fn reads_two_interleaved_records() {
    let frames = vec![
        (
            STREAM_KIND_TX,
            BPosition {
                term_id: 0,
                term_offset: 0,
            },
            Either::Tx(tx(1)),
        ),
        (
            STREAM_KIND_BOUNDARY,
            BPosition {
                term_id: 0,
                term_offset: 64,
            },
            Either::Boundary(BlockBoundaryStart {
                block_number: 1,
                end_tx_idx: BPosition {
                    term_id: 0,
                    term_offset: 64,
                },
                l2_timestamp: 1234,
            }),
        ),
    ];
    let bytes = build_segment(&frames);

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&bytes).unwrap();

    let reader = SegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(records.len(), 2);
    match &records[0] {
        SegmentRecord::Tx { env, .. } => assert_eq!(env.correlation_id, 1),
        _ => panic!("expected tx first"),
    }
    match &records[1] {
        SegmentRecord::Boundary { marker, .. } => assert_eq!(marker.block_number, 1),
        _ => panic!("expected boundary second"),
    }
}

#[test]
fn truncated_active_segment_stops_cleanly() {
    let frames = vec![(
        STREAM_KIND_TX,
        BPosition {
            term_id: 0,
            term_offset: 0,
        },
        Either::Tx(tx(99)),
    )];
    let mut bytes = build_segment(&frames);
    bytes.extend_from_slice(&[0xFFu8; 4]); // partial frame header — not enough for full frame

    let mut f = NamedTempFile::new().unwrap();
    f.write_all(&bytes).unwrap();

    let reader = SegmentReader::open(f.path()).unwrap();
    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(records.len(), 1);
}

#[test]
fn segment_path_uses_canonical_layout() {
    let dir = std::path::Path::new("/tmp/archive");
    let p = SegmentReader::segment_path(dir, 5, 16777216);
    assert_eq!(p, std::path::Path::new("/tmp/archive/5-16777216.rec"));
}
