//! M-archive (channel B + per-sequencer channel A) offline reader tests.
//!
//! Covers:
//!   * happy-path in-order resolution (refs come on B after the env was
//!     written to A — the natural case the sequencer drives);
//!   * out-of-order open: refs on B reference A-positions that haven't been
//!     read yet (we pre-load the A index, so it works either way — the test
//!     pins this invariant down);
//!   * missing-A-archive surfaced as a `BatcherError::Config`;
//!   * canonical-ordering invariant: the output `position` field is the
//!     channel-B canonical position, not the A-archive position the envelope
//!     was fetched from.
//!
//! Also exercises the CLI spec parser.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use alloy_primitives::{Address, B256};
use bytes::Bytes;
use kardamom_batcher::archive_reader::append_frame;
use kardamom_batcher::error::BatcherError;
use kardamom_batcher::multi_archive_reader::{
    MultiArchiveConfig, MultiArchiveReader, ResolvedRecord,
};
use kardamom_types::{BPosition, BlockBoundaryStart, TxEnvelope, TxOrderingMessage, TxRef};
use tempfile::TempDir;

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

/// Build a temporary `.rec` file with `frames` written via `append_frame`.
fn write_segment<T>(dir: &TempDir, name: &str, frames: &[(BPosition, T)]) -> PathBuf
where
    T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
{
    let mut buf = Vec::new();
    for (p, v) in frames {
        append_frame(&mut buf, *p, v);
    }
    let path = dir.path().join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&buf).unwrap();
    path
}

#[test]
fn happy_path_in_order_resolution() {
    let dir = TempDir::new().unwrap();

    // Two sequencers each write two envelopes to their own A archive.
    let a0 = write_segment(&dir, "a0.rec", &[(pos(0), tx(10)), (pos(128), tx(11))]);
    let a1 = write_segment(&dir, "a1.rec", &[(pos(0), tx(20)), (pos(128), tx(21))]);

    // TxOrdering records the canonical order: interleaved refs from both
    // sequencers, then a boundary.
    let b = write_segment(
        &dir,
        "b.rec",
        &[
            (
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 0, pos(0))),
            ),
            (
                pos(16),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 1, pos(0))),
            ),
            (
                pos(32),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 0, pos(128))),
            ),
            (
                pos(48),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 1, pos(128))),
            ),
            (
                pos(64),
                TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                    block_number: 1,
                    end_tx_idx: pos(64),
                    l2_timestamp: 1_700_000_000,
                }),
            ),
        ],
    );

    let mut a_segments = HashMap::new();
    a_segments.insert(0u8, a0);
    a_segments.insert(1u8, a1);

    let reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment: b,
        a_segments,
    })
    .unwrap();

    assert_eq!(reader.a_archive_count(), 2);
    assert_eq!(reader.a_archive_len(0), 2);
    assert_eq!(reader.a_archive_len(1), 2);

    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 5);

    // Canonical order matches the channel-B walk; payload bytes resolved
    // from the correct A-archive.
    let expected_correlation = [10u64, 20, 11, 21];
    for (i, exp) in expected_correlation.iter().enumerate() {
        let ResolvedRecord::Tx {
            position,
            sequencer_id,
            env,
            ..
        } = &records[i]
        else {
            panic!("expected tx at idx {i}");
        };
        assert_eq!(env.correlation_id, *exp);
        // The B-canonical position is what the accumulator records.
        assert_eq!(*position, pos((i as i32) * 16));
        // sequencer_id matches the TxRef the batcher resolved against.
        assert_eq!(*sequencer_id, (i % 2) as u8);
    }

    let ResolvedRecord::Boundary { position, marker } = &records[4] else {
        panic!("expected boundary at end");
    };
    assert_eq!(*position, pos(64));
    assert_eq!(marker.block_number, 1);
}

#[test]
fn out_of_order_b_refs_a_positions_still_resolve() {
    // TxOrdering carries refs in the canonical order — which may point at
    // A-positions on different sequencers and in non-monotone A-position
    // order (a sequencer that wrote envelopes to its own A at positions 0,
    // 128, 256 may have those positions appear on B in the order 256, 128,
    // 0 if its earlier offers were back-pressured behind another
    // sequencer's). The pre-loaded per-A index resolves them regardless.
    let dir = TempDir::new().unwrap();

    let a0 = write_segment(
        &dir,
        "a0.rec",
        &[(pos(0), tx(100)), (pos(128), tx(101)), (pos(256), tx(102))],
    );

    // B references a0 in reverse A-order.
    let b = write_segment(
        &dir,
        "b.rec",
        &[
            (
                pos(0),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 0, pos(256))),
            ),
            (
                pos(16),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 0, pos(128))),
            ),
            (
                pos(32),
                TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 0, pos(0))),
            ),
            (
                pos(48),
                TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
                    block_number: 7,
                    end_tx_idx: pos(48),
                    l2_timestamp: 1_700_000_007,
                }),
            ),
        ],
    );

    let mut a_segments = HashMap::new();
    a_segments.insert(0u8, a0);
    let reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment: b,
        a_segments,
    })
    .unwrap();

    let records: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), 4);

    let expected = [102u64, 101, 100];
    for (i, exp) in expected.iter().enumerate() {
        let ResolvedRecord::Tx { env, .. } = &records[i] else {
            panic!("expected tx at idx {i}");
        };
        assert_eq!(
            env.correlation_id, *exp,
            "B record at idx {i} should have resolved to A's tx({exp})"
        );
    }

    let ResolvedRecord::Boundary { marker, .. } = &records[3] else {
        panic!("expected boundary at end");
    };
    assert_eq!(marker.block_number, 7);
}

#[test]
fn missing_a_archive_surfaces_as_config_error() {
    let dir = TempDir::new().unwrap();
    let b = write_segment(
        &dir,
        "b.rec",
        &[(
            pos(0),
            TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 99, pos(0))),
        )],
    );

    let reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment: b,
        a_segments: HashMap::new(),
    })
    .unwrap();
    let mut it = reader;
    let first = it.next().expect("one record");
    match first {
        Err(BatcherError::Config(msg)) => {
            assert!(msg.contains("sequencer_id=99"), "msg: {msg}");
        }
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn missing_a_position_surfaces_as_frame_error() {
    let dir = TempDir::new().unwrap();
    // A-archive has only position 0; B references position 9999.
    let a0 = write_segment(&dir, "a0.rec", &[(pos(0), tx(1))]);
    let b = write_segment(
        &dir,
        "b.rec",
        &[(
            pos(0),
            TxOrderingMessage::TxRef(TxRef::new(alloy_primitives::B256::ZERO, 0, pos(9999))),
        )],
    );

    let mut a_segments = HashMap::new();
    a_segments.insert(0u8, a0);
    let mut reader = MultiArchiveReader::open(&MultiArchiveConfig {
        b_segment: b,
        a_segments,
    })
    .unwrap();
    let first = reader.next().expect("one record");
    match first {
        Err(BatcherError::Frame(msg)) => {
            assert!(msg.contains("not found"), "msg: {msg}");
        }
        other => panic!("expected Frame error, got {other:?}"),
    }
}

#[test]
fn parse_a_spec_accepts_well_formed_entries() {
    let parsed =
        MultiArchiveConfig::parse_a_spec("0=/tmp/a0.rec, 1=/tmp/a1.rec ,2=/tmp/a2.rec").unwrap();
    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[&0], PathBuf::from("/tmp/a0.rec"));
    assert_eq!(parsed[&1], PathBuf::from("/tmp/a1.rec"));
    assert_eq!(parsed[&2], PathBuf::from("/tmp/a2.rec"));
}

#[test]
fn parse_a_spec_rejects_duplicates() {
    let err = MultiArchiveConfig::parse_a_spec("0=/tmp/a.rec,0=/tmp/b.rec").unwrap_err();
    match err {
        BatcherError::Config(msg) => assert!(msg.contains("listed twice"), "msg: {msg}"),
        other => panic!("expected Config error, got {other:?}"),
    }
}

#[test]
fn parse_a_spec_rejects_missing_separator() {
    let err = MultiArchiveConfig::parse_a_spec("not-a-real-entry").unwrap_err();
    matches!(err, BatcherError::Config(_));
}

#[test]
fn parse_a_spec_empty_returns_empty() {
    let parsed = MultiArchiveConfig::parse_a_spec("").unwrap();
    assert!(parsed.is_empty());
    let parsed = MultiArchiveConfig::parse_a_spec("  , ,  ").unwrap();
    assert!(parsed.is_empty());
}
