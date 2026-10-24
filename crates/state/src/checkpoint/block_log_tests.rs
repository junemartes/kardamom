use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::Path;

use alloy_primitives::B256;
use kardamom_types::{BlockBoundary, BlockDelta, TxRef};

use super::{BLOCK_LOG_VERSION, BlockLog, BlockLogWriter, LogIdentity, Segment, SegmentHead};
use crate::error::StateError;
use crate::writer::WriteBatch;

const ID: LogIdentity = LogIdentity {
    genesis: B256::repeat_byte(0x11),
    schema: 3,
};

/// A segment limit that no test reaches.
const LARGE: NonZeroU64 = NonZeroU64::new(1 << 30).unwrap();

/// A batch of `block` with `refs` references, so the batches differ in
/// size and content.
fn batch(block: u64, refs: usize) -> WriteBatch {
    let boundary = BlockBoundary {
        block_number: block,
        l2_timestamp: block * 2,
        ..BlockBoundary::default()
    };
    let delta = BlockDelta {
        block_number: block,
        ..BlockDelta::default()
    };
    WriteBatch::with_refs(boundary, delta, vec![TxRef::default(); refs])
}

fn write(dir: &Path, limit: NonZeroU64, blocks: impl IntoIterator<Item = u64>) {
    let mut log = BlockLogWriter::open(dir, ID, limit).unwrap();
    blocks
        .into_iter()
        .for_each(|b| log.append(&batch(b, 3)).unwrap());
    log.sync().unwrap();
}

fn blocks_after(dir: &Path, base: u64, to: u64) -> Vec<u64> {
    let log = BlockLog::open(dir, ID).unwrap();
    log.after(base, to).map(|r| r.block).collect()
}

#[test]
fn a_batch_reads_back_as_it_was_written() {
    let dir = tempfile::tempdir().unwrap();
    let written = batch(7, 5);
    let mut log = BlockLogWriter::open(dir.path(), ID, LARGE).unwrap();
    log.append(&batch(6, 1)).unwrap();
    log.append(&written).unwrap();
    log.sync().unwrap();
    let read = BlockLog::open(dir.path(), ID).unwrap();
    let records: Vec<_> = read.after(6, u64::MAX).collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].batch().unwrap(), written);
}

#[test]
fn the_records_after_a_base_stop_at_the_end_block() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), LARGE, 1..=5);
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2, 3, 4, 5]);
    assert_eq!(blocks_after(dir.path(), 2, 4), [3, 4]);
    assert_eq!(blocks_after(dir.path(), 5, u64::MAX), [] as [u64; 0]);
}

/// A gap starts a new segment. The blocks before the gap are restore
/// points only for a base before the gap, and the blocks after it only
/// for a base after it.
#[test]
fn a_gap_ends_the_records() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), LARGE, [1, 2, 5, 6]);
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2]);
    assert_eq!(blocks_after(dir.path(), 3, u64::MAX), [] as [u64; 0]);
    assert_eq!(blocks_after(dir.path(), 4, u64::MAX), [5, 6]);
    let firsts: Vec<u64> = Segment::list(dir.path())
        .unwrap()
        .iter()
        .map(|s| s.first_block)
        .collect();
    assert_eq!(firsts, [1, 5]);
}

/// A full segment ends, and the next block starts the next one. The
/// records still chain across the segments.
#[test]
fn the_records_chain_across_full_segments() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), NonZeroU64::new(1).unwrap(), 1..=4);
    assert_eq!(Segment::list(dir.path()).unwrap().len(), 4);
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2, 3, 4]);
    assert_eq!(blocks_after(dir.path(), 2, u64::MAX), [3, 4]);
}

/// A crash tears the last record. The reader stops before it, and the
/// writer cuts it and writes that block again.
#[test]
fn the_writer_cuts_a_torn_record() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), LARGE, 1..=3);
    let path = Segment::list(dir.path()).unwrap()[0].path.clone();
    let len = std::fs::metadata(&path).unwrap().len();
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(len - 5).unwrap();
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2]);

    let mut log = BlockLogWriter::open(dir.path(), ID, LARGE).unwrap();
    assert_eq!(log.next_block(), Some(3));
    log.append(&batch(3, 3)).unwrap();
    log.append(&batch(4, 3)).unwrap();
    log.sync().unwrap();
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2, 3, 4]);
}

/// A changed byte fails the CRC. The reader stops before that record.
#[test]
fn a_bad_record_ends_the_records() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), LARGE, 1..=3);
    let path = Segment::list(dir.path()).unwrap()[0].path.clone();
    let len = std::fs::metadata(&path).unwrap().len();
    let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.seek(SeekFrom::Start(len - 1)).unwrap();
    file.write_all(&[0xFF]).unwrap();
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2]);
}

#[test]
fn a_block_below_the_next_block_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = BlockLogWriter::open(dir.path(), ID, LARGE).unwrap();
    log.append(&batch(1, 0)).unwrap();
    log.append(&batch(2, 0)).unwrap();
    let refused = log.append(&batch(2, 0));
    assert!(
        matches!(refused, Err(StateError::BlockLog { .. })),
        "{refused:?}"
    );
}

/// The segments of another chain or schema are not part of the log. The
/// writer leaves such a last segment as it is.
#[test]
fn a_segment_of_another_identity_is_not_in_the_log() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), LARGE, 1..=2);
    let other = LogIdentity {
        schema: ID.schema + 1,
        ..ID
    };
    let log = BlockLog::open(dir.path(), other).unwrap();
    assert_eq!(log.after(0, u64::MAX).count(), 0);
    let writer = BlockLogWriter::open(dir.path(), other, LARGE).unwrap();
    assert_eq!(writer.next_block(), None);
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2]);
}

#[test]
fn a_new_log_has_no_next_block() {
    let dir = tempfile::tempdir().unwrap();
    let log = BlockLogWriter::open(&dir.path().join("log"), ID, LARGE).unwrap();
    assert_eq!(log.next_block(), None);
    assert_eq!(
        blocks_after(&dir.path().join("missing"), 0, u64::MAX),
        [] as [u64; 0]
    );
}

#[test]
fn the_segment_head_reads_back() {
    let head = SegmentHead {
        identity: ID,
        first_block: 42,
    };
    assert_eq!(SegmentHead::decode(&head.encode()), Some(head));
}

#[test]
fn the_format_matches_the_registry() {
    use kardamom_formats::Registry;
    Registry::assert_exact("block-log", BLOCK_LOG_VERSION);
    Registry::assert_rkyv_layout::<WriteBatch>("block-log", "WriteBatch");
    let head = SegmentHead {
        identity: ID,
        first_block: 42,
    };
    let head = alloy_primitives::hex::encode(head.encode());
    Registry::assert_layout("block-log", "SegmentHead", &head);
}

/// A cut inside a segment drops the records after the block. The log then
/// continues with the next block, in the same segment.
#[test]
fn a_cut_drops_the_records_after_the_block() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), LARGE, 1..=5);
    let mut log = BlockLogWriter::open(dir.path(), ID, LARGE).unwrap();
    log.cut_after(3).unwrap();
    assert_eq!(log.next_block(), Some(4));
    let replaced = batch(4, 9);
    log.append(&replaced).unwrap();
    log.sync().unwrap();
    assert_eq!(blocks_after(dir.path(), 0, u64::MAX), [1, 2, 3, 4]);
    assert_eq!(Segment::list(dir.path()).unwrap().len(), 1);
    let read = BlockLog::open(dir.path(), ID).unwrap();
    assert_eq!(read.after(3, 4).next().unwrap().batch().unwrap(), replaced);
}
