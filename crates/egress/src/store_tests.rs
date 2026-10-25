use std::num::NonZeroU64;

use kardamom_cluster_adapter::wire::encode_egress_boundary;

use super::{Range, Store};
use crate::merge::{Head, Key};
use crate::ref_mark::RefMark;
use crate::test_support::{record, ref_record};

fn boundary(block: u64, end: u64) -> Vec<u8> {
    encode_egress_boundary(block, end, 0, 0)
}

/// Blocks 1..=n, two records each: r(2b-2) r(2b-1) b(b).
fn blocks(n: u64) -> Vec<Vec<u8>> {
    (1..=n)
        .flat_map(|b| [record(2 * b - 2), record(2 * b - 1), boundary(b, 2 * b)])
        .collect()
}

const GENESIS: Head = Head {
    next_index: 0,
    next_block: 1,
    last: None,
};

fn two_block_segments() -> NonZeroU64 {
    NonZeroU64::new(2).unwrap()
}

#[test]
fn a_reopened_store_has_the_head_of_its_frames() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&blocks(5)).unwrap();
    store.sync().unwrap();
    let head = store.head();
    assert_eq!(
        head,
        Head {
            next_index: 10,
            next_block: 6,
            last: Some(Key::Boundary(5)),
        }
    );
    drop(store);
    let again = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    assert_eq!(again.head(), head);
}

#[test]
fn a_reader_gets_the_frames_after_its_cursor_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    let all = blocks(5);
    store.append(&all).unwrap();
    // The cursor after block 1 and record 2: the rest of block 2 on.
    let Range::Frames {
        frames,
        after,
        head,
    } = store.read_from(3, 2, usize::MAX).unwrap()
    else {
        panic!("the store holds the cursor");
    };
    assert_eq!(frames, all[4..]);
    assert_eq!(head, store.head());
    assert_eq!(after, head, "the read reached the head");
}

#[test]
fn a_reader_gets_at_most_about_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&blocks(3)).unwrap();
    let Range::Frames { frames, after, .. } = store.read_from(0, 1, 1).unwrap() else {
        panic!("the store holds the cursor");
    };
    assert_eq!(frames, [record(0)], "one frame passes a limit of one byte");
    assert_eq!(
        (after.next_index, after.next_block),
        (1, 1),
        "the next read starts after record 0"
    );
}

#[test]
fn a_cursor_below_the_oldest_segment_is_unavailable() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&blocks(5)).unwrap();
    store.prune_below(4).unwrap();
    let floor = Head {
        next_index: 4,
        next_block: 3,
        last: Some(Key::Boundary(2)),
    };
    assert_eq!(
        store.read_from(0, 1, usize::MAX).unwrap(),
        Range::Unavailable {
            floor,
            mark: RefMark::default()
        }
    );
    let Range::Frames { frames, .. } = store.read_from(4, 3, usize::MAX).unwrap() else {
        panic!("the store keeps blocks 3 on");
    };
    assert_eq!(frames, blocks(5)[6..]);
}

#[test]
fn a_reopened_store_on_an_empty_segment_keeps_its_last_frame() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&blocks(2)).unwrap();
    let head = store.head();
    assert_eq!(
        head.last,
        Some(Key::Boundary(2)),
        "block 2 closed a segment"
    );
    drop(store);
    assert_eq!(
        Store::open(dir.path(), two_block_segments(), GENESIS)
            .unwrap()
            .head(),
        head,
        "the new segment names the frame before it"
    );
}

#[test]
fn a_torn_frame_at_the_end_is_cut_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), NonZeroU64::new(100).unwrap(), GENESIS).unwrap();
    store.append(&blocks(1)).unwrap();
    store.sync().unwrap();
    drop(store);
    let segment = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|e| e == "frames"))
        .unwrap();
    let mut torn = std::fs::read(&segment).unwrap();
    torn.extend_from_slice(&[9, 0, 0, 0, 1, 2]);
    std::fs::write(&segment, torn).unwrap();
    let mut again = Store::open(dir.path(), NonZeroU64::new(100).unwrap(), GENESIS).unwrap();
    assert_eq!(again.head().next_block, 2);
    again.append(&[record(2)]).unwrap();
    let Range::Frames { frames, .. } = again.read_from(2, 2, usize::MAX).unwrap() else {
        panic!("the store holds the cursor");
    };
    assert_eq!(frames, [record(2)], "the torn bytes are gone");
}

/// A reset drops every frame and starts the store at the new head. A
/// reader of an older cursor gets the new floor.
#[test]
fn a_reset_starts_the_store_at_the_new_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&blocks(3)).unwrap();
    let start = Head {
        next_index: 40,
        next_block: 21,
        last: Some(Key::Boundary(20)),
    };
    store.reset(start, RefMark::default()).unwrap();
    assert_eq!(store.head(), start);
    store
        .append(&[record(40), encode_egress_boundary(21, 41, 0, 0)])
        .unwrap();
    assert!(matches!(
        store.read_from(0, 1, 1 << 20).unwrap(),
        Range::Unavailable { floor, .. } if floor == start
    ));
    store.sync().unwrap();
    let reopened = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    assert_eq!(reopened.head().next_block, 22, "the reset is on disk");
}

/// Block 1 with record 0, a reference at index 1 and its boundary, then
/// blocks 2 to 5 with one record each.
fn with_a_reference() -> Vec<Vec<u8>> {
    let mut frames = vec![record(0), ref_record(1), boundary(1, 2)];
    frames.extend((2..=5).flat_map(|b| [record(b), boundary(b, b + 1)]));
    frames
}

const REFERENCE: RefMark = RefMark { index: 1, block: 1 };

#[test]
fn a_store_with_no_mark_opens_with_no_reference() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    assert_eq!(store.mark(), RefMark { index: 0, block: 0 });
    store.append(&blocks(3)).unwrap();
    assert_eq!(store.mark(), RefMark::default(), "no frame is a reference");
}

/// The reference is in the first segment, and the head comes from the
/// last one. The mark still names the reference after a restart.
#[test]
fn a_restart_reads_the_mark_of_a_reference_in_an_older_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&with_a_reference()).unwrap();
    assert_eq!(store.mark(), REFERENCE);
    store.append(&[record(6), boundary(6, 7)]).unwrap();
    assert_eq!(store.mark(), REFERENCE, "a batch with no reference keeps R");
    store.sync().unwrap();
    drop(store);
    let again = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    assert_eq!(again.head().next_block, 7);
    assert_eq!(
        again.mark(),
        REFERENCE.ahead(),
        "the file holds R plus the lead"
    );
}

/// The side file changes only when the mark passes it: a run of
/// references inside the lead writes it once.
#[test]
fn a_mark_inside_the_lead_does_not_write_the_side_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), NonZeroU64::new(1000).unwrap(), GENESIS).unwrap();
    let file = || RefMark::read(dir.path()).unwrap();
    store.append(&with_a_reference()).unwrap();
    assert_eq!(file(), REFERENCE.ahead());
    // Block b holds the reference at index b, from block 6 on.
    let refs = |blocks: std::ops::RangeInclusive<u64>| {
        blocks
            .flat_map(|b| [ref_record(b), boundary(b, b + 1)])
            .collect::<Vec<_>>()
    };
    let lead_end = REFERENCE.ahead().block;
    store.append(&refs(6..=lead_end)).unwrap();
    let inside = RefMark {
        index: lead_end,
        block: lead_end,
    };
    assert_eq!(store.mark(), inside, "R in memory is exact");
    assert_eq!(file(), REFERENCE.ahead(), "R stays inside the lead");
    store.append(&refs(lead_end + 1..=lead_end + 1)).unwrap();
    let past = RefMark {
        index: lead_end + 1,
        block: lead_end + 1,
    };
    assert_eq!(store.mark(), past);
    assert_eq!(file(), past.ahead(), "R passed the file, so the file moves");
}

/// Empty blocks move the block of the next reference past the lead while
/// its index stays inside it. The file then moves too.
#[test]
fn a_reference_past_the_lead_in_blocks_only_writes_the_side_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), NonZeroU64::new(1000).unwrap(), GENESIS).unwrap();
    store.append(&with_a_reference()).unwrap();
    let lead_end = REFERENCE.ahead().block;
    // Block b holds only its boundary, at index b, from block 6 on.
    let empty: Vec<_> = (6..=lead_end).map(|b| boundary(b, b + 1)).collect();
    store.append(&empty).unwrap();
    let block = lead_end + 1;
    store
        .append(&[ref_record(block), boundary(block, block + 1)])
        .unwrap();
    let top = RefMark {
        index: block,
        block,
    };
    assert!(
        top.index < REFERENCE.ahead().index,
        "the index is inside the lead"
    );
    assert_eq!(RefMark::read(dir.path()).unwrap(), top.ahead());
}

/// A clean stop writes the exact mark, so a restart after it reads no
/// lead.
#[test]
fn a_close_writes_the_exact_mark() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&with_a_reference()).unwrap();
    store.close().unwrap();
    drop(store);
    let again = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    assert_eq!(again.mark(), REFERENCE);
}

/// A reset keeps the mark, a higher mark of the peer raises it, and a
/// lower mark of the peer does not lower it.
#[test]
fn a_reset_keeps_or_raises_the_mark() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&with_a_reference()).unwrap();
    let start = |block: u64| Head {
        next_index: block * 10,
        next_block: block,
        last: Some(Key::Boundary(block - 1)),
    };
    store.reset(start(21), RefMark::default()).unwrap();
    assert_eq!(store.mark(), REFERENCE, "a reset with no peer keeps R");
    let higher = RefMark {
        index: 150,
        block: 20,
    };
    store.reset(start(31), higher).unwrap();
    assert_eq!(store.mark(), higher, "a higher mark of the peer raises R");
    store.reset(start(41), REFERENCE).unwrap();
    assert_eq!(store.mark(), higher, "a lower mark of the peer keeps R");
    drop(store);
    let again = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    assert!(higher.within(again.mark()), "the raised mark is on disk");
}

/// A crash after the mark and before the sync of the segment can lose the
/// frames of the batch. The mark then stays at or above every reference
/// that the segment still holds.
#[test]
fn a_crash_before_the_segment_sync_leaves_the_mark_at_or_above_the_frames() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), NonZeroU64::new(100).unwrap(), GENESIS).unwrap();
    store.append(&with_a_reference()).unwrap();
    drop(store);
    let segment = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|e| e == "frames"))
        .unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap();
    file.set_len(25).unwrap();
    let again = Store::open(dir.path(), NonZeroU64::new(100).unwrap(), GENESIS).unwrap();
    assert_eq!(again.head(), GENESIS, "the frames of the batch are lost");
    assert_eq!(
        again.mark(),
        REFERENCE.ahead(),
        "the mark is above the surviving frames"
    );
}

#[test]
fn a_mark_of_another_size_stops_the_open() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("ref-mark"), [1, 2, 3]).unwrap();
    assert!(Store::open(dir.path(), two_block_segments(), GENESIS).is_err());
}
