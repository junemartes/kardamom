use std::num::NonZeroU64;

use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};

use super::{Range, Store};
use crate::merge::{Head, Key};

fn record(index: u64) -> Vec<u8> {
    encode_egress_record(index, &[u8::try_from(index).unwrap(); 40]).unwrap()
}

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
    let Range::Frames { frames, head } = store.read_from(3, 2, usize::MAX).unwrap() else {
        panic!("the store holds the cursor");
    };
    assert_eq!(frames, all[4..]);
    assert_eq!(head, store.head());
}

#[test]
fn a_reader_gets_at_most_about_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path(), two_block_segments(), GENESIS).unwrap();
    store.append(&blocks(3)).unwrap();
    let Range::Frames { frames, .. } = store.read_from(0, 1, 1).unwrap() else {
        panic!("the store holds the cursor");
    };
    assert_eq!(frames, [record(0)], "one frame passes a limit of one byte");
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
        Range::Unavailable { floor }
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
        .next()
        .unwrap()
        .unwrap()
        .path();
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
