//! A restarted executor must anchor its new copy in the existing egress head.

use std::num::NonZeroUsize;

use kardamom_egress::merge::{Head, Key, Merge, MergeConfig};

use super::{history, outcome, reader, request, serve};
use crate::reader::carried::tests::{boundary, done, entry, tx_at, tx_frame};
use crate::reader::{FrameTee, TxOrderingSubscription};

#[test]
fn restarted_copies_join_the_egress_without_skipping_an_unposted_block() {
    let mut merge = Merge::new(
        MergeConfig {
            max_waiting: NonZeroUsize::new(64).unwrap(),
            compared: NonZeroUsize::new(64).unwrap(),
        },
        Head {
            next_index: 3,
            next_block: 4,
            last: Some(Key::Boundary(3)),
        },
    );
    let mut delivered = Vec::new();
    for session in 1..=3 {
        restarted_copy(session, &mut merge, &mut delivered);
    }
    assert_eq!(delivered, [tx_at(3), boundary(4, 4)]);
    assert_eq!(merge.head().next_index, 4);
    assert_eq!(merge.head().next_block, 5);
}

fn restarted_copy(session: i32, merge: &mut Merge, delivered: &mut Vec<Vec<u8>>) {
    let (sub, feed) = reader(history((0, 1), 3));
    let (out, frames) = crossbeam_channel::bounded(64);
    let mut sub = sub.with_tee(FrameTee::new(out));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..4);
    let next = request(&mut sub, &feed);
    assert_eq!((next.from_index, next.from_block), (3, 4));
    feed.send(tx_at(3));
    feed.send(boundary(4, 4));
    feed.send(done(next.request_id, 4, 5));
    sub.next().unwrap();
    sub.next().unwrap();
    frames
        .try_iter()
        .for_each(|message| merge.take(session, &message, delivered));
}

#[test]
fn a_divergent_comparison_publishes_no_partial_history() {
    let (sub, feed) = reader(history((0, 1), 3));
    let (out, frames) = crossbeam_channel::bounded(64);
    let mut sub = sub.with_tee(FrameTee::new(out));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..3);
    feed.send(tx_frame(2, &entry(9)));
    feed.send(boundary(3, 3));
    feed.send(done(first.request_id, 3, 4));
    assert!(outcome(&mut sub).is_err());
    assert!(frames.try_recv().is_err());
}

#[test]
fn a_reconnected_comparison_discards_its_old_partial_frames() {
    let (sub, feed) = reader(history((0, 1), 3));
    let (out, frames) = crossbeam_channel::bounded(64);
    let mut sub = sub.with_tee(FrameTee::new(out));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..2);
    (0..10).for_each(|_| sub.step().unwrap());
    assert!(frames.try_recv().is_err(), "partial history stays private");
    feed.reconnect();
    let again = std::iter::repeat_with(|| request(&mut sub, &feed))
        .find(|r| (r.from_index, r.from_block) == (0, 1))
        .unwrap();
    let newest = feed.requests().pop().unwrap_or(again);
    serve(&feed, newest.request_id, 1..4);
    request(&mut sub, &feed);
    let want: Vec<_> = (0u64..)
        .zip((0..3).flat_map(|i| [tx_at(i), boundary(i + 1, i + 1)]))
        .map(|(seq, frame)| [seq.to_le_bytes().as_slice(), &frame].concat())
        .collect();
    assert_eq!(frames.try_iter().collect::<Vec<_>>(), want);
}

#[test]
fn a_skipped_comparison_does_not_publish_its_partial_frames() {
    let (sub, feed) = reader(history((0, 1), 3));
    let (out, frames) = crossbeam_channel::bounded(64);
    let mut sub = sub.with_tee(FrameTee::new(out));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..2);
    (0..10).for_each(|_| sub.step().unwrap());
    feed.send(crate::reader::carried::tests::unavailable(
        first.request_id,
        5,
        5,
    ));
    request(&mut sub, &feed);
    assert!(frames.try_recv().is_err());
}
