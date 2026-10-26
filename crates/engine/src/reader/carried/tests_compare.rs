//! The comparison of the stream with the committed history: equal
//! history, a divergence of a receipt and of a header, a head below the
//! floor, a refused floor, and a new connection.

use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use kardamom_cluster_adapter::wire::{EGRESS_KIND_REPLAY_AHEAD, encode_feed_answer};
use kardamom_state::{StateEnvBuilder, StateSnapshot, StateWriter, WriteBatch};
use kardamom_types::{BPosition, BlockBoundary, BlockBoundaryStart, BlockDelta, Receipt};

use super::tests::{TIMING, boundary, done, entry, epoch_at, tx_at, tx_frame, unavailable};
use super::*;

/// The committed header of `block` that ends at `end`, as `boundary`
/// encodes it.
fn header(block: u64, end: u64) -> BlockBoundaryStart {
    BlockBoundaryStart {
        block_number: block,
        end_tx_idx: BPosition::from_index(end),
        l2_timestamp: 0,
        l1_origin: 0,
    }
}

/// A history of blocks `1..=blocks` with one entry each, compared from
/// `floor`.
fn history(floor: (u64, u64), blocks: u64) -> LocalHistory {
    LocalHistory::of(
        floor,
        (blocks, blocks + 1),
        (1..=blocks).map(|b| header(b, b)),
        (0..blocks).map(|i| (i, entry(i).canonical_id)),
    )
}

/// A compared reader of the in-memory feed, with no second source.
fn reader(history: LocalHistory) -> (CarriedSubscription<MemoryTransport, NoSource>, MemoryRemote) {
    let (feed, remote) = MemoryTransport::pair();
    (
        CarriedSubscription::compared_with_timing(feed, NoSource, history, TIMING),
        remote,
    )
}

/// Run the reader until the feed gets a request.
fn request(
    sub: &mut CarriedSubscription<MemoryTransport, NoSource>,
    feed: &MemoryRemote,
) -> Request {
    let deadline = Instant::now() + Duration::from_secs(5);
    std::iter::repeat_with(|| {
        assert!(Instant::now() < deadline, "no request");
        sub.step().expect("a step of the reader");
        feed.request(Duration::ZERO)
    })
    .find_map(|got| got)
    .expect("a request")
}

/// Run the reader until it fails or delivers, and return the outcome.
fn outcome(
    sub: &mut CarriedSubscription<MemoryTransport, NoSource>,
) -> Result<(BPosition, Ordered), ExecutorError> {
    let deadline = Instant::now() + Duration::from_secs(5);
    std::iter::repeat_with(|| {
        assert!(Instant::now() < deadline, "no outcome");
        match sub.deliver() {
            Ok(Some(item)) => Some(Ok(item)),
            Ok(None) => sub.step().err().map(Err),
            Err(e) => Some(Err(e)),
        }
    })
    .find_map(|got| got)
    .expect("an outcome")
}

/// Send one block of one entry each, for `blocks`, then the replay end.
fn serve(feed: &MemoryRemote, id: u64, blocks: std::ops::Range<u64>) {
    blocks.clone().for_each(|b| {
        feed.send(tx_at(b - 1));
        feed.send(boundary(b, b));
    });
    feed.send(done(id, blocks.end - 1, blocks.end));
}

#[test]
fn an_equal_stream_delivers_from_the_cursor() {
    let (mut sub, feed) = reader(history((0, 1), 3));
    let first = request(&mut sub, &feed);
    assert_eq!(
        (first.from_index, first.from_block),
        (0, 1),
        "from the floor"
    );
    serve(&feed, first.request_id, 1..4);
    let second = request(&mut sub, &feed);
    assert_eq!(
        (second.from_index, second.from_block),
        (3, 4),
        "from the cursor"
    );
    feed.send(tx_at(3));
    feed.send(boundary(4, 4));
    feed.send(done(second.request_id, 4, 5));
    let (pos, item) = outcome(&mut sub).expect("an entry");
    assert_eq!(pos.as_index(), 3);
    assert!(matches!(item, Ordered::Tx(_)));
}

#[test]
fn another_entry_at_a_committed_index_is_the_divergence_point() {
    let mut diverged = history((0, 1), 3);
    diverged = LocalHistory::of(
        (0, 1),
        diverged.cursor(),
        (1..=3).map(|b| header(b, b)),
        [(0, entry(0).canonical_id), (1, B256::repeat_byte(9))],
    );
    let (mut sub, feed) = reader(diverged);
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..4);
    match outcome(&mut sub) {
        Err(ExecutorError::StreamDiverged {
            block,
            index,
            stream,
        }) => assert_eq!((block, index, stream), (2, 1, entry(1).canonical_id)),
        other => panic!("expected the divergence, got {other:?}"),
    }
}

#[test]
fn a_longer_block_is_the_divergence_point() {
    let (mut sub, feed) = reader(history((0, 1), 3));
    let first = request(&mut sub, &feed);
    feed.send(tx_at(0));
    feed.send(boundary(1, 1));
    feed.send(tx_at(1));
    feed.send(tx_at(2));
    feed.send(boundary(2, 3));
    feed.send(done(first.request_id, 3, 3));
    match outcome(&mut sub) {
        Err(ExecutorError::StreamDiverged { block, index, .. }) => {
            assert_eq!((block, index), (2, 3));
        }
        other => panic!("expected the divergence, got {other:?}"),
    }
}

#[test]
fn a_head_below_the_floor_asks_again_until_it_passes() {
    let (mut sub, feed) = reader(history((1, 2), 3));
    let first = request(&mut sub, &feed);
    assert_eq!((first.from_index, first.from_block), (1, 2));
    feed.send(encode_feed_answer(
        EGRESS_KIND_REPLAY_AHEAD,
        0,
        1,
        first.request_id,
    ));
    let again = request(&mut sub, &feed);
    assert_ne!(again.request_id, first.request_id);
    assert_eq!(
        (again.from_index, again.from_block),
        (1, 2),
        "the same floor"
    );
    serve(&feed, again.request_id, 2..4);
    let delivery = request(&mut sub, &feed);
    assert_eq!((delivery.from_index, delivery.from_block), (3, 4));
}

/// A floor that the source refuses inside its retention names no point
/// of its stream: the divergence can be below the window, and a wider
/// window can find it.
#[test]
fn a_refused_floor_inside_the_retention_is_below_the_window() {
    let (mut sub, feed) = reader(history((1, 2), 3));
    let first = request(&mut sub, &feed);
    feed.send(unavailable(first.request_id, 0, 1));
    match outcome(&mut sub) {
        Err(ExecutorError::DivergedBelowWindow { floor_block, widen }) => {
            assert_eq!((floor_block, widen), (2, true));
        }
        other => panic!("expected a divergence below the window, got {other:?}"),
    }
}

/// The first compared block differs: the divergence can be below it, so
/// the reader reports no divergence point. At the first block of the
/// chain, no wider window exists.
#[test]
fn a_difference_in_the_first_compared_block_is_below_the_window() {
    let below = |floor: (u64, u64)| {
        let (mut sub, feed) = reader(history(floor, 3));
        let first = request(&mut sub, &feed);
        feed.send(tx_frame(floor.0, &entry(floor.0 + 100)));
        feed.send(boundary(floor.1, floor.0 + 1));
        feed.send(done(first.request_id, floor.0 + 1, floor.1 + 1));
        match outcome(&mut sub) {
            Err(ExecutorError::DivergedBelowWindow { floor_block, widen }) => (floor_block, widen),
            other => panic!("expected a divergence below the window, got {other:?}"),
        }
    };
    assert_eq!(below((1, 2)), (2, true));
    assert_eq!(below((0, 1)), (1, false));
}

/// A floor that moved up to the retention of the source cannot widen: a
/// difference at the raised floor needs the operator.
#[test]
fn a_difference_at_a_raised_floor_cannot_widen() {
    let (mut sub, feed) = reader(history((0, 1), 3));
    let first = request(&mut sub, &feed);
    feed.send(unavailable(first.request_id, 2, 2));
    let raised = request(&mut sub, &feed);
    assert_eq!((raised.from_index, raised.from_block), (2, 3));
    feed.send(tx_frame(2, &entry(99)));
    feed.send(boundary(3, 3));
    feed.send(done(raised.request_id, 3, 4));
    match outcome(&mut sub) {
        Err(ExecutorError::DivergedBelowWindow { floor_block, widen }) => {
            assert_eq!((floor_block, widen), (3, false));
        }
        other => panic!("expected a divergence below the window, got {other:?}"),
    }
}

#[test]
fn a_floor_below_the_retention_moves_up() {
    let (mut sub, feed) = reader(history((0, 1), 3));
    let first = request(&mut sub, &feed);
    feed.send(unavailable(first.request_id, 2, 2));
    let raised = request(&mut sub, &feed);
    assert_eq!((raised.from_index, raised.from_block), (2, 3));
    serve(&feed, raised.request_id, 3..4);
    let delivery = request(&mut sub, &feed);
    assert_eq!((delivery.from_index, delivery.from_block), (3, 4));
}

#[test]
fn a_retention_above_the_window_skips_the_comparison() {
    let (mut sub, feed) = reader(history((0, 1), 3));
    let first = request(&mut sub, &feed);
    feed.send(unavailable(first.request_id, 5, 5));
    let delivery = request(&mut sub, &feed);
    assert_eq!((delivery.from_index, delivery.from_block), (3, 4));
}

#[test]
fn a_new_connection_during_the_comparison_starts_it_again() {
    let (mut sub, feed) = reader(history((0, 1), 3));
    let first = request(&mut sub, &feed);
    feed.send(tx_at(0));
    feed.send(boundary(1, 1));
    feed.send(done(first.request_id, 1, 2));
    (0..10).for_each(|_| sub.step().expect("a step of the reader"));
    feed.reconnect();
    let again = std::iter::repeat_with(|| request(&mut sub, &feed))
        .find(|r| (r.from_index, r.from_block) == (0, 1))
        .expect("a request from the floor");
    let newest = feed.requests().pop().unwrap_or(again);
    assert_eq!((newest.from_index, newest.from_block), (0, 1));
    serve(&feed, newest.request_id, 1..4);
    let delivery = request(&mut sub, &feed);
    assert_eq!((delivery.from_index, delivery.from_block), (3, 4));
}

#[test]
fn a_new_connection_after_the_comparison_restarts_the_run() {
    let (mut sub, feed) = reader(history((0, 1), 3));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..4);
    request(&mut sub, &feed);
    feed.reconnect();
    match outcome(&mut sub) {
        Err(ExecutorError::SourceRestarted {
            next_index,
            next_block,
        }) => assert_eq!((next_index, next_block), (3, 4)),
        other => panic!("expected the restart from the state DB, got {other:?}"),
    }
}

/// Commit blocks `1..=blocks` with one receipt each, and read the history
/// of a window from the database.
#[test]
fn the_history_reads_the_window_from_the_state_db() {
    let dir = tempfile::tempdir().unwrap();
    let env = StateEnvBuilder::new(dir.path()).open().unwrap();
    let mut writer = StateWriter::spawn(env.clone()).unwrap();
    (1..=5u64).for_each(|b| {
        let receipt = Receipt {
            tx_idx: BPosition::from_index(b - 1),
            tx_hash: B256::repeat_byte(u8::try_from(b).unwrap()),
            block_number: b,
            ..Receipt::default()
        };
        let boundary = BlockBoundary {
            block_number: b,
            end_tx_idx: BPosition::from_index(b),
            l2_timestamp: 100 + b,
            l1_origin: 7,
            ..BlockBoundary::default()
        };
        let delta = BlockDelta {
            block_number: b,
            receipts: vec![receipt],
            ..BlockDelta::default()
        };
        writer
            .delta_tx
            .send(WriteBatch::new(boundary, delta))
            .unwrap();
    });
    std::iter::from_fn(|| writer.snapshot_rx.recv())
        .find(|s| s.block_number() >= 5)
        .expect("block 5 commits");
    writer.shutdown().unwrap();

    let snapshot = StateSnapshot::open(&env).unwrap();
    let window = NonZeroU64::new(3).unwrap();
    let got = LocalHistory::read(&snapshot, (5, 6), window)
        .unwrap()
        .expect("a history");
    let committed = |floor: (u64, u64)| {
        LocalHistory::of(
            floor,
            (5, 6),
            (floor.1..=5).map(|b| BlockBoundaryStart {
                l2_timestamp: 100 + b,
                l1_origin: 7,
                ..header(b, b)
            }),
            (floor.0..5).map(|i| (i, B256::repeat_byte(u8::try_from(i + 1).unwrap()))),
        )
    };
    assert_eq!(got, committed((2, 3)));

    let whole = LocalHistory::read(&snapshot, (5, 6), NonZeroU64::new(50).unwrap())
        .unwrap()
        .expect("a history");
    assert_eq!(whole, committed((0, 1)), "a long window starts at block 1");
    assert_eq!(
        LocalHistory::read(&snapshot, (0, 1), window).unwrap(),
        None,
        "nothing to compare at genesis"
    );
}

/// The history of blocks `1..=3`, with the receipts in `tail` published
/// above its cursor.
fn with_tail(tail: PublishedTail) -> LocalHistory {
    history((0, 1), 3).with_tail(tail)
}

/// A known tail with one receipt published at index 3 of block 4.
fn published(hash: B256) -> PublishedTail {
    PublishedTail::Known([(3, (4, hash))].into())
}

/// Run a reader of `history` on blocks `1..5` of one entry each, and
/// return the outcome.
fn compared(history: LocalHistory) -> Result<(BPosition, Ordered), ExecutorError> {
    let (mut sub, feed) = reader(history);
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..5);
    outcome(&mut sub)
}

#[test]
fn an_equal_published_tail_delivers_from_the_cursor() {
    let (mut sub, feed) = reader(with_tail(published(entry(3).canonical_id)));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..5);
    let delivery = request(&mut sub, &feed);
    assert_eq!(
        (delivery.from_index, delivery.from_block),
        (3, 4),
        "the comparison read the published block, and delivery starts at the cursor"
    );
}

#[test]
fn another_entry_at_a_published_index_voids_the_published_receipts() {
    match compared(with_tail(published(B256::repeat_byte(9)))) {
        Err(ExecutorError::PublishedDiverged {
            block,
            index,
            stream,
        }) => assert_eq!((block, index, stream), (4, 3, entry(3).canonical_id)),
        other => panic!("expected void published receipts, got {other:?}"),
    }
}

#[test]
fn a_block_sealed_before_a_published_receipt_voids_it() {
    let (mut sub, feed) = reader(with_tail(published(entry(3).canonical_id)));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..4);
    feed.send(boundary(4, 3));
    feed.send(done(first.request_id, 3, 5));
    match outcome(&mut sub) {
        Err(ExecutorError::PublishedDiverged { block, index, .. }) => {
            assert_eq!((block, index), (4, 3));
        }
        other => panic!("expected void published receipts, got {other:?}"),
    }
}

#[test]
fn an_unknown_tail_voids_from_the_first_frame_above_the_cursor() {
    match compared(with_tail(PublishedTail::Unknown)) {
        Err(ExecutorError::PublishedDiverged {
            block,
            index,
            stream,
        }) => assert_eq!((block, index, stream), (4, 3, entry(3).canonical_id)),
        other => panic!("expected void published receipts, got {other:?}"),
    }
}

/// Block 3 has two entries, and the second one differs. A reader that
/// committed block 3 and a reader that only published its receipts name
/// the divergence with one fingerprint: the first entry of block 3.
#[test]
fn one_divergence_has_one_fingerprint() {
    let lost = B256::repeat_byte(9);
    let committed = LocalHistory::of(
        (0, 1),
        (4, 4),
        [header(1, 1), header(2, 2), header(3, 4)],
        [
            (0, entry(0).canonical_id),
            (1, entry(1).canonical_id),
            (2, entry(2).canonical_id),
            (3, lost),
        ],
    );
    let in_flight = LocalHistory::of(
        (0, 1),
        (2, 3),
        [header(1, 1), header(2, 2)],
        [(0, entry(0).canonical_id), (1, entry(1).canonical_id)],
    )
    .with_tail(PublishedTail::Known(
        [(2, (3, entry(2).canonical_id)), (3, (3, lost))].into(),
    ));
    let run = |history: LocalHistory| {
        let (mut sub, feed) = reader(history);
        let first = request(&mut sub, &feed);
        let frames = [
            tx_at(0),
            boundary(1, 1),
            tx_at(1),
            boundary(2, 2),
            tx_at(2),
            tx_at(3),
            boundary(3, 4),
            done(first.request_id, 4, 4),
        ];
        for frame in frames {
            feed.send(frame);
        }
        outcome(&mut sub)
    };
    match (run(committed), run(in_flight)) {
        (
            Err(ExecutorError::StreamDiverged { block, stream, .. }),
            Err(ExecutorError::PublishedDiverged {
                block: other_block,
                stream: other,
                ..
            }),
        ) => {
            assert_eq!((block, other_block), (3, 3));
            assert_eq!(stream, entry(2).canonical_id);
            assert_eq!(other, stream, "one divergence, one fingerprint");
        }
        other => panic!("expected the divergence on both readers, got {other:?}"),
    }
}

/// A reader of `tail` on blocks `1..4` of one entry each, then block 4
/// with an epoch of one deposit at index 3. The epoch marker takes index 3
/// and the deposit index 4.
fn epoch_block(
    tail: PublishedTail,
) -> (CarriedSubscription<MemoryTransport, NoSource>, MemoryRemote) {
    let (mut sub, feed) = reader(with_tail(tail));
    let first = request(&mut sub, &feed);
    serve(&feed, first.request_id, 1..4);
    feed.send(epoch_at(3, 1));
    feed.send(boundary(4, 5));
    feed.send(done(first.request_id, 5, 5));
    (sub, feed)
}

/// A published entry at the index of an epoch marker is void: the marker
/// has no receipt.
#[test]
fn a_marker_at_a_published_index_voids_it() {
    let (mut sub, _feed) = epoch_block(published(entry(3).canonical_id));
    match outcome(&mut sub) {
        Err(ExecutorError::PublishedDiverged { block, index, .. }) => {
            assert_eq!((block, index), (4, 3));
        }
        other => panic!("expected void published receipts, got {other:?}"),
    }
}

/// A published deposit receipt at the slot of an equal deposit is equal:
/// its hash is the source hash of the deposit.
#[test]
fn an_equal_epoch_with_a_published_deposit_is_equal() {
    let (mut sub, feed) = epoch_block(PublishedTail::Known([(4, (4, B256::ZERO))].into()));
    let delivery = request(&mut sub, &feed);
    assert_eq!(
        (delivery.from_index, delivery.from_block),
        (3, 4),
        "the comparison ends equal, and delivery starts at the cursor"
    );
}

#[path = "tests_compare_tee.rs"]
mod tee;
