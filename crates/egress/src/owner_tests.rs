use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, B256};
use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::{BPosition, DropReason, Receipt, ReceiptBatch, TxDrop};
use tokio::sync::oneshot;

use super::{FramePublisher, Input, Join, JoinState, Owner, OwnerConfig, Parts, ReadRequest};
use crate::drops::{Dropped, Drops, Status};
use crate::merge::{Head, Key, Merge, MergeConfig};
use crate::receipts::Receipts;
use crate::ref_mark::RefMark;
use crate::store::{Range, Store};
use crate::test_support::record;

/// The frames that went out live, on a channel the test reads.
#[derive(Clone)]
struct Live(crossbeam_channel::Sender<Vec<u8>>);

impl FramePublisher for Live {
    fn publish(&self, frame: &[u8]) {
        self.0.send(frame.to_vec()).unwrap();
    }
}

fn live() -> (Live, crossbeam_channel::Receiver<Vec<u8>>) {
    let (tx, rx) = crossbeam_channel::unbounded();
    (Live(tx), rx)
}

const GENESIS: Head = Head {
    next_index: 0,
    next_block: 1,
    last: None,
};

fn owner(dir: &std::path::Path, live: Live) -> Owner<Live> {
    owner_with(dir, live, Join::FromPeer)
}

fn owner_with(dir: &std::path::Path, live: Live, join: Join) -> Owner<Live> {
    let store = Store::open(dir, NonZeroU64::new(10).unwrap(), GENESIS).unwrap();
    let merge = Merge::new(
        MergeConfig {
            max_waiting: NonZeroUsize::new(64).unwrap(),
            compared: NonZeroUsize::new(64).unwrap(),
        },
        store.head(),
    );
    Owner::new(
        OwnerConfig {
            sync_every: Duration::from_millis(10),
            keep_blocks: NonZeroU64::new(1_000).unwrap(),
            join,
        },
        Parts {
            merge,
            store,
            receipts: Receipts::new(NonZeroUsize::new(16).unwrap()),
            drops: Drops::new(NonZeroUsize::new(16).unwrap()),
            live: live.clone(),
            receipts_live: live,
        },
    )
}

fn message(seq: u64, frame: &[u8]) -> Input {
    Input::Frame {
        session: 1,
        message: [seq.to_le_bytes().as_slice(), frame].concat(),
    }
}

#[test]
fn delivered_frames_go_out_live_and_a_read_finds_them_stored() {
    let dir = tempfile::tempdir().unwrap();
    let (live, published) = live();
    let mut o = owner(dir.path(), live);
    let frames = [
        encode_egress_record(0, &[1; 40]).unwrap(),
        encode_egress_boundary(1, 1, 0, 0),
    ];
    frames
        .iter()
        .zip(0u64..)
        .for_each(|(frame, seq)| o.on(message(seq, frame)).unwrap());
    assert_eq!(published.try_iter().collect::<Vec<_>>(), frames);

    let (reply, answer) = oneshot::channel();
    o.on(Input::Read(ReadRequest {
        from_index: 0,
        from_block: 1,
        limit: usize::MAX,
        reply,
    }))
    .unwrap();
    let Ok(Ok(Range::Frames {
        frames: read, head, ..
    })) = answer.blocking_recv()
    else {
        panic!("the store answers the read");
    };
    assert_eq!(read, frames);
    assert_eq!(head.next_block, 2);
}

#[test]
fn the_run_ends_when_every_sender_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let o = owner(dir.path(), live);
    let (inputs, rx) = crossbeam_channel::unbounded();
    inputs
        .send(message(0, &encode_egress_record(0, &[1; 40]).unwrap()))
        .unwrap();
    drop(inputs);
    o.run(&rx).unwrap();
    let again = Store::open(dir.path(), NonZeroU64::new(10).unwrap(), GENESIS).unwrap();
    assert_eq!(again.head().next_index, 1, "the frame is on disk");
}

/// The drop of the hash `hash`, as a sequencer reports it.
fn drop_input(hash: u8, reason: DropReason) -> Input {
    Input::Drop(TxDrop {
        tx_hash: B256::with_last_byte(hash),
        sender: Address::repeat_byte(0xA1),
        nonce: 3,
        reason,
    })
}

/// The receipt batch with one receipt of the hash `hash`.
fn receipt_input(hash: u8) -> Input {
    Input::Receipts(ReceiptFrame::Batch(ReceiptBatch {
        receipts: vec![Receipt {
            tx_hash: B256::with_last_byte(hash),
            block_number: 1,
            tx_idx: BPosition::from_index(u64::from(hash)),
            ..Receipt::default()
        }],
        accounts: Vec::new(),
    }))
}

fn status_of(o: &mut Owner<Live>, hash: u8) -> Status {
    let (reply, answer) = oneshot::channel();
    o.on(Input::Status {
        tx_hash: B256::with_last_byte(hash),
        reply,
    })
    .unwrap();
    answer.blocking_recv().unwrap()
}

fn dropped(reason: DropReason) -> Status {
    Status::Dropped(Dropped {
        sender: Address::repeat_byte(0xA1),
        nonce: 3,
        reason,
    })
}

#[test]
fn a_drop_that_both_twins_report_is_one_status() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    o.on(drop_input(1, DropReason::NonceTooLow)).unwrap();
    o.on(drop_input(1, DropReason::ReplacementUnderpriced))
        .unwrap();
    assert_eq!(status_of(&mut o, 1), dropped(DropReason::NonceTooLow));
    assert_eq!(status_of(&mut o, 2), Status::Unknown);
}

#[test]
fn a_receipt_after_a_drop_wins() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    o.on(drop_input(1, DropReason::TxpoolFull)).unwrap();
    o.on(receipt_input(1)).unwrap();
    assert_eq!(status_of(&mut o, 1), Status::Executed);
    assert_eq!(o.parts.drops.dropped(&B256::with_last_byte(1)), None);
}

#[test]
fn a_drop_after_the_receipt_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    o.on(receipt_input(1)).unwrap();
    o.on(drop_input(1, DropReason::NonceTooLow)).unwrap();
    assert_eq!(status_of(&mut o, 1), Status::Executed);
    assert_eq!(o.parts.drops.dropped(&B256::with_last_byte(1)), None);
}

/// The distance counts the records that a copy sent above the stored head.
/// A copy that misses a frame stalls the head, and the distance grows.
#[test]
fn the_distance_counts_the_records_above_the_stored_head() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    assert_eq!(o.distance(), 0, "no copy sent a record yet");
    o.on(message(0, &encode_egress_record(0, &[1; 40]).unwrap()))
        .unwrap();
    assert_eq!(o.distance(), 0, "the store holds every record a copy sent");
    [2u64, 3]
        .iter()
        .try_for_each(|&index| {
            o.on(message(
                index,
                &encode_egress_record(index, &[1; 40]).unwrap(),
            ))
        })
        .unwrap();
    assert_eq!(
        o.distance(),
        3,
        "the copy missed record 1: records 1 to 3 are above the stored head"
    );
}

/// The message of `frame` from a copy that started before this egress.
fn late(seq: u64, frame: &[u8]) -> Input {
    message(100 + seq, frame)
}

fn state(o: &mut Owner<Live>) -> JoinState {
    let (reply, answer) = oneshot::channel();
    o.on(Input::State(reply)).unwrap();
    answer.blocking_recv().unwrap()
}

/// What a read from genesis finds: the frames, or the floor.
fn read_from_genesis(o: &mut Owner<Live>) -> Range {
    let (reply, answer) = oneshot::channel();
    o.on(Input::Read(ReadRequest {
        from_index: 0,
        from_block: 1,
        limit: usize::MAX,
        reply,
    }))
    .unwrap();
    answer.blocking_recv().unwrap().unwrap()
}

/// With no peer, an empty store on a running chain starts after the first
/// boundary of a copy. The frames after it chain, and a reader from
/// genesis gets the new floor.
#[test]
fn with_no_peer_the_stream_starts_after_a_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (live, published) = live();
    let mut o = owner_with(dir.path(), live, Join::AtBoundary);
    o.on(late(0, &record(200))).unwrap();
    o.on(late(1, &encode_egress_boundary(30, 201, 0, 0)))
        .unwrap();
    o.on(late(2, &record(201))).unwrap();
    assert_eq!(published.try_iter().collect::<Vec<_>>(), [record(201)]);
    assert!(state(&mut o).joined);
    let start = Head {
        next_index: 201,
        next_block: 31,
        last: Some(Key::Boundary(30)),
    };
    assert_eq!(
        read_from_genesis(&mut o),
        Range::Unavailable {
            floor: start,
            mark: RefMark::default()
        }
    );
}

/// The frames of a peer fill the place, and the frames of the copy that
/// wait behind them follow.
#[test]
fn the_frames_of_a_peer_join_the_copies() {
    let dir = tempfile::tempdir().unwrap();
    let (live, published) = live();
    let mut o = owner(dir.path(), live);
    let stream = [
        record(0),
        record(1),
        encode_egress_boundary(1, 2, 0, 0),
        record(2),
        record(3),
    ];
    o.on(late(0, &stream[3])).unwrap();
    o.on(late(1, &stream[4])).unwrap();
    assert!(!state(&mut o).joined, "a record is never certain alone");
    o.on(Input::Caught(stream[..4].to_vec())).unwrap();
    assert_eq!(published.try_iter().collect::<Vec<_>>(), stream);
    assert!(state(&mut o).joined);
}

/// A peer floor above the head starts the stream there. A floor at or
/// below the head changes nothing.
#[test]
fn a_peer_floor_above_the_head_starts_the_stream_there() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    let floor = Head {
        next_index: 40,
        next_block: 21,
        last: Some(Key::Boundary(20)),
    };
    o.on(Input::Floor {
        floor,
        mark: RefMark::default(),
    })
    .unwrap();
    assert_eq!(state(&mut o).head, floor);
    o.on(Input::Floor {
        floor: GENESIS,
        mark: RefMark::default(),
    })
    .unwrap();
    assert_eq!(state(&mut o).head, floor);
}

/// A start at the floor of a peer raises the reference mark to the mark
/// of the peer.
#[test]
fn a_peer_floor_raises_the_mark_to_the_mark_of_the_peer() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    let floor = Head {
        next_index: 40,
        next_block: 21,
        last: Some(Key::Boundary(20)),
    };
    let mark = RefMark {
        index: 30,
        block: 16,
    };
    o.on(Input::Floor { floor, mark }).unwrap();
    assert_eq!(
        read_from_genesis(&mut o),
        Range::Unavailable { floor, mark }
    );
}

/// The frames of a peer reach the store through the same append as the
/// copies, so a reference among them raises the mark.
#[test]
fn a_reference_in_the_frames_of_a_peer_raises_the_mark() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    let caught = vec![
        record(0),
        crate::test_support::ref_record(1),
        encode_egress_boundary(1, 2, 0, 0),
    ];
    o.on(Input::Caught(caught)).unwrap();
    assert_eq!(o.parts.store.mark(), RefMark { index: 1, block: 1 });
}

/// When the peers give up, the next boundary of a copy starts the stream.
/// Before, a boundary changes nothing.
#[test]
fn after_the_peers_give_up_a_boundary_starts_the_stream() {
    let dir = tempfile::tempdir().unwrap();
    let (live, _published) = live();
    let mut o = owner(dir.path(), live);
    o.on(late(0, &encode_egress_boundary(30, 201, 0, 0)))
        .unwrap();
    assert_eq!(state(&mut o).head, GENESIS);
    o.on(Input::GiveUp).unwrap();
    o.on(late(1, &encode_egress_boundary(31, 205, 0, 0)))
        .unwrap();
    assert_eq!(state(&mut o).head.next_block, 32);
    assert!(state(&mut o).joined);
}
