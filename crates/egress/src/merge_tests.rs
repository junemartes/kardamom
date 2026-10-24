use std::num::NonZeroUsize;

use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};

use super::{Head, Key, Merge, MergeConfig};

fn record(index: u64) -> Vec<u8> {
    encode_egress_record(index, &[u8::try_from(index).unwrap(); 40]).unwrap()
}

fn boundary(block: u64, end: u64) -> Vec<u8> {
    encode_egress_boundary(block, end, 0, 0)
}

/// A copy: the frames of one executor with their sequence numbers.
struct Copy {
    session: i32,
    seq: u64,
}

impl Copy {
    fn new(session: i32) -> Self {
        Self { session, seq: 0 }
    }

    /// The message of `frame` with the next sequence.
    fn send(&mut self, merge: &mut Merge, frame: &[u8]) -> Vec<Vec<u8>> {
        let message = [self.seq.to_le_bytes().as_slice(), frame].concat();
        self.seq += 1;
        let mut out = Vec::new();
        merge.take(self.session, &message, &mut out);
        out
    }

    /// A frame that the executor dropped: it takes a sequence number.
    fn lose(&mut self) {
        self.seq += 1;
    }
}

fn merge() -> Merge {
    Merge::new(
        MergeConfig {
            max_waiting: NonZeroUsize::new(64).unwrap(),
            compared: NonZeroUsize::new(64).unwrap(),
        },
        Head::default(),
    )
}

#[test]
fn one_copy_goes_out_in_its_order() {
    let mut m = merge();
    let mut a = Copy::new(1);
    let stream = [record(0), record(1), boundary(1, 2), record(2)];
    let out: Vec<Vec<u8>> = stream.iter().flat_map(|f| a.send(&mut m, f)).collect();
    assert_eq!(out, stream);
    assert_eq!(
        m.head(),
        Head {
            next_index: 3,
            next_block: 2,
            last: Some(Key::Record(2)),
        }
    );
}

#[test]
fn a_second_copy_adds_nothing() {
    let mut m = merge();
    let (mut a, mut b) = (Copy::new(1), Copy::new(2));
    let stream = [record(0), boundary(1, 1), record(1)];
    let out: Vec<Vec<u8>> = stream
        .iter()
        .flat_map(|f| [a.send(&mut m, f), b.send(&mut m, f)].concat())
        .collect();
    assert_eq!(out, stream);
}

/// Copy A lost the boundary between records 1 and 2. Its record 2 must not
/// go out before the boundary: copy B fills the gap, and the order holds.
#[test]
fn a_gap_in_one_copy_waits_for_the_other_copy() {
    let mut m = merge();
    let (mut a, mut b) = (Copy::new(1), Copy::new(2));
    let mut out = Vec::new();
    out.extend(a.send(&mut m, &record(0)));
    out.extend(a.send(&mut m, &record(1)));
    a.lose();
    out.extend(a.send(&mut m, &record(2)));
    assert_eq!(
        out,
        [record(0), record(1)],
        "record 2 does not jump the gap"
    );
    out.extend(b.send(&mut m, &record(0)));
    out.extend(b.send(&mut m, &record(1)));
    out.extend(b.send(&mut m, &boundary(1, 2)));
    out.extend(b.send(&mut m, &record(2)));
    assert_eq!(out, [record(0), record(1), boundary(1, 2), record(2)]);
}

/// Copy B runs ahead while copy A holds the head: its frames wait, and go
/// out behind A's frame that they follow.
#[test]
fn a_copy_ahead_of_the_head_waits_and_then_chains() {
    let mut m = merge();
    let (mut a, mut b) = (Copy::new(1), Copy::new(2));
    let mut out = a.send(&mut m, &record(0));
    b.lose();
    // B's first message after its gap names no frame before it.
    out.extend(b.send(&mut m, &record(1)));
    out.extend(b.send(&mut m, &record(2)));
    out.extend(b.send(&mut m, &record(3)));
    assert_eq!(out, [record(0)]);
    out.extend(a.send(&mut m, &record(1)));
    assert_eq!(
        out,
        [record(0), record(1), record(2), record(3)],
        "B's 2 waited behind 1, and 3 behind 2"
    );
}

#[test]
fn a_restarted_copy_chains_again_once_it_reaches_the_head() {
    let mut m = merge();
    let mut a = Copy::new(1);
    let mut out = Vec::new();
    for frame in [record(0), record(1), record(2)] {
        out.extend(a.send(&mut m, &frame));
    }
    // The executor restarts from record 1 with a new session.
    let mut again = Copy::new(3);
    for frame in [record(1), record(2), record(3)] {
        out.extend(again.send(&mut m, &frame));
    }
    assert_eq!(out, [record(0), record(1), record(2), record(3)]);
}

#[test]
fn a_frame_that_is_no_record_and_no_boundary_carries_nothing() {
    let mut m = merge();
    let mut a = Copy::new(1);
    let out = a.send(
        &mut m,
        &kardamom_cluster_adapter::wire::encode_replay_done(1, 1),
    );
    assert!(out.is_empty());
    assert_eq!(m.head(), Head::default());
}

/// The relayed frame of an epoch with `deposits` deposits at `index`.
fn epoch_at(index: u64, deposits: usize) -> Vec<u8> {
    let epoch = kardamom_types::EpochRecord {
        l1_number: 7,
        l1_hash: alloy_primitives::B256::repeat_byte(7),
        deposits: vec![kardamom_types::Deposit::default(); deposits],
    };
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(&epoch).unwrap();
    let payload = [
        epoch.canonical_id().as_slice(),
        &[kardamom_cluster_adapter::wire::RT_EPOCH],
        body.as_slice(),
    ]
    .concat();
    encode_egress_record(index, &payload).unwrap()
}

/// An epoch takes its marker and one slot for each deposit: the next
/// index after it is that many on, as a reader of the stream counts it.
#[test]
fn an_epoch_moves_the_next_index_by_its_slots() {
    let mut m = merge();
    let mut a = Copy::new(1);
    let out: Vec<Vec<u8>> = [epoch_at(0, 2), record(3)]
        .iter()
        .flat_map(|f| a.send(&mut m, f))
        .collect();
    assert_eq!(out.len(), 2);
    assert_eq!(m.head().next_index, 4, "the epoch at 0 takes 0, 1 and 2");
}

/// A merge with `head`, as after a restart on a stored head.
fn merge_at(head: Head) -> Merge {
    Merge::new(
        MergeConfig {
            max_waiting: NonZeroUsize::new(64).unwrap(),
            compared: NonZeroUsize::new(64).unwrap(),
        },
        head,
    )
}

/// A copy of an executor that started before this merge.
fn copy_from(session: i32, seq: u64) -> Copy {
    Copy { session, seq }
}

/// After a restart no copy names a frame before its first one, so nothing
/// goes out alone. The frames of a peer, in order, fill the place, and the
/// frames of the copies that wait behind them follow.
#[test]
fn a_restart_joins_behind_the_frames_of_a_peer() {
    let stored = Head {
        next_index: 3,
        next_block: 2,
        last: Some(Key::Record(2)),
    };
    let mut m = merge_at(stored);
    let (mut a, mut b) = (copy_from(1, 100), copy_from(2, 200));
    let live = [record(3), record(4), boundary(2, 5), record(5)];
    let out: Vec<Vec<u8>> = live
        .iter()
        .flat_map(|f| [a.send(&mut m, f), b.send(&mut m, f)].concat())
        .collect();
    assert_eq!(out, [] as [Vec<u8>; 0], "a record is never certain alone");
    assert!(!m.joined());

    let mut out = Vec::new();
    m.take_ordered(&record(3), &mut out);
    assert_eq!(out, live, "the copies chain behind the frame of the peer");
    assert!(m.joined());
}

/// An empty store on a running chain does not start in the middle of a
/// block: a frame of an executor that did not start at genesis waits.
#[test]
fn an_empty_store_on_a_running_chain_waits() {
    let genesis = Head {
        next_index: 0,
        next_block: 1,
        last: None,
    };
    let mut m = merge_at(genesis);
    let mut a = copy_from(1, 100);
    let out: Vec<Vec<u8>> = [record(200), record(201), boundary(30, 202), record(202)]
        .iter()
        .flat_map(|f| a.send(&mut m, f))
        .collect();
    assert_eq!(out, [] as [Vec<u8>; 0]);
    assert_eq!(m.head(), genesis);
}

/// The boundary of the next block that closes at the next index has a
/// certain place, so it goes out alone, and its copy chains after it.
#[test]
fn the_boundary_that_closes_at_the_head_goes_out_alone() {
    let stored = Head {
        next_index: 5,
        next_block: 3,
        last: Some(Key::Record(4)),
    };
    let mut m = merge_at(stored);
    let mut a = copy_from(1, 50);
    let mut out = a.send(&mut m, &boundary(3, 5));
    out.extend(a.send(&mut m, &record(5)));
    assert_eq!(out, [boundary(3, 5), record(5)]);
    assert!(m.joined());
    let mut b = copy_from(2, 60);
    assert_eq!(
        b.send(&mut m, &boundary(4, 7)),
        [] as [Vec<u8>; 0],
        "a boundary that does not close at the next index waits"
    );
}

/// With no peer, the stream starts again after a boundary of a copy. The
/// frames of that copy after the boundary chain.
#[test]
fn a_rebase_starts_after_a_boundary() {
    let genesis = Head {
        next_index: 0,
        next_block: 1,
        last: None,
    };
    let mut m = merge_at(genesis);
    let mut a = copy_from(1, 100);
    let mut out = a.send(&mut m, &record(200));
    let join = boundary(30, 201);
    out.extend(a.send(&mut m, &join));
    assert_eq!(out, [] as [Vec<u8>; 0]);
    let start = Head::after_boundary(&join).unwrap();
    assert_eq!(
        start,
        Head {
            next_index: 201,
            next_block: 31,
            last: Some(Key::Boundary(30)),
        }
    );
    m.rebase(start);
    assert_eq!(a.send(&mut m, &record(201)), [record(201)]);
    assert!(m.joined());
    assert_eq!(Head::after_boundary(&record(3)), None);
}
