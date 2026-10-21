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
