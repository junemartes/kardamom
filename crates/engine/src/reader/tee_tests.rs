use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};
use kardamom_types::{BPosition, BlockBoundaryStart, TxOrderingMessage, VoidRecord};

use super::{Forward, FrameTee};
use crate::reader::Ordered;

/// A record frame at `index`. The payload is opaque to the tee.
fn record(index: u64) -> Vec<u8> {
    encode_egress_record(index, &[u8::try_from(index).unwrap(); 40]).unwrap()
}

fn boundary_item(block: u64) -> Ordered {
    Ordered::Message(TxOrderingMessage::BoundaryStart(BlockBoundaryStart {
        block_number: block,
        ..BlockBoundaryStart::default()
    }))
}

/// Any record item: the tee keys a record by its position.
fn record_item() -> Ordered {
    Ordered::Message(TxOrderingMessage::Void(VoidRecord::default()))
}

fn tee() -> (Forward, crossbeam_channel::Receiver<Vec<u8>>) {
    let (out, rx) = crossbeam_channel::bounded(16);
    (Forward::on(FrameTee::new(out)), rx)
}

#[test]
fn frames_go_out_in_the_order_of_delivery_once() {
    let (mut tee, rx) = tee();
    // The live record 2 comes first, then the replay of 0 and 1 with a copy
    // of 1, then the boundary of block 1.
    for frame in [record(2), record(0), record(1), record(1)] {
        tee.hold(&frame);
    }
    tee.hold(&encode_egress_boundary(1, 2, 0, 0));
    for (index, item) in [(0, record_item()), (1, record_item())] {
        tee.release((BPosition::from_index(index), item));
    }
    tee.release((BPosition::from_index(2), boundary_item(1)));
    tee.release((BPosition::from_index(2), record_item()));
    let got: Vec<Vec<u8>> = rx.try_iter().collect();
    let want: Vec<Vec<u8>> = [
        record(0),
        record(1),
        encode_egress_boundary(1, 2, 0, 0),
        record(2),
    ]
    .into_iter()
    .zip(0u64..)
    .map(|(frame, seq)| [seq.to_le_bytes().as_slice(), &frame].concat())
    .collect();
    assert_eq!(got, want, "each frame once, behind its sequence");
}

#[test]
fn a_dropped_frame_still_takes_its_sequence() {
    let (out, rx) = crossbeam_channel::bounded(1);
    let mut tee = Forward::on(FrameTee::new(out));
    tee.hold(&record(0));
    tee.hold(&record(1));
    tee.release((BPosition::from_index(0), record_item()));
    tee.release((BPosition::from_index(1), record_item()));
    let first = rx.try_recv().unwrap();
    assert_eq!(first[..8], 0u64.to_le_bytes(), "the queue takes one");
    tee.hold(&record(2));
    tee.release((BPosition::from_index(2), record_item()));
    assert_eq!(
        rx.try_recv().unwrap()[..8],
        2u64.to_le_bytes(),
        "frame 1 dropped, so the next one carries 2: the egress sees the gap"
    );
}

#[test]
fn a_tee_that_is_off_forwards_nothing_and_passes_the_item_on() {
    let mut off = Forward::default();
    off.hold(&record(0));
    let (position, _) = off.release((BPosition::from_index(0), record_item()));
    assert_eq!(position, BPosition::from_index(0));
}

#[test]
fn a_frame_that_is_no_record_and_no_boundary_is_not_held() {
    let (mut tee, rx) = tee();
    tee.hold(&kardamom_cluster_adapter::wire::encode_replay_done(5, 2));
    tee.release((BPosition::from_index(5), record_item()));
    assert!(rx.try_recv().is_err());
}
