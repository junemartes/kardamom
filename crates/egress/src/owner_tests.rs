use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use alloy_primitives::{Address, B256};
use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};
use kardamom_log::aeron_live::ReceiptFrame;
use kardamom_types::{BPosition, DropReason, Receipt, ReceiptBatch, TxDrop};
use tokio::sync::oneshot;

use super::{FramePublisher, Input, Owner, OwnerConfig, Parts, ReadRequest};
use crate::drops::{Dropped, Drops, Status};
use crate::merge::{Head, Merge, MergeConfig};
use crate::receipts::Receipts;
use crate::store::{Range, Store};

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
