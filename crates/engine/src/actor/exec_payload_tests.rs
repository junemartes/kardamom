//! The block payload records the exec thread hands the state writer: the
//! frames the DA payload of the block carries, in arrival order.

use std::sync::{Arc, Mutex};

use alloy_primitives::address;
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::kar1::{BlockRecords, TxFrame};
use kardamom_types::{BlockBoundary, BlockDelta};

use super::StateWriterQueue;
use super::test_support::{ExecRig, ImmediateCommit, boundary_msg, feed, funded, tx_msg};
use crate::error::ExecutorError;
use crate::state::StaticSnapshotSource;

/// A writer queue that keeps only the records of each submitted block.
struct RecordsQueue(Arc<Mutex<Vec<(u64, BlockRecords)>>>);

impl StateWriterQueue for RecordsQueue {
    fn submit(
        &mut self,
        block: BlockBoundary,
        _delta: BlockDelta,
        records: BlockRecords,
    ) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push((block.block_number, records));
        Ok(())
    }
}

/// Each boundary submits the frames of its own block, and only those: the
/// batcher packs the same frames, so the stored bytes are the posted
/// bytes.
#[test]
fn each_boundary_submits_the_frames_of_its_own_block() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let log = Arc::new(Mutex::new(Vec::new()));
    let rig = ExecRig::new(
        StaticSnapshotSource(funded(&signer, 0)),
        ImmediateCommit,
        RecordsQueue(log.clone()),
    );
    let first = tx_msg(&signer, to, 0, 0, 10);
    let second = tx_msg(&signer, to, 1, 1, 20);
    let third = tx_msg(&signer, to, 2, 2, 30);
    let frame_of = |msg: &crate::reader::ReaderToExec| match msg {
        crate::reader::ReaderToExec::Tx { envelope, .. } => TxFrame::from(envelope),
        other => panic!("a tx fixture, got {other:?}"),
    };
    let want_block_1 = vec![frame_of(&first), frame_of(&second)];
    let want_block_2 = vec![frame_of(&third)];

    let rx = feed(vec![
        first,
        second,
        boundary_msg(1, 2, 1_700_000_000),
        third,
        boundary_msg(2, 3, 1_700_000_002),
        boundary_msg(3, 3, 1_700_000_003),
    ]);
    let (h, _rx_e2c) = rig.spawn(rx);
    h.join().expect("no panic").expect("exec ok");

    let submitted = log.lock().unwrap();
    let blocks: Vec<u64> = submitted.iter().map(|(n, _)| *n).collect();
    assert_eq!(blocks, vec![1, 2, 3]);
    assert_eq!(submitted[0].1.txs, want_block_1);
    assert_eq!(submitted[1].1.txs, want_block_2);
    assert!(
        submitted[2].1.txs.is_empty(),
        "an empty block carries no frame"
    );
    assert!(submitted.iter().all(|(_, r)| r.remote_epochs.is_empty()));
}
