//! The transaction references the exec thread hands the state writer
//! with each block: one per transaction, in arrival order, none for a
//! deposit.

use std::sync::{Arc, Mutex};

use alloy_primitives::address;
use alloy_signer_local::PrivateKeySigner;
use kardamom_types::{BlockBoundary, BlockDelta, TxRef};

use super::StateWriterQueue;
use super::test_support::{ExecRig, ImmediateCommit, boundary_msg, feed, funded, tx_msg};
use crate::error::ExecutorError;
use crate::reader::ReaderToExec;
use crate::state::StaticSnapshotSource;

/// The references of each submitted block, by block number.
type RefsLog = Arc<Mutex<Vec<(u64, Vec<TxRef>)>>>;

/// A writer queue that keeps only the references of each submitted block.
struct RefsQueue(RefsLog);

impl StateWriterQueue for RefsQueue {
    fn submit(
        &mut self,
        block: BlockBoundary,
        _delta: BlockDelta,
        refs: Vec<TxRef>,
    ) -> Result<(), ExecutorError> {
        self.0.lock().unwrap().push((block.block_number, refs));
        Ok(())
    }
}

/// Each boundary submits the references of its own block, and only
/// those, in the order the stream carried them.
#[test]
fn each_boundary_submits_the_references_of_its_own_block() {
    let signer = PrivateKeySigner::random();
    let to = address!("00000000000000000000000000000000000ABCDE");
    let log = Arc::new(Mutex::new(Vec::new()));
    let rig = ExecRig::new(
        StaticSnapshotSource(funded(&signer, 0)),
        ImmediateCommit,
        RefsQueue(log.clone()),
    );
    let first = tx_msg(&signer, to, 0, 0, 10);
    let second = tx_msg(&signer, to, 1, 1, 20);
    let third = tx_msg(&signer, to, 2, 2, 30);
    let ref_of = |msg: &ReaderToExec| match msg {
        ReaderToExec::Tx { tx_ref, .. } => *tx_ref,
        other => panic!("a tx fixture, got {other:?}"),
    };
    let want_block_1 = vec![ref_of(&first), ref_of(&second)];
    let want_block_2 = vec![ref_of(&third)];

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
    assert_eq!(submitted[0].1, want_block_1);
    assert_eq!(submitted[1].1, want_block_2);
    assert!(
        submitted[2].1.is_empty(),
        "an empty block carries no reference"
    );
}
