//! The spool continuation at start, and the recovery of a gap the sealer
//! no longer retains from the payload store.

use kardamom_types::BPosition;

use super::*;
use crate::batch::ClosedBlock;
use crate::live::payload_store::tests::{FakeStore, stored_row};

fn block(number: u64, end: i32) -> ClosedBlock {
    ClosedBlock {
        block_number: number,
        l2_timestamp: 0,
        end_tx_idx: BPosition {
            term_id: 0,
            term_offset: end,
        },
        l1_origin: 0,
        remote_epochs: Vec::new(),
        txs: Vec::new(),
    }
}

fn cursor(next_index: u64, next_block: u64) -> BatchCursor {
    BatchCursor {
        next_index,
        next_block,
        last_batch_index: 3,
    }
}

#[test]
fn a_spool_that_continues_the_cursor_moves_the_resume_point() {
    let dir = tempfile::tempdir().unwrap();
    let spool = Spool::open(dir.path()).unwrap();
    for n in 11..=13 {
        spool
            .append(&block(n, 100 + i32::try_from(n).unwrap()))
            .unwrap();
    }
    let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
    assert_eq!(restored.blocks.len(), 3);
    assert_eq!(resume.next_block, 14);
    assert_eq!(resume.next_index, block(13, 113).end_tx_idx.as_index());
    assert_eq!(resume.last_batch_index, 3);
}

#[test]
fn a_spool_behind_a_confirmed_post_drops_the_covered_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let spool = Spool::open(dir.path()).unwrap();
    for n in 11..=13 {
        spool.append(&block(n, 0)).unwrap();
    }
    // L1 covers through 12 (a post confirmed after the spool write).
    let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 12).unwrap();
    assert_eq!(restored.blocks.len(), 1);
    assert_eq!(restored.blocks[0].block_number, 13);
    assert_eq!(resume.next_block, 14);
}

#[test]
fn a_spool_that_does_not_continue_the_cursor_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let spool = Spool::open(dir.path()).unwrap();
    spool.append(&block(20, 0)).unwrap();
    let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
    assert!(restored.blocks.is_empty());
    assert_eq!(resume, cursor(90, 11));
    assert!(spool.load().unwrap().blocks.is_empty());
}

/// A feed loop over a mocked L1 provider: the recovery touches the spool
/// and the pending group only, never the sender.
fn feed_over(spool: Spool, restored: Restored) -> FeedLoop<impl Provider> {
    let provider = ProviderBuilder::new()
        .connect_mocked_client(alloy_transport::mock::Asserter::new());
    let dir = tempfile::tempdir().unwrap();
    let sender = LiveSender::new(
        provider,
        Address::ZERO,
        crate::da::DaProxy::new("http://127.0.0.1:1").unwrap(),
        0,
        0,
        dir.path().join("cursor.json"),
        0,
    );
    let cfg = FeedConfig {
        blocks_per_batch: NonZeroUsize::new(100).unwrap(),
        compress: false,
        chain_id: 1,
        flush: Duration::from_secs(3600),
        idle_flush: Duration::from_secs(3600),
        target_payload_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        skip_through_block: 0,
    };
    FeedLoop::new(sender, cfg, spool, restored)
}

/// The sealer refuses the replay from block 13 and holds block 15 and
/// after: the store serves 13..=15 into the spool and the group, and
/// the reader resumes at the end of block 15, a boundary the sealer
/// holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_replay_fills_the_gap_from_the_store_and_resumes_at_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let spool = Spool::open(dir.path()).unwrap();
    for n in 11..=12 {
        spool.append(&block(n, 100 + i32::try_from(n).unwrap())).unwrap();
    }
    let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
    assert_eq!(resume.next_block, 13);
    let rows = (13..=15)
        .map(|n| (n, stored_row(&block(n, 100 + i32::try_from(n).unwrap()))))
        .collect();
    let store = FakeStore::serve(rows).await;
    let mut feed = feed_over(spool.clone(), restored);

    let resumed = recover_from_store(&PayloadStore::new(vec![store.url()]), &mut feed, resume, 15)
        .await
        .unwrap();
    assert_eq!(resumed.next_block, 16);
    assert_eq!(resumed.next_index, block(15, 115).end_tx_idx.as_index());
    assert_eq!(feed.skip_through_block(), 15);
    assert_eq!(feed.pending_blocks(), 5);
    let spooled: Vec<u64> = spool
        .load()
        .unwrap()
        .blocks
        .iter()
        .map(|b| b.block_number)
        .collect();
    assert_eq!(spooled, vec![11, 12, 13, 14, 15]);
}

/// Without a payload source, a refused replay stays the fail-stop it is
/// today, and the error says which setting is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_replay_without_a_store_is_a_fail_stop_that_names_the_setting() {
    let dir = tempfile::tempdir().unwrap();
    let spool = Spool::open(dir.path()).unwrap();
    let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
    let mut feed = feed_over(spool, restored);
    let err = recover_from_store(&PayloadStore::new(Vec::new()), &mut feed, resume, 15)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("--payload-source"), "{err:#}");
}
