//! The spool continuation at start, and the recovery of a gap the sealer
//! no longer retains, from the references and the archives.

use kardamom_types::BPosition;

use super::*;
use crate::batch::ClosedBlock;
use std::collections::HashMap;

use kardamom_types::TxEnvelope;

use crate::live::rebuild::tests::{archive_of, live_block, refs_of};
use crate::live::rebuild::{ArchiveLoc, rebuild};
use crate::live::refs_store::BlockRefs;
use crate::live::refs_store::tests::FakeStore;

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
    let provider =
        ProviderBuilder::new().connect_mocked_client(alloy_transport::mock::Asserter::new());
    let dir = tempfile::tempdir().unwrap();
    let sender = LiveSender::new(
        provider,
        Address::ZERO,
        crate::da::DaProxy::new("http://127.0.0.1:1").unwrap(),
        0,
        0,
        dir.path().join("cursor.json"),
    );
    let cfg = FeedConfig {
        blocks_per_batch: NonZeroUsize::new(100).unwrap(),
        compress: false,
        chain_id: 1,
        flush: Duration::from_secs(3600),
        idle_flush: Duration::from_secs(3600),
        target_payload_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        skip_through_block: 0,
        da_lag_due: None,
    };
    FeedLoop::new(sender, cfg, spool, restored, 0)
}

/// A rebuilder over a map of envelopes: the test's archive.
struct MapRebuilder(HashMap<ArchiveLoc, TxEnvelope>);

impl Rebuilder for MapRebuilder {
    fn rebuild(mut self, blocks: Vec<BlockRefs>) -> Result<Vec<ClosedBlock>> {
        rebuild(&blocks, &mut self.0)
    }
}

/// The JSON the query endpoint serves for `block`.
fn block_json(block: &ClosedBlock) -> serde_json::Value {
    let refs = refs_of(block);
    serde_json::json!({
        "block_number": refs.block_number,
        "end_tx_idx": refs.end_tx_idx,
        "l1_origin": refs.l1_origin,
        "l2_timestamp": refs.l2_timestamp,
        "refs": refs.refs.iter().map(|r| serde_json::json!({
            "tx_hash": r.tx_hash,
            "tx_idx": r.tx_idx,
            "shard_id": r.shard_id,
            "session_id": r.session_id,
            "position": r.position,
        })).collect::<Vec<_>>(),
    })
}

/// The sealer refuses the replay from block 13 and holds block 15 and
/// after: the gap 13..=15 is rebuilt from the references and the
/// archive into the spool and the group, and the reader resumes at the
/// end of block 15, a boundary the sealer holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_replay_rebuilds_the_gap_and_resumes_at_the_floor() {
    let dir = tempfile::tempdir().unwrap();
    let spool = Spool::open(dir.path()).unwrap();
    for n in 11..=12 {
        spool
            .append(&block(n, 100 + i32::try_from(n).unwrap()))
            .unwrap();
    }
    let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
    assert_eq!(resume.next_block, 13);
    // Blocks 13..=15: two transactions, none, three, ending at 114, 114
    // and 117.
    let gap = vec![
        live_block(13, 114, 2),
        live_block(14, 114, 0),
        live_block(15, 117, 3),
    ];
    let rows = gap
        .iter()
        .map(|b| (b.block_number, block_json(b)))
        .collect();
    let store = FakeStore::serve(rows).await;
    let mut feed = feed_over(spool.clone(), restored);

    let resumed = recover_from_refs(
        &RefsStore::new(vec![store.url()]),
        MapRebuilder(archive_of(&gap)),
        &mut feed,
        cursor(112, 13),
        15,
    )
    .await
    .unwrap();
    assert_eq!(resumed.next_block, 16);
    assert_eq!(resumed.next_index, 117);
    assert_eq!(feed.skip_through_block(), 15);
    assert_eq!(feed.pending_blocks(), 5);
    let spooled = spool.load().unwrap().blocks;
    assert_eq!(
        spooled.iter().map(|b| b.block_number).collect::<Vec<_>>(),
        vec![11, 12, 13, 14, 15]
    );
    assert_eq!(spooled[2..], gap[..]);
}

/// Without a query endpoint, a refused replay stays the fail-stop it is
/// today, and the error says which setting is missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_replay_without_a_source_is_a_fail_stop_that_names_the_setting() {
    let dir = tempfile::tempdir().unwrap();
    let spool = Spool::open(dir.path()).unwrap();
    let (restored, resume) = continue_from_spool(&spool, cursor(90, 11), 10).unwrap();
    let mut feed = feed_over(spool, restored);
    let err = recover_from_refs(
        &RefsStore::new(Vec::new()),
        MapRebuilder(HashMap::new()),
        &mut feed,
        resume,
        15,
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("--block-refs-source"), "{err:#}");
}

/// L1 silence clears by itself; a refused resume waits for an operator.
#[test]
fn a_start_failure_names_its_halt() {
    let l1 = StartError::L1(anyhow::anyhow!("connect L1 RPC http://l1: refused"));
    let halt = l1.halt();
    assert_eq!(halt.cause, HaltCause::L1Unreachable);
    assert!(halt.detail.contains("connect L1 RPC"), "{}", halt.detail);
    assert_eq!(halt.clears, Clears::Auto);

    let resume = StartError::Resume(anyhow::anyhow!("spool does not continue the cursor"));
    let halt = resume.halt();
    assert_eq!(halt.cause, HaltCause::ReplayUnavailable);
    assert_eq!(halt.clears, Clears::Operator);
}
