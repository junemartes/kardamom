//! A batcher frozen past the sealers' floor, with the real sender, spool
//! and startup on Anvil.
//!
//! The frozen process has a group in its spool that L1 does not hold, and
//! the freeze ends it. Its restart must post that group first, right after
//! the block L1 covers. The sealers refused the replay of the gap below
//! their floor, so the restart rebuilds the gap from the block references
//! and the archived bytes, posts it next, and then posts the live blocks
//! from the floor on: every block once, in order, with one post per batch
//! and no duplicate.

use std::ops::RangeInclusive;

use super::*;
use crate::batch::ClosedBlock;
use crate::live::rebuild::rebuild;
use crate::live::rebuild::tests::{archive_of, live_block, refs_of};

/// The transactions of each block in the rebuilt gap.
const GAP_TXS: u8 = 2;

impl Fixture {
    /// The freeze: the post of `block` was signed but never reached L1,
    /// and the process ended. The spool keeps the block and the cursor
    /// stays at the block before it.
    async fn freeze_with_spool(&self, block: u64) {
        self.automine(false).await;
        let pending = self.start(block..=block).await;
        self.await_mempool(block).await;
        pending.cancel().await;
        let _: () = self
            .rig
            .provider
            .raw_request("anvil_dropAllTransactions".into(), ())
            .await
            .unwrap();
        self.automine(true).await;
        let truth = L1Truth::read(&self.rig.provider, self.settlement)
            .await
            .unwrap();
        assert_eq!(truth.last_batch_index, block - 1, "the group is not on L1");
        self.assert_cursor(Self::cursor(block - 1, block - 1));
        self.assert_spooled(&[block]);
    }

    /// The restart: the spool's group posts first; the rebuilt `gap`
    /// follows; the live blocks from the floor follow it.
    async fn restart_past_the_floor(&self, gap: RangeInclusive<u64>, live: RangeInclusive<u64>) {
        let args = self.args();
        let resumed = args
            .start_l1_side()
            .await
            .unwrap_or_else(|_| panic!("start"));
        let spool = Spool::open(&args.spool_dir).unwrap();
        let (restored, resume) =
            continue_from_spool(&spool, resumed.cursor, resumed.skip_through_block).unwrap();
        let mut feed = resumed.feed(&args, spool, restored, resume);
        // The first stack posts the restored group, then its replay is
        // refused below the floor and the stack ends.
        let (_, refused) = tokio::sync::mpsc::channel(1);
        let ended = feed.run(refused).await;
        assert!(
            ended.to_string().contains("reader channel closed"),
            "{ended:#}"
        );
        let restored_end = *gap.start() - 1;
        self.assert_cursor(Self::cursor(restored_end, restored_end));
        let floor = *gap.end() + 1;
        let blocks: Vec<ClosedBlock> = gap.map(|n| live_block(n, n * 10, GAP_TXS)).collect();
        let mut archive = archive_of(&blocks);
        let refs: Vec<_> = blocks.iter().map(refs_of).collect();
        let rebuilt = rebuild(&refs, &mut archive).unwrap();
        assert_eq!(rebuilt, blocks, "the rebuild returns the archived bytes");
        assert_eq!(feed.absorb(rebuilt).unwrap().next_block, floor);
        Self::run_feed(feed, live).finish().await;
    }

    /// L1 holds `batches` posts that cover blocks `1..=through` in order,
    /// each block once, and the batcher sent exactly one transaction per
    /// post.
    async fn assert_contiguous(&self, batches: u64, through: u64) {
        let records = crate::l1::read_posted_batches(&self.rig.provider, self.settlement, 0)
            .await
            .unwrap();
        let ranges: Vec<(u64, u64, u64)> = records
            .iter()
            .map(|r| (r.index, r.l2_block_start, r.l2_block_end))
            .collect();
        assert_eq!(u64::try_from(ranges.len()).unwrap(), batches, "{ranges:?}");
        let covered = ranges.iter().try_fold(0, |end, (index, start, last)| {
            (*start == end + 1 && last >= start && *index >= 1).then_some(*last)
        });
        assert_eq!(covered, Some(through), "{ranges:?}");
        let nonce = self
            .rig
            .provider
            .get_transaction_count(self.batcher)
            .await
            .unwrap();
        assert_eq!(nonce, batches, "one transaction per post");
    }
}

#[tokio::test]
#[ignore = "requires Anvil and the compiled settlement contract"]
async fn a_batcher_frozen_past_the_floor_posts_its_spool_then_the_rebuilt_gap_once() {
    let fixture = Fixture::new().await;
    fixture.start(1..=1).await.finish().await;
    fixture.freeze_with_spool(2).await;
    fixture.restart_past_the_floor(3..=4, 5..=6).await;
    fixture.assert_cursor(Fixture::cursor(6, 5));
    fixture.assert_contiguous(5, 6).await;
    fixture.assert_spooled(&[]);
}
