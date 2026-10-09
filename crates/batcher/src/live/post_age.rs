//! The age of the last post, as the L1 follower tells it.
//!
//! The L1 follower is the one reader of L1 data. Each record of its
//! `l1_blocks` stream carries the `BatchPosted` events of its block and
//! the block's timestamp. The age is the seconds since the newest block
//! that carried a post. The value never comes from this process's memory
//! of its own posts: a batcher whose posts L1 does not show sees its own
//! age grow, and a batcher that stopped posting sees the same. Both are
//! the alert. At start, the follower's archive gives the time of the last
//! post it holds.
//!
//! The batcher reads its post age from the follower, so it waits on the
//! follower for it: every follower instance halted, or a stream silent
//! for the silence window, pauses the batcher with the follower as its
//! root. The posts themselves do not wait.

use std::ops::ControlFlow;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kardamom_log::aeron_live::L1BlocksSubscriberHandle;
use kardamom_obs::events::BoardView;
use kardamom_obs::follower::FollowerWatch;
use kardamom_types::L1Block;
use metrics::gauge;
use tokio::sync::watch;
use tokio::time::Instant;
use tracing::warn;

use super::live_metric_names;
use crate::indexer::IndexerClient;

/// The timestamp of the newest L1 block that carried a post.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct LastPost(Option<u64>);

impl LastPost {
    /// A record: the newest post moves to its block when it carries one.
    fn seen(&mut self, record: &L1Block) {
        if !record.batches.is_empty() {
            self.at(record.timestamp);
        }
    }

    /// A post at `timestamp`; an older one changes nothing.
    fn at(&mut self, timestamp: u64) {
        self.0 = self.0.max(Some(timestamp));
    }

    /// The age at `now`, in seconds. `None` before the first post.
    fn age(self, now: u64) -> Option<u64> {
        self.0.map(|posted| now.saturating_sub(posted))
    }
}

/// The watch's state: the stream, the start's source, and the newest
/// post seen.
pub(crate) struct PostAge {
    blocks: L1BlocksSubscriberHandle,
    indexer: Option<IndexerClient>,
    follower: FollowerWatch,
    last_post: LastPost,
    every: Duration,
}

impl PostAge {
    pub(crate) fn new(
        blocks: L1BlocksSubscriberHandle,
        board: watch::Receiver<BoardView>,
        indexer: Option<IndexerClient>,
        silence: Duration,
        every: Duration,
    ) -> Self {
        Self {
            blocks,
            indexer,
            follower: FollowerWatch::new(silence).with_board(board),
            last_post: LastPost::default(),
            every,
        }
    }

    /// Watch until the stream closes.
    pub(crate) async fn run(mut self) {
        let mut interval = tokio::time::interval(self.every);
        while self.step(&mut interval).await.is_continue() {}
    }

    /// One record, or one tick: the seed from the archive while none is
    /// known, the export, and the follower check.
    async fn step(&mut self, interval: &mut tokio::time::Interval) -> ControlFlow<()> {
        tokio::select! {
            record = self.blocks.recv() => match record {
                Some((_, record)) => self.on_record(&record),
                None => return ControlFlow::Break(()),
            },
            _ = interval.tick() => self.tick().await,
        }
        ControlFlow::Continue(())
    }

    fn on_record(&mut self, record: &L1Block) {
        self.follower.saw_record();
        self.last_post.seen(record);
    }

    async fn tick(&mut self) {
        if self.last_post.0.is_none() {
            self.seed().await;
        }
        self.publish();
        self.follower.follow(Instant::now(), false);
    }

    /// The time of the last post the follower's archive holds.
    async fn seed(&mut self) {
        let Some(indexer) = &self.indexer else {
            return;
        };
        match indexer.last_post_time().await {
            Ok(Some(seed)) => self.last_post.at(seed),
            Ok(None) => {}
            Err(error) => warn!(%error, "post age: the follower's archive did not answer"),
        }
    }

    fn publish(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let Some(age) = self.last_post.age(now) else {
            return;
        };
        #[allow(
            clippy::cast_precision_loss,
            reason = "metric value; never nears 2^52 for an age in seconds"
        )]
        gauge!(live_metric_names::LAST_POST_AGE).set(age as f64);
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{B256, Bytes};
    use kardamom_types::{BatchEntry, L1Block};

    use super::LastPost;

    fn block(number: u64, posts: usize) -> L1Block {
        let entry = BatchEntry {
            index: number,
            da_cert: Bytes::from_static(&[3]),
            l2_block_start: 1,
            l2_block_end: 2,
            records_commitment: B256::ZERO,
            l1_block: number,
            l1_tx: B256::ZERO,
        };
        L1Block {
            number,
            timestamp: number * 12,
            batches: vec![entry; posts],
            ..L1Block::default()
        }
    }

    /// Only a block that carried a post moves the age; the age grows from
    /// the newest one, and an older seed changes nothing.
    #[test]
    fn the_age_runs_from_the_newest_block_that_carried_a_post() {
        let mut last = LastPost::default();
        assert_eq!(last.age(1_000), None);
        last.seen(&block(10, 1));
        last.seen(&block(11, 0));
        assert_eq!(last.age(150), Some(30));
        last.at(60);
        assert_eq!(last.age(150), Some(30), "an older seed changes nothing");
        last.seen(&block(12, 2));
        assert_eq!(last.age(150), Some(6));
    }
}
