//! Live batcher: tails the canonical ordering from the Aeron Cluster
//! egress, joins `tx_data`, packs batches, and posts them to L1 as a
//! long-lived service.
//!
//! The batcher is a third cluster-egress consumer, next to the executor and
//! the validator. It reuses the `kardamom-engine` reader stack (cluster
//! `tx_ordering` subscription, `tx_data` join buffer, archive refetch) as-is.
//! Only the sink differs: `ReaderToExec` records feed a [`BatchAccumulator`]
//! instead of an execution pipeline. `Deposit` records are skipped, because
//! deposits are absent from DA by design. A reconstructor re-derives them
//! from L1.
//!
//! Durability model:
//! - L1 (`lastBatchIndex` and the `BatchPosted` event) is the authoritative
//!   record of what has been posted.
//! - The cursor file holds the ordering-stream position matching that
//!   truth. It is written only after a confirmed post (at-least-once, like
//!   the da-watcher's L1 cursor). A stale or lost cursor causes
//!   re-observation. The feed loop drops re-observed blocks
//!   (`block_number <= skip_through_block`) without posting. The contract's
//!   compare-and-swap check makes any double post revert loudly instead of
//!   landing.
//!
//! [`BatchAccumulator`]: crate::batch::BatchAccumulator

mod cursor;
mod feed;
pub mod poll;
mod post_age;
mod run;
mod sender;
mod spool;

pub use cursor::{BatchCursor, L1Truth, read_last_batch_index};
pub use run::{LiveArgs, connect_l1, run};
pub use sender::LiveSender;

/// Live-mode metric names, alongside [`crate::batcher::metric_names`]. In
/// live mode, `kardamom_batcher_batches_posted_total` and
/// `_blobs_posted_total` count confirmed L1 posts (receipt observed or
/// reconciled on-chain), not packed batches.
pub use live_metric_names::FEED_RUNNING;

pub(crate) mod live_metric_names {
    /// L1 post attempts that failed and were retried. This includes
    /// transient transport errors and CAS races that reconciled as not
    /// ours.
    pub(crate) const L1_POST_RETRIES: &str = "kardamom_batcher_l1_post_retries_total";
    /// Highest L2 block confirmed on L1 by this batcher.
    pub(crate) const LAST_POSTED_BLOCK: &str = "kardamom_batcher_last_posted_block";
    /// The contract's `lastBatchIndex` after this batcher's latest confirmed
    /// post.
    pub(crate) const LAST_BATCH_INDEX: &str = "kardamom_batcher_last_batch_index";
    /// Closed blocks buffered, waiting for the group to fill or flush.
    pub(crate) const PENDING_BLOCKS: &str = "kardamom_batcher_pending_blocks";
    /// Re-observed blocks dropped because L1 already covers them (stale
    /// cursor replay after a crash between post and cursor write).
    pub(crate) const SKIPPED_POSTED_BLOCKS: &str = "kardamom_batcher_skipped_posted_blocks_total";
    /// Seconds since the block of the last `BatchPosted` log on L1, as
    /// L1 serves it. The value comes from L1 on every probe tick, never
    /// from this process's memory: an endpoint that swallows the logs
    /// makes it grow, which is the alert.
    pub(crate) const LAST_POST_AGE: &str = "kardamom_batcher_last_post_age_seconds";
    /// The idle flush wait, in seconds. The alert rule compares the post
    /// age with twice this value.
    pub(crate) const IDLE_FLUSH_SECONDS: &str = "kardamom_batcher_idle_flush_seconds";
    /// Starts whose L1 read failed. The start retries in-process, so the
    /// counter is scrapeable while L1 stays unreachable.
    pub(crate) const RESUME_FAILURES: &str = "kardamom_batcher_resume_failures_total";
    /// 1 once the feed loop runs over the spool it restored. The
    /// readiness rule of the live batcher requires it.
    pub const FEED_RUNNING: &str = "kardamom_batcher_feed_running";
}
