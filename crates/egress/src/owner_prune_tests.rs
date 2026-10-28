//! The prune of the store follows the L1 heads, past the window of the
//! readers that resume.

use std::num::NonZeroU64;
use std::time::Duration;

use kardamom_cluster_adapter::wire::encode_egress_boundary;

use super::{Live, live, message, owner_with, read_from_genesis};
use crate::l1_heads::L1Heads;
use crate::owner::{Input, Join, Owner};
use crate::store::Range;

/// An owner that keeps 10 blocks below its head and syncs at each call,
/// with the receiver of its live frames.
fn pruning(
    dir: &std::path::Path,
    heads: L1Heads,
) -> (Owner<Live>, crossbeam_channel::Receiver<Vec<u8>>) {
    let (live, published) = live();
    let mut o = owner_with(dir, live, Join::FromPeer).with_heads(heads);
    o.cfg.sync_every = Duration::ZERO;
    o.cfg.keep_blocks = NonZeroU64::new(10).unwrap();
    (o, published)
}

/// Deliver the empty blocks `from..=to` from one copy, then sync.
fn blocks(o: &mut Owner<Live>, from: u64, to: u64) {
    (from..=to)
        .try_for_each(|block| o.on(message(block - 1, &encode_egress_boundary(block, 0, 0, 0))))
        .unwrap();
    o.sync_if_due().unwrap();
}

/// Take the heads, then sync.
fn heads(o: &mut Owner<Live>, inputs: impl IntoIterator<Item = Input>) {
    inputs
        .into_iter()
        .try_for_each(|input| o.on(input))
        .unwrap();
    o.sync_if_due().unwrap();
}

/// The first block that the store holds. A segment holds 10 blocks.
fn first_block(o: &mut Owner<Live>) -> u64 {
    match read_from_genesis(o) {
        Range::Frames { .. } => 1,
        Range::Unavailable { floor, .. } => floor.next_block,
    }
}

#[test]
fn a_frozen_posted_head_keeps_every_block_above_it_past_the_window() {
    let dir = tempfile::tempdir().unwrap();
    let (mut o, _published) = pruning(dir.path(), L1Heads::posted_only());
    blocks(&mut o, 1, 100);
    assert_eq!(
        first_block(&mut o),
        1,
        "no posted head: the store keeps every block"
    );
    heads(&mut o, [Input::Posted(25)]);
    assert_eq!(first_block(&mut o), 21, "the segment of block 26 stays");
    blocks(&mut o, 101, 300);
    assert_eq!(
        first_block(&mut o),
        21,
        "the posted head does not move: every block above it stays, 270 blocks past the window"
    );
}

#[test]
fn the_prune_follows_the_lower_of_the_posted_head_and_the_final_head() {
    let dir = tempfile::tempdir().unwrap();
    let (mut o, _published) = pruning(dir.path(), L1Heads::with_oracle());
    heads(&mut o, [Input::Posted(55)]);
    blocks(&mut o, 1, 101);
    assert_eq!(first_block(&mut o), 1, "no output is final yet");
    heads(&mut o, [Input::Final(35)]);
    assert_eq!(first_block(&mut o), 31, "the final head is the lower head");
    heads(&mut o, [Input::Final(75)]);
    assert_eq!(first_block(&mut o), 51, "the posted head is the lower head");
    heads(&mut o, [Input::Posted(95)]);
    assert_eq!(
        first_block(&mut o),
        71,
        "the final head is the lower head again"
    );
    heads(&mut o, [Input::Posted(500), Input::Final(500)]);
    assert_eq!(
        first_block(&mut o),
        91,
        "both heads are above the window: the store keeps the window"
    );
}

#[test]
fn unposted_blocks_remain_replayable_after_pruning_and_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (mut o, _published) = pruning(dir.path(), L1Heads::posted_only());
        heads(&mut o, [Input::Posted(25)]);
        blocks(&mut o, 1, 300);
        assert_eq!(first_block(&mut o), 21);
    }
    let (mut o, _published) = pruning(dir.path(), L1Heads::posted_only());
    o.sync_if_due().unwrap();
    let Range::Frames {
        frames,
        after,
        head,
    } = o.parts.store.read_from(0, 26, usize::MAX).unwrap()
    else {
        panic!("the posted cursor must still be replayable");
    };
    let expected: Vec<_> = (26..=300)
        .map(|block| encode_egress_boundary(block, 0, 0, 0))
        .collect();
    assert_eq!(frames, expected, "every unposted block survives in order");
    assert_eq!(after, head);
    assert_eq!(head.next_block, 301);
}

#[tokio::test(start_paused = true)]
async fn a_small_retention_window_prunes_only_after_sync_and_segment_boundaries() {
    let dir = tempfile::tempdir().unwrap();
    let interval = Duration::from_secs(3_600);
    {
        let (mut owner, _published) = pruning(dir.path(), L1Heads::with_oracle());
        owner.cfg.sync_every = interval;
        heads(&mut owner, [Input::Posted(100), Input::Final(100)]);
        blocks(&mut owner, 1, 29);
        assert_eq!(first_block(&mut owner), 1);

        tokio::time::advance(interval.checked_sub(Duration::from_nanos(1)).unwrap()).await;
        owner.sync_if_due().unwrap();
        assert_eq!(
            first_block(&mut owner),
            1,
            "no prune before the sync deadline"
        );
        tokio::time::advance(Duration::from_nanos(1)).await;
        owner.sync_if_due().unwrap();
        assert_eq!(
            first_block(&mut owner),
            11,
            "block 20 still lies in the retained window"
        );

        blocks(&mut owner, 30, 30);
        assert_eq!(
            first_block(&mut owner),
            11,
            "the next segment waits for the next sync"
        );
        tokio::time::advance(interval).await;
        owner.sync_if_due().unwrap();
        assert_eq!(first_block(&mut owner), 21);
    }
    let (mut restored, _published) = pruning(dir.path(), L1Heads::with_oracle());
    assert_eq!(first_block(&mut restored), 21);
    let Range::Frames { frames, .. } = restored.parts.store.read_from(0, 21, usize::MAX).unwrap()
    else {
        panic!("the retained window must survive restart");
    };
    let expected: Vec<_> = (21..=30)
        .map(|block| encode_egress_boundary(block, 0, 0, 0))
        .collect();
    assert_eq!(frames, expected);
}
