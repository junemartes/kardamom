use std::num::NonZeroUsize;

use super::*;

fn cfg() -> FeedConfig {
    FeedConfig {
        blocks_per_batch: NonZeroUsize::new(3).unwrap(),
        compress: false,
        chain_id: 1,
        flush: Duration::from_secs(60),
        idle_flush: Duration::from_secs(3600),
        target_payload_bytes: NonZeroUsize::new(1000).unwrap(),
        skip_through_block: 0,
        da_lag_due: NonZeroU64::new(50),
    }
}

fn empty_block(block_number: u64) -> ClosedBlock {
    ClosedBlock {
        block_number,
        l2_timestamp: 0,
        end_tx_idx: kardamom_types::BPosition {
            term_id: 0,
            term_offset: 0,
        },
        l1_origin: 0,
        remote_epochs: Vec::new(),
        txs: Vec::new(),
    }
}

fn group(age: Duration, blocks: u64, has_traffic: bool) -> PendingGroup {
    PendingGroup {
        since: Instant::now().checked_sub(age).unwrap(),
        blocks: (1..=blocks).map(empty_block).collect(),
        has_traffic,
        raw_bytes: 0,
        cursor: BatchCursor::genesis(),
    }
}

#[test]
fn a_group_with_traffic_waits_the_short_timer() {
    assert!(!group(Duration::from_secs(59), 1, true).due(&cfg(), 0));
    assert!(group(Duration::from_secs(60), 1, true).due(&cfg(), 0));
}

#[test]
fn an_idle_group_waits_the_long_timer() {
    assert!(!group(Duration::from_secs(60), 1, false).due(&cfg(), 0));
    assert!(!group(Duration::from_secs(3599), 1, false).due(&cfg(), 0));
    assert!(group(Duration::from_secs(3600), 1, false).due(&cfg(), 0));
}

#[test]
fn a_full_group_is_due_at_once() {
    assert!(group(Duration::ZERO, 3, false).due(&cfg(), 0));
    assert!(!group(Duration::ZERO, 2, true).due(&cfg(), 0));
}

#[test]
fn a_group_at_the_byte_target_is_due_at_once() {
    let mut g = group(Duration::ZERO, 1, true);
    g.raw_bytes = 999;
    assert!(!g.due(&cfg(), 0));
    g.raw_bytes = 1000;
    assert!(g.due(&cfg(), 0));
}

/// A quiet group of a young age is due once the sealed head is half
/// the DA-lag budget past the posted head; with the guard off, never.
#[test]
fn a_quiet_group_is_due_at_half_the_da_lag_budget() {
    let g = group(Duration::from_secs(1), 2, false);
    assert!(!g.due(&cfg(), 0), "two blocks past an empty chain");
    let mut far = group(Duration::from_secs(1), 1, false);
    far.blocks = vec![empty_block(149)];
    assert!(!far.due(&cfg(), 100), "49 blocks of lag");
    far.blocks = vec![empty_block(150)];
    assert!(far.due(&cfg(), 100), "50 blocks of lag: half the budget");
    let off = FeedConfig {
        da_lag_due: None,
        ..cfg()
    };
    assert!(!far.due(&off, 0));
}

#[path = "feed_recovery_tests.rs"]
mod recovery;
