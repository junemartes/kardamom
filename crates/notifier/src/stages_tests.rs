use std::time::{Duration, Instant};

use alloy_primitives::B256;

use super::{Event, PROBE, Rolled, Stage, StageTable};

fn event(hash: B256, stage: Stage) -> Event {
    Event {
        hash,
        stage,
        block: None,
        executor: None,
    }
}

fn at_block(hash: B256, stage: Stage, block: u64) -> Event {
    Event {
        block: Some(block),
        ..event(hash, stage)
    }
}

/// A hash whose home slot is `home` in a table of `size` slots.
fn homed(home: u64, tag: u8) -> B256 {
    let mut bytes = [tag; 32];
    bytes[..8].copy_from_slice(&home.to_le_bytes());
    B256::from(bytes)
}

#[test]
fn an_event_above_the_state_is_sent_and_a_copy_is_not() {
    let start = Instant::now();
    let mut table = StageTable::new(64, start);
    let hash = B256::repeat_byte(1);
    assert!(table.observe(event(hash, Stage::Accepted), start).send);
    assert!(table.observe(event(hash, Stage::Sent), start).send);
    assert!(
        !table.observe(event(hash, Stage::Sent), start).send,
        "the twin's copy"
    );
    assert!(table.observe(at_block(hash, Stage::Receipt, 7), start).send);
    assert!(
        !table.observe(at_block(hash, Stage::Receipt, 7), start).send,
        "the other egress"
    );
}

#[test]
fn a_late_event_is_not_sent_but_counts_for_the_latencies() {
    let start = Instant::now();
    let mut table = StageTable::new(64, start);
    let hash = B256::repeat_byte(2);
    table.observe(event(hash, Stage::Accepted), start);
    let receipt = table.observe(
        at_block(hash, Stage::Receipt, 3),
        start + Duration::from_millis(5),
    );
    assert!(receipt.send);
    assert_eq!(receipt.latency, Some(Duration::from_millis(5)));
    // `sent` and `executed` come after the receipt: too late to send.
    let sent = table.observe(event(hash, Stage::Sent), start + Duration::from_millis(6));
    assert!(!sent.send);
    assert_eq!(
        sent.latency,
        Some(Duration::from_millis(6)),
        "the stage still counts"
    );
    let executed = Event {
        executor: Some(2),
        ..at_block(hash, Stage::Executed, 3)
    };
    let seen = table.observe(executed, start + Duration::from_millis(9));
    assert!(!seen.send);
    assert_eq!(
        seen.executor_latency,
        Some(Duration::from_millis(9)),
        "a slow executor still shows"
    );
}

#[test]
fn a_skipped_stage_is_not_filled_in() {
    let start = Instant::now();
    let mut table = StageTable::new(64, start);
    let hash = B256::repeat_byte(3);
    let sent: Vec<bool> = [
        Stage::Accepted,
        Stage::Receipt,
        Stage::Sent,
        Stage::Executed,
    ]
    .into_iter()
    .map(|stage| table.observe(at_block(hash, stage, 1), start).send)
    .collect();
    assert_eq!(sent, [true, true, false, false]);
}

#[test]
fn a_rejection_is_terminal() {
    let start = Instant::now();
    let mut table = StageTable::new(64, start);
    let hash = B256::repeat_byte(4);
    table.observe(event(hash, Stage::Accepted), start);
    assert!(table.observe(event(hash, Stage::Rejected), start).send);
    assert!(
        !table.observe(event(hash, Stage::Sent), start).send,
        "the other twin"
    );
}

#[test]
fn the_merge_latency_runs_from_executed_to_receipt() {
    let start = Instant::now();
    let mut table = StageTable::new(64, start);
    let hash = B256::repeat_byte(5);
    table.observe(event(hash, Stage::Accepted), start);
    let executed = Event {
        executor: Some(0),
        ..at_block(hash, Stage::Executed, 2)
    };
    table.observe(executed, start + Duration::from_millis(2));
    let receipt = table.observe(
        at_block(hash, Stage::Receipt, 2),
        start + Duration::from_millis(5),
    );
    assert_eq!(receipt.merge_latency, Some(Duration::from_millis(3)));
}

#[test]
fn a_rollback_resets_only_executed_and_receipt_above_its_block() {
    let start = Instant::now();
    let mut table = StageTable::new(64, start);
    let kept = B256::repeat_byte(6);
    let revoked_hash = B256::repeat_byte(7);
    let no_sent = B256::repeat_byte(8);
    let posted = B256::repeat_byte(9);
    for (hash, block) in [(kept, 10), (revoked_hash, 11), (no_sent, 12), (posted, 13)] {
        table.observe(event(hash, Stage::Accepted), start);
        if hash != no_sent {
            table.observe(event(hash, Stage::Sent), start);
        }
        table.observe(at_block(hash, Stage::Receipt, block), start);
    }
    table.observe(event(posted, Stage::Posted), start);
    let mut revoked = Vec::new();
    let rolled = table.roll_back(10, |hash| revoked.push(hash));
    assert_eq!(
        rolled,
        Rolled {
            reset: 2,
            past_posted: 1
        }
    );
    revoked.sort();
    assert_eq!(revoked, [revoked_hash, no_sent]);
    // The new receipt after the rollback is sent again, and the entry
    // without `sent` went back to `accepted`.
    assert!(
        table
            .observe(at_block(revoked_hash, Stage::Receipt, 11), start)
            .send
    );
    assert!(table.observe(event(no_sent, Stage::Sent), start).send);
    assert!(
        !table
            .observe(at_block(kept, Stage::Receipt, 10), start)
            .send
    );
}

#[test]
fn a_full_probe_overwrites_its_oldest_entry() {
    let start = Instant::now();
    let mut table = StageTable::new(64, start);
    let size = u64::try_from(table.capacity()).unwrap();
    let probe = u8::try_from(PROBE).unwrap();
    // PROBE hashes with one home slot fill its probe, the first oldest.
    let filling: Vec<B256> = (0..probe).map(|tag| homed(size, tag + 1)).collect();
    for (step, hash) in filling.iter().enumerate() {
        let at = start + Duration::from_millis(u64::try_from(step).unwrap());
        assert!(!table.observe(event(*hash, Stage::Accepted), at).evicted);
    }
    let later = start + Duration::from_millis(100);
    let newcomer = table.observe(event(homed(size, 200), Stage::Accepted), later);
    assert!(newcomer.evicted);
    // The oldest is gone, so its next event starts a new entry.
    assert!(
        table
            .observe(event(filling[0], Stage::Accepted), later)
            .send
    );
    assert!(
        !table
            .observe(event(filling[PROBE - 1], Stage::Accepted), later)
            .send
    );
}
