use alloy_primitives::B256;

use super::{Admit, DEDUP_HORIZON, L1Block, L1BlockDedup};

/// Admit a record, and take it when it is the next one.
fn admit(dedup: &mut L1BlockDedup, record: &L1Block) -> Admit {
    let admit = dedup.admit(record);
    if admit == Admit::Next {
        dedup.take(record);
    }
    admit
}
use crate::epoch::EpochRecord;

fn hash(number: u64, fork: u8) -> B256 {
    let mut bytes = [fork; 32];
    bytes[..8].copy_from_slice(&number.to_be_bytes());
    B256::from(bytes)
}

/// Block `number` of chain `fork`: its parent is block `number - 1` of
/// the same chain.
fn block(number: u64, fork: u8) -> L1Block {
    L1Block {
        number,
        hash: hash(number, fork),
        parent_hash: hash(number - 1, fork),
        timestamp: number * 12,
        epoch: EpochRecord {
            l1_number: number,
            l1_hash: hash(number, fork),
            deposits: Vec::new(),
        },
        batches: Vec::new(),
    }
}

/// Two instances publish the same blocks: the consumer takes each block
/// once, in order, whichever instance is first.
#[test]
fn the_second_instance_s_copies_are_dropped() {
    let mut dedup = L1BlockDedup::after(10, hash(10, 0));
    let a: Vec<Admit> = (11..=13).map(|n| admit(&mut dedup, &block(n, 0))).collect();
    assert_eq!(a, [Admit::Next; 3]);
    let b: Vec<Admit> = (11..=14).map(|n| admit(&mut dedup, &block(n, 0))).collect();
    assert_eq!(
        b,
        [
            Admit::Duplicate,
            Admit::Duplicate,
            Admit::Duplicate,
            Admit::Next
        ]
    );
    assert_eq!(dedup.head(), 14);
    assert_eq!(dedup.head_hash(), hash(14, 0));
}

/// The start block and blocks below it are dropped, not checked.
#[test]
fn blocks_at_or_below_the_start_are_dropped() {
    let mut dedup = L1BlockDedup::after(10, hash(10, 0));
    assert_eq!(admit(&mut dedup, &block(10, 0)), Admit::Duplicate);
    assert_eq!(admit(&mut dedup, &block(3, 7)), Admit::Duplicate);
}

/// Two records of one number with different hashes are the disagreement
/// halt, whichever came first.
#[test]
fn two_hashes_for_one_number_are_a_disagreement() {
    let mut dedup = L1BlockDedup::after(10, hash(10, 0));
    assert_eq!(admit(&mut dedup, &block(11, 0)), Admit::Next);
    assert_eq!(
        admit(&mut dedup, &block(11, 9)),
        Admit::Disagreement {
            number: 11,
            first: hash(11, 0),
            second: hash(11, 9),
        }
    );
    assert_eq!(dedup.head(), 11);
}

/// A record that the consumer does not take stays the next one.
#[test]
fn a_record_not_taken_stays_the_next() {
    let dedup = L1BlockDedup::after(10, hash(10, 0));
    assert_eq!(dedup.admit(&block(11, 0)), Admit::Next);
    assert_eq!(dedup.admit(&block(11, 0)), Admit::Next);
    assert_eq!(dedup.head(), 10);
}

/// A gap on the subscription is reported with the block the consumer
/// needs next; the head does not move.
#[test]
fn a_record_past_the_next_block_is_ahead() {
    let mut dedup = L1BlockDedup::after(10, hash(10, 0));
    assert_eq!(
        admit(&mut dedup, &block(13, 0)),
        Admit::Ahead { expected: 11 }
    );
    assert_eq!(dedup.head(), 10);
}

/// The next block must descend from the head.
#[test]
fn the_next_block_must_name_the_head_as_its_parent() {
    let mut dedup = L1BlockDedup::after(10, hash(10, 0));
    assert_eq!(
        admit(&mut dedup, &block(11, 5)),
        Admit::ParentMismatch {
            number: 11,
            parent: hash(10, 5),
            expected: hash(10, 0),
        }
    );
    assert_eq!(dedup.head(), 10);
}

/// The dedup keeps the first hash of the horizon below the head only.
#[test]
fn the_horizon_bounds_what_the_dedup_keeps() {
    let mut dedup = L1BlockDedup::after(0, hash(0, 0));
    (1..=DEDUP_HORIZON + 10).for_each(|n| assert_eq!(admit(&mut dedup, &block(n, 0)), Admit::Next));
    assert_eq!(
        dedup.seen.len(),
        usize::try_from(DEDUP_HORIZON).unwrap() + 1
    );
    assert_eq!(admit(&mut dedup, &block(5, 9)), Admit::Duplicate);
    assert_eq!(
        admit(&mut dedup, &block(DEDUP_HORIZON + 5, 9)),
        Admit::Disagreement {
            number: DEDUP_HORIZON + 5,
            first: hash(DEDUP_HORIZON + 5, 0),
            second: hash(DEDUP_HORIZON + 5, 9),
        }
    );
}

#[test]
fn matching_headers_do_not_hide_changed_payloads() {
    let mut dedup = L1BlockDedup::after(10, hash(10, 0));
    let record = block(11, 0);
    assert_eq!(admit(&mut dedup, &record), Admit::Next);
    let mut changed = record.clone();
    changed.epoch.l1_number = 12;
    assert_eq!(
        dedup.admit(&changed),
        Admit::ContentDisagreement { number: 11 }
    );
    changed = record.clone();
    changed.timestamp += 1;
    assert_eq!(
        dedup.admit(&changed),
        Admit::ContentDisagreement { number: 11 }
    );
    assert_eq!(dedup.admit(&record), Admit::Duplicate);
}
