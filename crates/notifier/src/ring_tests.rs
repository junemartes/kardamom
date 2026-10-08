use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256};
use kardamom_types::{TxError, TxErrorReason, TxStatus};

use super::{Insert, Ring, RingConfig};
use crate::dto::{Stage, StatusFilter};

const AGE: Duration = Duration::from_secs(60);

fn ring(max_events: usize) -> Ring {
    Ring::new(RingConfig {
        max_age: AGE,
        max_events: NonZeroUsize::new(max_events).unwrap(),
    })
}

fn hash(b: u8) -> B256 {
    B256::repeat_byte(b)
}

fn sender(b: u8) -> Address {
    Address::repeat_byte(b)
}

fn stages(ring: &Ring, filter: &StatusFilter) -> Vec<Stage> {
    ring.replay(filter, 0, 100)
        .into_iter()
        .map(|s| s.event.stage)
        .collect()
}

#[test]
fn stores_each_stage_once_and_in_order() {
    let mut r = ring(100);
    let now = Instant::now();
    let h = hash(1);
    assert!(matches!(
        r.insert(&TxStatus::offered(h, sender(1), 7), now),
        Insert::Stored(_)
    ));
    assert!(matches!(
        r.insert(&TxStatus::sealed(h), now),
        Insert::Stored(_)
    ));
    assert_eq!(r.insert(&TxStatus::sealed(h), now), Insert::Duplicate);
    assert_eq!(r.len(), 2);
    assert_eq!(r.transactions(), 1);
    assert_eq!(
        stages(&r, &StatusFilter::TxHash { tx_hash: h }),
        vec![Stage::Offered, Stage::Sealed]
    );
}

#[test]
fn a_sealed_after_an_offered_names_its_sender() {
    let mut r = ring(100);
    let now = Instant::now();
    let h = hash(2);
    r.insert(&TxStatus::offered(h, sender(2), 3), now);
    let Insert::Stored(sealed) = r.insert(&TxStatus::sealed(h), now) else {
        panic!("sealed stored");
    };
    assert_eq!(sealed.event.sender, Some(sender(2)));
    assert_eq!(sealed.event.nonce, Some(3));
    assert_eq!(
        stages(&r, &StatusFilter::Sender { sender: sender(2) }),
        vec![Stage::Offered, Stage::Sealed]
    );
}

#[test]
fn a_sealed_before_an_offered_joins_the_sender_later() {
    let mut r = ring(100);
    let now = Instant::now();
    let h = hash(3);
    let Insert::Stored(sealed) = r.insert(&TxStatus::sealed(h), now) else {
        panic!("sealed stored");
    };
    assert_eq!(sealed.event.sender, None);
    assert_eq!(
        stages(&r, &StatusFilter::Sender { sender: sender(3) }).len(),
        0
    );
    r.insert(&TxStatus::offered(h, sender(3), 0), now);
    // The sender index now covers the earlier sealed event too.
    assert_eq!(
        stages(&r, &StatusFilter::Sender { sender: sender(3) }),
        vec![Stage::Sealed, Stage::Offered]
    );
    assert_eq!(r.resolve(sender(3), 0), Some(h));
    assert_eq!(r.resolve(sender(3), 1), None);
}

#[test]
fn replay_pages_by_sequence_number() {
    let mut r = ring(100);
    let now = Instant::now();
    for i in 0..10u8 {
        r.insert(&TxStatus::offered(hash(i), sender(9), u64::from(i)), now);
    }
    let first = r.replay(&StatusFilter::all(), 0, 4);
    assert_eq!(first.len(), 4);
    let last = first.last().unwrap().seq;
    let second = r.replay(&StatusFilter::all(), last, 4);
    assert_eq!(second.first().unwrap().seq, last + 1);
    let rest = r.replay(&StatusFilter::all(), second.last().unwrap().seq, 4);
    assert_eq!(rest.len(), 2, "a short page is the last one");
    assert_eq!(
        r.replay(&StatusFilter::all(), rest.last().unwrap().seq, 4)
            .len(),
        0
    );
}

#[test]
fn eviction_by_count_drops_the_oldest_and_its_indexes() {
    let mut r = ring(3);
    let now = Instant::now();
    for i in 0..5u8 {
        r.insert(&TxStatus::offered(hash(i), sender(i), 0), now);
    }
    assert_eq!(r.evict(now), 2);
    assert_eq!(r.len(), 3);
    assert_eq!(r.transactions(), 3);
    assert_eq!(
        stages(&r, &StatusFilter::TxHash { tx_hash: hash(0) }).len(),
        0
    );
    assert_eq!(r.resolve(sender(0), 0), None);
    assert_eq!(r.resolve(sender(4), 0), Some(hash(4)));
    // A replay from before the ring starts at its oldest event.
    assert_eq!(r.replay(&StatusFilter::all(), 0, 10).len(), 3);
}

#[test]
fn eviction_by_age_keeps_the_young() {
    let mut r = ring(100);
    let t0 = Instant::now();
    r.insert(&TxStatus::offered(hash(1), sender(1), 0), t0);
    r.insert(&TxStatus::offered(hash(2), sender(2), 0), t0 + AGE);
    assert_eq!(r.evict(t0 + AGE + Duration::from_millis(1)), 1);
    assert_eq!(
        stages(&r, &StatusFilter::TxHash { tx_hash: hash(2) }),
        vec![Stage::Offered]
    );
    // The transaction stays known while one of its events remains.
    r.insert(&TxStatus::sealed(hash(1)), t0 + AGE);
    assert_eq!(r.transactions(), 2);
}

#[test]
fn rejected_carries_the_reason_words() {
    let mut r = ring(100);
    let error = TxError {
        sender: sender(5),
        nonce: 5,
        reason: TxErrorReason::Expired { expected_nonce: 4 },
    };
    let Insert::Stored(s) = r.insert(&TxStatus::rejected(hash(5), &error), Instant::now()) else {
        panic!("stored");
    };
    assert_eq!(s.event.stage, Stage::Rejected);
    assert_eq!(s.event.reason.as_deref(), Some("expired"));
    assert_eq!(s.event.expected_nonce, Some(4));
}

#[test]
fn a_record_lag_refusal_carries_its_word_and_no_nonce() {
    let mut r = ring(100);
    let error = TxError {
        sender: sender(6),
        nonce: 6,
        reason: TxErrorReason::RecordLag {
            sealed_index: 20_000,
            recorded_index: 3_000,
            budget: 16_384,
        },
    };
    let Insert::Stored(s) = r.insert(&TxStatus::rejected(hash(6), &error), Instant::now()) else {
        panic!("stored");
    };
    assert_eq!(s.event.reason.as_deref(), Some("record-lag"));
    assert_eq!(s.event.expected_nonce, None);
}
