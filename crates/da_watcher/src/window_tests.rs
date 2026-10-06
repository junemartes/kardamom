use alloy_primitives::B256;

use super::*;

fn at(number: u64) -> L1Cursor {
    L1Cursor {
        number,
        hash: B256::left_padding_from(&number.to_be_bytes()),
    }
}

fn epoch(number: u64) -> EpochRecord {
    EpochRecord {
        l1_number: number,
        l1_hash: at(number).hash,
        deposits: Vec::new(),
    }
}

/// A window after 100 that holds 101..=104.
fn window() -> Window {
    let mut w = Window::new(at(100));
    (101..=104).for_each(|n| w.push(epoch(n)));
    w
}

#[test]
fn an_origin_inside_the_window_drops_the_confirmed_epochs() {
    let mut w = window();
    assert_eq!(w.confirm(102), Confirm::Inside);
    assert_eq!(w.base(), at(102), "the base takes the epoch's own hash");
    assert_eq!(w.head(), at(104));
    assert_eq!(w.len(), 2);
    assert_eq!(w.confirm(102), Confirm::Inside, "a repeat changes nothing");
    assert_eq!(w.len(), 2);
    assert_eq!(w.confirm(104), Confirm::Inside);
    assert_eq!(w.len(), 0);
    assert_eq!(w.head(), at(104));
}

#[test]
fn an_origin_outside_the_window_changes_nothing() {
    let mut w = window();
    assert_eq!(w.confirm(99), Confirm::Outside, "the sealer moved back");
    assert_eq!(w.confirm(105), Confirm::Outside, "the sealer moved ahead");
    assert_eq!(w.base(), at(100));
    assert_eq!(w.len(), 4);
}

#[tokio::test(start_paused = true)]
async fn a_republish_is_due_after_the_timeout_and_at_most_once_for_each() {
    let mut w = Window::new(at(100));
    assert_eq!(w.republish_at(), None, "nothing waits");
    w.push(epoch(101));
    let due = w.republish_at().unwrap();
    assert_eq!(due, Instant::now() + REPUBLISH_AFTER);

    tokio::time::advance(REPUBLISH_AFTER).await;
    let again: Vec<u64> = w.republish().map(|e| e.l1_number).collect();
    assert_eq!(again, [101]);
    assert_eq!(w.republish_at().unwrap(), Instant::now() + REPUBLISH_AFTER);

    tokio::time::advance(REPUBLISH_AFTER / 2).await;
    w.push(epoch(102));
    w.confirm(101);
    assert_eq!(
        w.republish_at().unwrap(),
        Instant::now() + REPUBLISH_AFTER,
        "a confirm restarts the timer"
    );
}

#[test]
fn the_window_is_full_at_its_bound() {
    let mut w = Window::new(at(0));
    let bound = u64::try_from(PUBLISH_WINDOW.get()).unwrap();
    (1..bound).for_each(|n| w.push(epoch(n)));
    assert!(!w.is_full());
    w.push(epoch(bound));
    assert!(w.is_full());
    w.confirm(1);
    assert!(!w.is_full());
}
