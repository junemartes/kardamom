use super::*;
use std::time::Duration;

fn s(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

fn mk(cfg: ResyncConfig) -> (ResyncController, Sender<FloorUpdate>, SharedWatermark) {
    let (c, tx, _reject_tx, w) = resync_channel(cfg, 0);
    (c, tx, w)
}

fn calm_down(c: &mut ResyncController, w: &SharedWatermark, t: &mut Instant) {
    // Boundaries flow (the count advances). No lag flag, no stall.
    // The exit hold elapses across a few observe calls.
    for i in 1..=6u64 {
        w.store(c.last_watermark + i);
        *t += Duration::from_millis(500);
        c.observe(*t);
    }
    assert!(!c.active(), "controller should have exited resync");
}

#[test]
fn starts_in_resync_and_exits_when_calm() {
    let (mut c, _tx, w) = mk(ResyncConfig::default());
    assert!(c.active(), "startup trigger");
    let mut t = Instant::now();
    calm_down(&mut c, &w, &mut t);
}

#[test]
fn watermark_jump_enters() {
    let (mut c, _tx, w) = mk(ResyncConfig::default());
    let mut t = Instant::now();
    calm_down(&mut c, &w, &mut t);
    // Jump past 25% of 2^17 = 32768.
    w.store(c.last_watermark + 40_000);
    t += Duration::from_millis(100);
    c.observe(t);
    assert!(c.active(), "jump must re-enter resync");
}

#[test]
fn feed_lag_flag_enters_even_if_raised_while_loop_was_blocked() {
    let (mut c, _tx, w) = mk(ResyncConfig::default());
    let mut t = Instant::now();
    calm_down(&mut c, &w, &mut t);
    // The FEED thread saw a 30 second boundary-arrival gap while the
    // publish loop was blocked in a session offer. The flag is sticky.
    // The loop consumes it on its next turn, however late that is.
    w.flag_lag(30_000);
    t += Duration::from_millis(70_000);
    c.observe(t);
    assert!(c.active(), "sticky lag flag must enter resync");
    // A second, smaller gap flagged before consumption must not hide
    // a larger one (fetch_max).
    w.flag_lag(12_000);
    w.flag_lag(3_000);
    assert_eq!(w.take_lag(), Some(12_000));
}

#[test]
fn idle_boundaries_do_not_thrash() {
    // Idle traffic: boundaries arrive, but the count never advances. The
    // controller must stay out of resync. Silence is judged by boundary
    // arrival in the feed thread, not by count changes here.
    let (mut c, _tx, w) = mk(ResyncConfig::default());
    let mut t = Instant::now();
    calm_down(&mut c, &w, &mut t);
    for _ in 0..10 {
        t += Duration::from_millis(10_000);
        c.observe(t); // count unchanged, no lag flag raised
        assert!(!c.active(), "idle must not re-enter resync");
    }
}

#[test]
fn small_jump_stays_calm() {
    let (mut c, _tx, w) = mk(ResyncConfig::default());
    let mut t = Instant::now();
    calm_down(&mut c, &w, &mut t);
    w.store(c.last_watermark + 100);
    t += Duration::from_millis(500);
    c.observe(t);
    assert!(!c.active(), "ordinary progress must not trigger");
}

#[test]
fn publish_stall_enters() {
    let (mut c, _tx, w) = mk(ResyncConfig::default());
    let mut t = Instant::now();
    calm_down(&mut c, &w, &mut t);
    c.note_publish_stall(t);
    assert!(!c.active(), "stall below threshold must not trigger");
    t += Duration::from_millis(10_001);
    c.note_publish_stall(t);
    assert!(c.active(), "sustained stall must trigger");
}

#[test]
fn floor_updates_raise_and_report() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate {
        deposit: false,
        sender: s(1),
        executed_nonce: 4,
        invalid_skip: false,
    })
    .unwrap();
    let (raised, confirmations) = c.drain_floor_updates();
    assert_eq!(raised, vec![(s(1), 5)]);
    assert_eq!(confirmations, vec![(s(1), 4)], "every receipt confirms");
    assert_eq!(c.floor(s(1)), Some(5), "nonces 0..=4 proven executed");
    assert_eq!(c.floor(s(2)), None, "unknown sender has no proof");
    // Re-draining with nothing pending raises nothing.
    let (raised, confirmations) = c.drain_floor_updates();
    assert!(raised.is_empty() && confirmations.is_empty());
}

#[test]
fn skip_receipts_confirm_but_never_raise_floors() {
    // A skip receipt proves the ref was ordered (a valid publish
    // confirmation), while it proves no nonce was consumed. It is not
    // floor evidence.
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate {
        deposit: false,
        sender: s(1),
        executed_nonce: 7,
        invalid_skip: true,
    })
    .unwrap();
    let (raised, confirmations) = c.drain_floor_updates();
    assert!(raised.is_empty(), "skip is not floor evidence");
    assert_eq!(c.floor(s(1)), None);
    assert_eq!(confirmations, vec![(s(1), 7)], "skip IS a confirmation");
}

#[test]
fn deposit_receipts_neither_confirm_nor_raise() {
    // Deposits carry a filler nonce 0 and consume no L2 nonce. A deposit
    // must not confirm a same-sender nonce-0 TxRef, and must not raise the
    // floor. The code marks this by tx_type, not by inferring from the
    // nonce.
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate {
        deposit: true,
        sender: s(1),
        executed_nonce: 0,
        invalid_skip: false,
    })
    .unwrap();
    let (raised, confirmations) = c.drain_floor_updates();
    assert!(raised.is_empty() && confirmations.is_empty());
}

/// This test pins a regression: a genuine nonce-0 transaction must confirm
/// its ref, and raise the floor to 1. When nonce 0 was excluded wholesale,
/// a one-transaction sender's ref could never confirm. The unconfirmed
/// ledger then re-offered it on every confirm timeout, forever, rewinding
/// that sender's nonce floor on every sweep for the life of the process.
#[test]
fn genuine_nonce_zero_tx_confirms_and_raises() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate {
        deposit: false,
        sender: s(1),
        executed_nonce: 0,
        invalid_skip: false,
    })
    .unwrap();
    let (raised, confirmations) = c.drain_floor_updates();
    assert_eq!(
        confirmations,
        vec![(s(1), 0)],
        "a real nonce-0 receipt must confirm its published ref"
    );
    assert_eq!(raised, vec![(s(1), 1)], "and prove execution through 0");
}

/// A nonce-0 skip receipt confirms the publish, but is not floor
/// evidence. The two exclusions are independent.
#[test]
fn nonce_zero_skip_confirms_without_raising() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate {
        deposit: false,
        sender: s(1),
        executed_nonce: 0,
        invalid_skip: true,
    })
    .unwrap();
    let (raised, confirmations) = c.drain_floor_updates();
    assert_eq!(confirmations, vec![(s(1), 0)]);
    assert!(raised.is_empty(), "a skip consumed no nonce");
}

#[test]
fn contiguity_rejects_split_drops_from_rewinds() {
    let (mut c, _tx, reject_tx, _w) = resync_channel(ResyncConfig::default(), 0);
    // Gap rejects (nonce >= expected): a rejected batch produces one
    // reject per entry. The drain collapses them into one rewind per
    // sender, at the lowest expected value.
    reject_tx.send((s(1), 9, 7)).unwrap();
    reject_tx.send((s(1), 8, 5)).unwrap();
    reject_tx.send((s(1), 12, 9)).unwrap();
    reject_tx.send((s(2), 3, 3)).unwrap();
    // Committed-proof reject (nonce < expected): the ref sealed long ago,
    // and its dedup entry aged out. This confirms by reject, so drop the
    // entry.
    reject_tx.send((s(3), 0, 1)).unwrap();
    let (drops, mut rewinds) = c.drain_contiguity_rejects();
    rewinds.sort();
    assert_eq!(rewinds, vec![(s(1), 5), (s(2), 3)]);
    assert_eq!(drops, vec![(s(3), 0)]);
    let (drops, rewinds) = c.drain_contiguity_rejects();
    assert!(drops.is_empty() && rewinds.is_empty());
}

#[test]
fn floors_are_monotonic() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate {
        deposit: false,
        sender: s(1),
        executed_nonce: 9,
        invalid_skip: false,
    })
    .unwrap();
    // A lower receipt arriving later (a late multicast frame) must not
    // regress the floor.
    tx.send(FloorUpdate {
        deposit: false,
        sender: s(1),
        executed_nonce: 3,
        invalid_skip: false,
    })
    .unwrap();
    let (raised, _) = c.drain_floor_updates();
    assert_eq!(raised, vec![(s(1), 10)]);
    assert_eq!(c.floor(s(1)), Some(10));
}
