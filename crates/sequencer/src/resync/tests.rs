use super::*;
use alloy_primitives::B256;
use kardamom_types::ReceiptRollback;
use std::time::Duration;

fn s(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

/// The hash of a skipped test entry.
fn h(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

fn mk(cfg: ResyncConfig) -> (ResyncController, Sender<FloorEvent>, SharedWatermark) {
    let ResyncChannel {
        controller,
        floor_tx,
        watermark,
        ..
    } = ResyncChannel::open(cfg, 0).unwrap();
    (controller, floor_tx, watermark)
}

/// `(sender, nonce-or-floor)` pairs.
type Pairs = Vec<(Address, u64)>;

/// The raised floors and the confirmations of one receipt drain.
fn raised_and_confirmed(c: &mut ResyncController) -> (Pairs, Pairs) {
    let ReceiptDrain {
        raised,
        confirmations,
        ..
    } = c.drain_floor_updates();
    (raised, confirmations)
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

/// A controller already past startup and settled ([`calm_down`]), with
/// its [`SharedWatermark`] and the clock instant `calm_down` left off at.
/// Wraps `mk` + `calm_down` for every test that only cares about
/// post-calm behavior (as opposed to `starts_in_resync_and_exits_when_calm`,
/// which tests the calm-down transition itself).
fn calm_controller() -> (ResyncController, SharedWatermark, Instant) {
    let (mut c, _tx, w) = mk(ResyncConfig::default());
    let mut t = Instant::now();
    calm_down(&mut c, &w, &mut t);
    (c, w, t)
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
    let (mut c, w, mut t) = calm_controller();
    // Jump past 25% of 2^17 = 32768.
    w.store(c.last_watermark + 40_000);
    t += Duration::from_millis(100);
    c.observe(t);
    assert!(c.active(), "jump must re-enter resync");
}

#[test]
fn feed_lag_flag_enters_even_if_raised_while_loop_was_blocked() {
    let (mut c, w, mut t) = calm_controller();
    // The FEED thread saw a 30 second boundary-arrival gap while the
    // publish loop was blocked in a session offer. The flag is sticky.
    // The loop consumes it on its next turn, however late that is.
    w.flag_lag(30_000);
    t += Duration::from_secs(70);
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
    let (mut c, _w, mut t) = calm_controller();
    for _ in 0..10 {
        t += Duration::from_secs(10);
        c.observe(t); // count unchanged, no lag flag raised
        assert!(!c.active(), "idle must not re-enter resync");
    }
}

#[test]
fn small_jump_stays_calm() {
    let (mut c, w, mut t) = calm_controller();
    w.store(c.last_watermark + 100);
    t += Duration::from_millis(500);
    c.observe(t);
    assert!(!c.active(), "ordinary progress must not trigger");
}

#[test]
fn publish_stall_enters() {
    let (mut c, _w, mut t) = calm_controller();
    c.note_publish_stall(t);
    assert!(!c.active(), "stall below threshold must not trigger");
    t += Duration::from_millis(10_001);
    c.note_publish_stall(t);
    assert!(c.active(), "sustained stall must trigger");
}

#[test]
fn floor_updates_raise_and_report() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::executed(s(1), 4).into()).unwrap();
    let (raised, confirmations) = raised_and_confirmed(&mut c);
    assert_eq!(raised, vec![(s(1), 5)]);
    assert_eq!(confirmations, vec![(s(1), 4)], "every receipt confirms");
    assert_eq!(c.floor(s(1)), Some(5), "nonces 0..=4 proven executed");
    assert_eq!(c.floor(s(2)), None, "unknown sender has no proof");
    // Re-draining with nothing pending raises nothing.
    let (raised, confirmations) = raised_and_confirmed(&mut c);
    assert!(raised.is_empty() && confirmations.is_empty());
}

#[test]
fn skip_receipts_confirm_but_never_raise_floors() {
    // A skip receipt proves the ref was ordered (a valid publish
    // confirmation), while it proves no nonce was consumed. It is not
    // floor evidence.
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::skip(s(1), 7, kardamom_types::SkipReason::NonceTooLow, h(7)).into())
        .unwrap();
    let (raised, confirmations) = raised_and_confirmed(&mut c);
    assert!(raised.is_empty(), "skip is not floor evidence");
    assert_eq!(c.floor(s(1)), None);
    assert_eq!(confirmations, vec![(s(1), 7)], "skip IS a confirmation");
}

/// A skip uses no nonce, so each skip names the nonce the chain expects
/// next. An insufficient-funds skip names its own nonce. A
/// nonce-too-high skip names the receipt floor, 0 for a sender with no
/// executed receipt. A nonce-too-low skip names nothing, and so does a
/// late skip below the receipt floor.
#[test]
fn skip_receipts_release_to_the_chain_nonce() {
    use kardamom_types::SkipReason;
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::skip(s(1), 0, SkipReason::InsufficientFunds, h(10)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(1), 1, SkipReason::NonceTooHigh, h(11)).into())
        .unwrap();
    tx.send(FloorUpdate::executed(s(2), 4).into()).unwrap();
    tx.send(FloorUpdate::skip(s(2), 9, SkipReason::NonceTooHigh, h(29)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(2), 7, SkipReason::InsufficientFunds, h(27)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(2), 2, SkipReason::NonceTooLow, h(22)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(2), 3, SkipReason::InsufficientFunds, h(23)).into())
        .unwrap();
    let ReceiptDrain {
        raised,
        released,
        sealer,
        ..
    } = c.drain_floor_updates();
    assert_eq!(raised, vec![(s(2), 5)]);
    assert_eq!(
        released,
        vec![(s(1), 0), (s(1), 0), (s(2), 5), (s(2), 7)],
        "each skip names the chain nonce; a too-low or late skip names none"
    );
    assert_eq!(
        sealer,
        vec![release(1, 0, 10), release(2, 7, 27)],
        "only an insufficient-funds skip at or above the floor releases the sealer's guard"
    );
}

#[test]
fn deposit_receipts_neither_confirm_nor_raise() {
    // Deposits carry a filler nonce 0 and consume no L2 nonce. A deposit
    // must not confirm a same-sender nonce-0 TxRef, and must not raise the
    // floor. The code marks this by tx_type, not by inferring from the
    // nonce.
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::deposit(s(1)).into()).unwrap();
    let (raised, confirmations) = raised_and_confirmed(&mut c);
    assert!(raised.is_empty() && confirmations.is_empty());
}

/// A receipt maps to one outcome. A deposit wins over a skip reason, so a
/// deposit never confirms a genuine nonce-0 ref.
#[test]
fn a_receipt_maps_to_its_outcome() {
    use kardamom_types::{Receipt, SkipReason, TX_TYPE_DEPOSIT, TX_TYPE_LEGACY};
    let receipt = |tx_type, skip_reason| Receipt {
        tx_type,
        nonce: 3,
        from: s(1),
        skip_reason,
        ..Receipt::default()
    };
    let cases = [
        (TX_TYPE_LEGACY, None, Outcome::Executed { nonce: 3 }),
        (
            TX_TYPE_LEGACY,
            Some(SkipReason::NonceTooLow),
            Outcome::Skipped {
                nonce: 3,
                reason: SkipReason::NonceTooLow,
                tx_hash: B256::ZERO,
            },
        ),
        (TX_TYPE_DEPOSIT, None, Outcome::Deposit),
        (
            TX_TYPE_DEPOSIT,
            Some(SkipReason::GasLimit),
            Outcome::Deposit,
        ),
    ];
    for (tx_type, skip_reason, outcome) in cases {
        assert_eq!(
            FloorUpdate::of_receipt(&receipt(tx_type, skip_reason)),
            FloorUpdate {
                sender: s(1),
                outcome,
                evidence: Evidence::Receipt { block: 0 },
            }
        );
    }
}

/// This test pins a regression: a genuine nonce-0 transaction must confirm
/// its ref, and raise the floor to 1. When nonce 0 was excluded wholesale,
/// a one-transaction sender's ref could never confirm. The unconfirmed
/// ledger then re-offered it on every confirm timeout, forever, rewinding
/// that sender's nonce floor on every sweep for the life of the process.
#[test]
fn genuine_nonce_zero_tx_confirms_and_raises() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::executed(s(1), 0).into()).unwrap();
    let (raised, confirmations) = raised_and_confirmed(&mut c);
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
    tx.send(FloorUpdate::skip(s(1), 0, kardamom_types::SkipReason::NonceTooLow, h(0)).into())
        .unwrap();
    let (raised, confirmations) = raised_and_confirmed(&mut c);
    assert_eq!(confirmations, vec![(s(1), 0)]);
    assert!(raised.is_empty(), "a skip consumed no nonce");
}

#[test]
fn contiguity_rejects_split_drops_from_rewinds() {
    let ResyncChannel {
        controller: mut c,
        answer_tx,
        ..
    } = ResyncChannel::open(ResyncConfig::default(), 0).unwrap();
    // Gap rejects (nonce >= expected): a rejected batch produces one
    // reject per entry. The drain collapses them into one rewind per
    // sender, at the lowest expected value.
    let reject = |sender, nonce, expected| SealerAnswer::ContiguityReject {
        sender,
        nonce,
        expected,
    };
    answer_tx.send(reject(s(1), 9, 7)).unwrap();
    answer_tx.send(reject(s(1), 8, 5)).unwrap();
    answer_tx.send(reject(s(1), 12, 9)).unwrap();
    answer_tx.send(reject(s(2), 4, 3)).unwrap();
    // Committed-proof reject (nonce < expected): the ref sealed long ago,
    // and its dedup entry aged out. This confirms by reject, so drop the
    // entry.
    answer_tx.send(reject(s(3), 0, 1)).unwrap();
    // A window-full answer names no expected nonce. It is neither a drop
    // nor a gap.
    answer_tx
        .send(SealerAnswer::WindowFull {
            sender: s(4),
            nonce: 6,
        })
        .unwrap();
    let drain = c.drain_answers();
    let mut gaps: Vec<_> = drain.gaps.into_iter().collect();
    gaps.sort();
    assert_eq!(gaps, vec![(s(1), 5), (s(2), 3)]);
    assert_eq!(drain.drops, vec![(s(3), 0, 1)]);
    assert_eq!(drain.window_full, vec![(s(4), 6)]);
    let drain = c.drain_answers();
    assert!(drain.drops.is_empty() && drain.gaps.is_empty() && drain.window_full.is_empty());
}

#[test]
fn floors_are_monotonic() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::executed(s(1), 9).into()).unwrap();
    // A lower receipt arriving later (a late multicast frame) must not
    // regress the floor.
    tx.send(FloorUpdate::executed(s(1), 3).into()).unwrap();
    let (raised, _) = raised_and_confirmed(&mut c);
    assert_eq!(raised, vec![(s(1), 10)]);
    assert_eq!(c.floor(s(1)), Some(10));
}

#[test]
fn channels_are_bounded_to_dedup_capacity() {
    // The floor and reject channels must not grow without limit on a
    // stall: they are bounded to the resync window (`dedup_capacity`),
    // and the producer drops on overflow instead of blocking or growing.
    let cfg = ResyncConfig {
        dedup_capacity: NonZeroU64::new(2).unwrap(),
        // Small enough that 2 * enter_percent / 100 stays nonzero; the
        // enter threshold itself is not what this test checks.
        enter_percent: NonZeroU64::new(100).unwrap(),
        ..ResyncConfig::default()
    };
    // `controller` must stay bound (even if unused): it owns the
    // receiver half of both channels below, and a receiver drop would
    // disconnect the channel, making every `try_send` fail as
    // `Disconnected` instead of demonstrating the `Full` bound this test
    // checks.
    let ResyncChannel {
        controller: _controller,
        floor_tx: tx,
        answer_tx,
        ..
    } = ResyncChannel::open(cfg, 0).unwrap();

    let one = FloorUpdate::executed(s(1), 0);
    assert!(tx.try_send(one.into()).is_ok());
    assert!(tx.try_send(one.into()).is_ok());
    // The channel is now at its 2-entry capacity: a third send must not
    // block, and must not be accepted.
    assert!(matches!(
        tx.try_send(one.into()),
        Err(crossbeam_channel::TrySendError::Full(_))
    ));

    let full = SealerAnswer::WindowFull {
        sender: s(1),
        nonce: 0,
    };
    assert!(answer_tx.try_send(full).is_ok());
    assert!(answer_tx.try_send(full).is_ok());
    assert!(matches!(
        answer_tx.try_send(full),
        Err(crossbeam_channel::TrySendError::Full(_))
    ));
}

#[test]
fn enter_threshold_rejects_a_product_that_rounds_to_zero() {
    // 2 * 25 / 100 == 0: neither `dedup_capacity` nor `enter_percent` is
    // individually invalid (both are nonzero), but the pair yields a
    // threshold of 0 records, which would mean "resync on every
    // watermark tick". This must be a validation error, never rounded
    // up to 1 in place.
    let cfg = ResyncConfig {
        dedup_capacity: NonZeroU64::new(2).unwrap(),
        enter_percent: NonZeroU64::new(25).unwrap(),
        ..ResyncConfig::default()
    };
    assert!(matches!(
        cfg.enter_threshold(),
        Err(ResyncConfigError::ThresholdTooSmall { .. })
    ));
    assert!(matches!(
        cfg.validate(),
        Err(ResyncConfigError::ThresholdTooSmall { .. })
    ));
}

/// A rollback forgets only the floors that a receipt above its block
/// set. A floor of a lower block and a floor of a lookup answer stay. The
/// drain ends at the rollback, so an update after it goes into the next
/// drain.
#[test]
fn a_rollback_forgets_only_the_floors_set_above_its_block() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(receipt_floor(1, 4, 10)).unwrap();
    tx.send(receipt_floor(2, 7, 5)).unwrap();
    tx.send(FloorUpdate::executed(s(3), 9).into()).unwrap();
    tx.send(rollback(5, 0xA1)).unwrap();
    tx.send(receipt_floor(1, 2, 6)).unwrap();

    let mut drain = c.drain_floor_updates();
    assert_eq!(drain.raised, vec![(s(1), 5), (s(2), 8), (s(3), 10)]);
    drain.forgotten.sort_unstable();
    assert_eq!(drain.forgotten, vec![s(1), s(3)]);
    assert_eq!(c.floor(s(1)), None, "a receipt of block 10 is void");
    assert_eq!(c.floor(s(2)), Some(8), "block 5 stays");
    assert_eq!(c.floor(s(3)), None, "a lookup answer from before is void");

    let next = c.drain_floor_updates();
    assert_eq!(next.raised, vec![(s(1), 3)], "the new stream sets it again");
    assert_eq!(next.forgotten, Vec::<Address>::new());
    assert_eq!(c.floor(s(1)), Some(3));
}

/// The floor event of a receipt of `who` at `nonce` in `block`.
fn receipt_floor(who: u8, nonce: u64, block: u64) -> FloorEvent {
    FloorUpdate::executed(s(who), nonce)
        .with_evidence(Evidence::Receipt { block })
        .into()
}

/// The floor event of a lookup answer for `who` at `nonce`, to a request
/// of `generation`.
fn lookup_floor(who: u8, nonce: u64, generation: u64) -> FloorEvent {
    FloorUpdate::executed(s(who), nonce)
        .with_evidence(Evidence::Lookup { generation })
        .into()
}

/// A rollback to `to_block` with the id `id`.
fn rollback(to_block: u64, id: u8) -> FloorEvent {
    FloorEvent::Rollback(ReceiptRollback {
        to_block,
        id: B256::repeat_byte(id),
    })
}

/// An answer to a lookup request from before a rollback can come from a
/// voided state, so it sets no floor and confirms nothing. An answer to a
/// later request sets the floor, and the next rollback voids it.
#[test]
fn a_lookup_answer_to_a_request_from_before_a_rollback_sets_no_floor() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(rollback(5, 0xB1)).unwrap();
    c.drain_floor_updates();
    assert_eq!(c.generation(), 1);

    tx.send(lookup_floor(1, 9, 0)).unwrap();
    let stale = c.drain_floor_updates();
    assert_eq!(stale, ReceiptDrain::default(), "a stale answer is nothing");
    assert_eq!(c.floor(s(1)), None);

    tx.send(lookup_floor(1, 9, 1)).unwrap();
    assert_eq!(c.drain_floor_updates().raised, vec![(s(1), 10)]);
    tx.send(rollback(8, 0xB2)).unwrap();
    assert_eq!(c.drain_floor_updates().forgotten, vec![s(1)]);
    assert_eq!(c.floor(s(1)), None, "a later rollback voids the answer");
}

/// A copy of an applied rollback comes after the receipts of the new
/// stream: it keeps their floors. It voids the lookup answers from before
/// it, because its sender rolled back only now.
#[test]
fn a_copy_of_an_applied_rollback_keeps_the_receipt_floors() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(rollback(5, 0xC1)).unwrap();
    c.drain_floor_updates();
    tx.send(receipt_floor(1, 4, 8)).unwrap();
    tx.send(lookup_floor(2, 6, 1)).unwrap();
    tx.send(rollback(5, 0xC1)).unwrap();
    let drain = c.drain_floor_updates();
    assert_eq!(drain.raised, vec![(s(1), 5), (s(2), 7)]);
    assert_eq!(drain.forgotten, vec![s(2)]);
    assert_eq!(c.floor(s(1)), Some(5), "the new stream stays");
    assert_eq!(c.floor(s(2)), None);
    assert_eq!(c.generation(), 2);
}

/// A floor that a later receipt raised carries the block of that later
/// receipt: a rollback between the two blocks forgets it.
#[test]
fn a_raised_floor_carries_the_block_of_the_receipt_that_raised_it() {
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(
        FloorUpdate::executed(s(1), 1)
            .with_evidence(Evidence::Receipt { block: 2 })
            .into(),
    )
    .unwrap();
    tx.send(
        FloorUpdate::executed(s(1), 3)
            .with_evidence(Evidence::Receipt { block: 8 })
            .into(),
    )
    .unwrap();
    tx.send(FloorEvent::Rollback(ReceiptRollback {
        to_block: 4,
        ..Default::default()
    }))
    .unwrap();
    let drain = c.drain_floor_updates();
    assert_eq!(drain.forgotten, vec![s(1)]);
    assert_eq!(c.floor(s(1)), None);
}

fn release(who: u8, nonce: u64, hash: u8) -> NonceRelease {
    NonceRelease {
        sender: s(who),
        nonce,
        tx_hash: h(hash),
    }
}

/// An insufficient-funds skip proves the chain nonce, so it releases the
/// sealer's guard with no receipt floor: the first transaction of a new
/// key. A skip from a check before the nonce check proves nothing about
/// the nonce: with no floor it sends no release, and with a floor it
/// releases only at the floor. A nonce skip never releases.
#[test]
fn a_release_to_the_sealer_needs_a_proof_of_the_chain_nonce() {
    use kardamom_types::SkipReason;
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::skip(s(1), 0, SkipReason::InsufficientFunds, h(1)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(2), 0, SkipReason::GasLimit, h(2)).into())
        .unwrap();
    tx.send(FloorUpdate::executed(s(3), 2).into()).unwrap();
    tx.send(FloorUpdate::skip(s(3), 3, SkipReason::Fee, h(3)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(3), 1, SkipReason::Fee, h(4)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(3), 4, SkipReason::NonceTooHigh, h(5)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(3), 2, SkipReason::NonceTooLow, h(6)).into())
        .unwrap();
    let ReceiptDrain { sealer, .. } = c.drain_floor_updates();
    assert_eq!(
        sealer,
        vec![release(1, 0, 1), release(3, 3, 3)],
        "a fee skip at the floor releases; below it, or with no floor, none"
    );
}

/// Floor f. Entry f skips for insufficient funds, and f + 1 skips for a fee
/// check that runs before the nonce check. Only f releases: the guard
/// reopens at f, and a late release of f + 1 cannot set it below the chain
/// after f' and f+1' are ordered. A fee skip at the floor itself releases.
#[test]
fn a_skip_before_the_nonce_check_releases_only_at_the_floor() {
    use kardamom_types::SkipReason;
    let (mut c, tx, _w) = mk(ResyncConfig::default());
    tx.send(FloorUpdate::executed(s(1), 4).into()).unwrap();
    tx.send(FloorUpdate::skip(s(1), 5, SkipReason::InsufficientFunds, h(5)).into())
        .unwrap();
    tx.send(FloorUpdate::skip(s(1), 6, SkipReason::Fee, h(6)).into())
        .unwrap();
    tx.send(FloorUpdate::executed(s(2), 1).into()).unwrap();
    tx.send(FloorUpdate::skip(s(2), 2, SkipReason::Fee, h(7)).into())
        .unwrap();
    let ReceiptDrain { sealer, .. } = c.drain_floor_updates();
    assert_eq!(sealer, vec![release(1, 5, 5), release(2, 2, 7)]);
}

/// A sealer that has not reported decision version 3 on the current
/// session gets no release, and a new session forgets the report.
#[test]
fn the_sealer_takes_releases_only_after_it_reported_version_three() {
    let (c, _tx, w) = mk(ResyncConfig::default());
    assert!(!c.sealer_takes_releases());
    w.set_sealer_version(2);
    assert!(!c.sealer_takes_releases());
    w.set_sealer_version(3);
    assert!(c.sealer_takes_releases());
    w.set_sealer_version(0);
    assert!(!c.sealer_takes_releases());
}
