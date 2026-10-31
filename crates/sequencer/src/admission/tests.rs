use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, U256};

use super::{AccountState, Admission, AdmissionConfig, Candidate, ImagePos, Verdict};

const COST: u64 = 100;

fn cfg() -> AdmissionConfig {
    AdmissionConfig {
        max_parked: NonZeroUsize::new(4).unwrap(),
        max_forwarded: NonZeroUsize::new(8).unwrap(),
        loss_blocks: NonZeroU64::new(3).unwrap(),
        park_width: NonZeroU64::new(1_000).unwrap(),
        park_ttl: Duration::from_secs(4),
        relook_after: Duration::from_millis(500),
        max_senders: NonZeroUsize::new(16).unwrap(),
    }
}

fn sender(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

/// A transaction of `who` at `nonce`, at `unit` of image (0, 1), with payload `nonce`.
/// The ingress `who + 100` published it.
fn tx(who: u8, nonce: u64, unit: u64) -> Candidate<u64> {
    Candidate {
        sender: sender(who),
        nonce,
        tx_hash: B256::with_last_byte(u8::try_from(nonce).unwrap_or(u8::MAX)),
        cost: U256::from(COST),
        image: ImagePos {
            lane: 0,
            session: 1,
            unit,
        },
        ingress: u16::from(who) + 100,
        answered: false,
        payload: nonce,
    }
}

fn state(nonce: u64, balance: u64, tx_idx: u64) -> AccountState {
    AccountState {
        nonce,
        balance: U256::from(balance),
        delegated: false,
        tx_idx,
    }
}

/// The verdicts as short strings: `F<nonce>`, `R<nonce>:<refusal>`, `L`,
/// `A<nonce>`. A forward or a reject of a transaction that has its answer
/// gets a `*`.
fn names(out: &[Verdict<u64>]) -> Vec<String> {
    out.iter()
        .map(|v| match v {
            Verdict::Forward(c, _) => format!("F{}{}", c.nonce, star(c)),
            Verdict::Reject(c, r) => format!("R{}{}:{r:?}", c.nonce, star(c)),
            Verdict::SealerRefused(c, r) => format!("S{}{}:{r:?}", c.nonce, star(c)),
            Verdict::DropReference(c) => format!("D{}{}", c.nonce, star(c)),
            Verdict::Lookup(_) => "L".to_owned(),
            Verdict::Accept(ack) => format!("A{}", ack.nonce),
        })
        .collect()
}

fn star(c: &Candidate<u64>) -> &'static str {
    if c.answered { "*" } else { "" }
}

/// A filter that holds `state` for sender 1.
fn known(s: AccountState) -> Admission<u64> {
    let mut a = Admission::new(cfg(), Instant::now());
    a.on_lookup(sender(1), s, &mut Vec::new());
    a
}

fn offer(a: &mut Admission<u64>, c: Candidate<u64>) -> Vec<String> {
    let mut out = Vec::new();
    a.offer(c, &mut out);
    names(&out)
}

#[test]
fn a_run_goes_out_while_the_balance_covers_its_summed_cost() {
    let mut a = known(state(5, 3 * COST, 1));
    assert_eq!(offer(&mut a, tx(1, 5, 1)), ["F5"]);
    assert_eq!(offer(&mut a, tx(1, 6, 2)), ["F6"]);
    assert_eq!(offer(&mut a, tx(1, 7, 3)), ["F7"]);
    let refused = offer(&mut a, tx(1, 8, 4));
    assert!(
        refused[0].starts_with("R8:InsufficientFunds"),
        "{refused:?}"
    );
}

#[test]
fn a_newer_state_confirms_the_run_and_frees_its_cost() {
    let mut a = known(state(5, 2 * COST, 1));
    offer(&mut a, tx(1, 5, 1));
    offer(&mut a, tx(1, 6, 2));
    assert!(offer(&mut a, tx(1, 7, 3))[0].starts_with("R7:Insufficient"));
    let mut out = Vec::new();
    a.on_rows(1, [(sender(1), state(7, 2 * COST, 2))], &mut out);
    assert_eq!(offer(&mut a, tx(1, 7, 4)), ["F7"]);
}

#[test]
fn a_state_that_is_not_newer_changes_nothing() {
    let mut a = known(state(5, COST, 9));
    offer(&mut a, tx(1, 5, 1));
    let mut out = Vec::new();
    a.on_rows(1, [(sender(1), state(6, 10 * COST, 9))], &mut out);
    assert!(offer(&mut a, tx(1, 6, 2))[0].starts_with("R6:Insufficient"));
}

#[test]
fn a_gap_parks_and_the_gap_filler_frees_the_run_behind_it() {
    let mut a = known(state(5, 10 * COST, 1));
    assert_eq!(
        offer(&mut a, tx(1, 7, 1)),
        ["A7"],
        "a parked one is in the mempool"
    );
    assert_eq!(offer(&mut a, tx(1, 6, 2)), ["A6"]);
    assert_eq!(
        offer(&mut a, tx(1, 5, 3)),
        ["F5", "F6*", "F7*"],
        "the run goes out with no second answer"
    );
}

#[test]
fn a_sender_with_no_state_asks_once_and_the_answer_frees_its_transactions() {
    let mut a = Admission::new(cfg(), Instant::now());
    assert_eq!(offer(&mut a, tx(1, 3, 1)), ["L"]);
    assert!(offer(&mut a, tx(1, 4, 2)).is_empty(), "one lookup is out");
    let mut out = Vec::new();
    a.on_lookup(sender(1), state(3, 10 * COST, 1), &mut out);
    assert_eq!(names(&out), ["F3", "F4"]);
}

#[test]
fn a_past_nonce_and_a_nonce_in_flight_are_refused() {
    let mut a = known(state(5, 10 * COST, 1));
    assert_eq!(
        offer(&mut a, tx(1, 4, 1)),
        ["R4:NonceTooLow { chain_nonce: 5 }"]
    );
    offer(&mut a, tx(1, 5, 2));
    assert_eq!(
        offer(&mut a, tx(1, 5, 3)),
        ["R5:InFlight { expected_nonce: 6 }"]
    );
    offer(&mut a, tx(1, 7, 4));
    assert_eq!(
        offer(&mut a, tx(1, 7, 5)),
        ["R7:InFlight { expected_nonce: 6 }"]
    );
}

#[test]
fn a_new_state_refuses_the_parked_transactions_below_its_nonce() {
    let mut a = known(state(5, 10 * COST, 1));
    offer(&mut a, tx(1, 7, 1));
    let mut out = Vec::new();
    a.on_rows(1, [(sender(1), state(8, 10 * COST, 2))], &mut out);
    assert_eq!(names(&out), ["R7*:NonceTooLow { chain_nonce: 8 }"]);
}

#[test]
fn an_account_with_delegated_code_gets_one_transaction_at_a_time() {
    let mut a = known(AccountState {
        delegated: true,
        ..state(5, 10 * COST, 1)
    });
    assert_eq!(offer(&mut a, tx(1, 5, 1)), ["F5"]);
    assert_eq!(offer(&mut a, tx(1, 6, 2)), ["A6"], "the next one waits");
    let mut out = Vec::new();
    a.on_rows(
        1,
        [(
            sender(1),
            AccountState {
                delegated: true,
                ..state(6, 10 * COST, 2)
            },
        )],
        &mut out,
    );
    assert_eq!(names(&out), ["F6*"]);
}

#[test]
fn a_run_that_no_state_confirms_is_lost_after_the_loss_window() {
    let mut a = known(state(5, 2 * COST, 1));
    offer(&mut a, tx(1, 5, 1));
    offer(&mut a, tx(1, 6, 2));
    let mut out = Vec::new();
    a.on_rows(2, [], &mut out);
    a.on_rows(4, [], &mut out);
    a.sweep(Instant::now(), &mut out);
    assert!(
        offer(&mut a, tx(1, 5, 3))[0].starts_with("R5:InFlight"),
        "the window of 3 blocks starts at block 2, the first rows after the forward"
    );
    a.on_rows(5, [], &mut out);
    a.sweep(Instant::now(), &mut out);
    assert_eq!(
        offer(&mut a, tx(1, 5, 4)),
        ["F5"],
        "the sender sends from its chain nonce again"
    );
}

#[test]
fn a_parked_transaction_expires_when_its_image_moves_past_it() {
    let mut a = known(state(5, 10 * COST, 1));
    offer(&mut a, tx(1, 9, 10));
    offer(&mut a, tx(2, 0, 1_009));
    let mut out = Vec::new();
    a.sweep(Instant::now(), &mut out);
    assert!(out.is_empty(), "999 units is inside the width");
    offer(&mut a, tx(2, 1, 1_010));
    a.sweep(Instant::now(), &mut out);
    assert_eq!(
        names(&out),
        ["R9*:Expired { expected_nonce: 5 }"],
        "the client had its answer: the expiry is no second one"
    );
}

#[test]
fn a_sweep_forgets_the_idle_senders_above_the_bound() {
    let mut a: Admission<u64> = Admission::new(cfg(), Instant::now());
    let mut out = Vec::new();
    (0..32u8).for_each(|b| a.on_lookup(sender(b), state(0, COST, 1), &mut out));
    a.offer(tx(1, 0, 1), &mut out);
    a.sweep(Instant::now(), &mut out);
    assert_eq!(a.senders(), 1, "only the sender with a forwarded run stays");
}

#[test]
fn a_rollback_makes_each_sender_ask_again() {
    let mut a = known(state(5, 10 * COST, 1));
    a.on_rollback();
    assert_eq!(offer(&mut a, tx(1, 5, 1)), ["L"]);
}

#[test]
fn a_parked_transaction_expires_when_it_waited_too_long() {
    let start = Instant::now();
    let mut a: Admission<u64> = Admission::new(cfg(), start);
    a.on_rows(1, [(sender(1), state(5, 10 * COST, 1))], &mut Vec::new());
    offer(&mut a, tx(1, 9, 10));
    let mut out = Vec::new();
    a.sweep(start + Duration::from_secs(3), &mut out);
    assert!(out.is_empty(), "three seconds is inside the time to live");
    a.sweep(start + Duration::from_secs(4), &mut out);
    assert_eq!(names(&out), ["R9*:Expired { expected_nonce: 5 }"]);
}

/// A lookup that no source answers ends without an event. The expiry of
/// the parked transaction ends it in the filter too, so the next
/// transaction of the sender asks again, and a source that returned in
/// the meantime can answer it.
#[test]
fn a_sender_asks_again_after_its_unanswered_lookup_expired() {
    let start = Instant::now();
    let mut a: Admission<u64> = Admission::new(cfg(), start);
    assert_eq!(offer(&mut a, tx(1, 5, 1)), ["L"]);
    assert_eq!(
        offer(&mut a, tx(1, 6, 2)),
        Vec::<String>::new(),
        "one lookup is out"
    );
    let mut out = Vec::new();
    a.sweep(start + Duration::from_secs(4), &mut out);
    assert_eq!(
        out.len(),
        2,
        "both parked transactions expire: {:?}",
        names(&out)
    );
    assert_eq!(offer(&mut a, tx(1, 5, 3)), ["L"], "the sender asks again");
    a.on_lookup(sender(1), state(5, 10 * COST, 1), &mut out);
    out.clear();
    assert_eq!(offer(&mut a, tx(1, 6, 4)), ["F6"]);
}

/// A relook that no source answers ends without an event too. The
/// expiry of the parked transaction ends it in the filter, so the next
/// gap park of the sender relooks again.
#[test]
fn a_sender_relooks_again_after_its_unanswered_relook_expired() {
    let start = Instant::now();
    let mut a: Admission<u64> = Admission::new(cfg(), start);
    let mut out = Vec::new();
    a.on_lookup(sender(1), state(48, 10 * COST, 390), &mut out);
    offer(&mut a, tx(1, 49, 1));
    a.sweep(start + Duration::from_millis(600), &mut out);
    assert_eq!(names(&out), ["L"], "the relook asks");
    out.clear();
    a.sweep(start + Duration::from_secs(5), &mut out);
    assert_eq!(out.len(), 1, "the park expires: {:?}", names(&out));
    out.clear();
    offer(&mut a, tx(1, 49, 2));
    a.sweep(start + Duration::from_millis(5600), &mut out);
    assert_eq!(names(&out), ["L"], "the next gap park relooks again");
}

#[test]
fn a_transaction_that_waits_for_a_lookup_gets_its_ack_with_the_state() {
    let mut a: Admission<u64> = Admission::new(cfg(), Instant::now());
    assert_eq!(offer(&mut a, tx(1, 5, 1)), ["L"], "no ack before the state");
    assert_eq!(offer(&mut a, tx(1, 7, 2)), Vec::<String>::new());
    let mut out = Vec::new();
    a.on_lookup(sender(1), state(5, 10 * COST, 1), &mut out);
    assert_eq!(
        names(&out),
        ["F5", "A7"],
        "5 goes out; 7 waits for 6, accepted"
    );
}

/// Position 0 names the empty chain and the state after its first
/// transaction. The rows of that transaction replace a lookup answer of
/// the empty chain.
#[test]
fn the_rows_of_the_first_transaction_replace_an_answer_of_the_empty_chain() {
    let mut f = Admission::new(cfg(), Instant::now());
    let mut out = Vec::new();
    f.on_lookup(sender(1), state(0, 10 * COST, 0), &mut out);
    f.offer(tx(1, 0, 1), &mut out);
    f.on_rows(19, [(sender(1), state(1, 9 * COST, 0))], &mut out);
    out.clear();
    f.offer(tx(1, 1, 2), &mut out);
    assert!(matches!(out.as_slice(), [Verdict::Forward(c, _)] if c.nonce == 1));
}

/// A transaction forwarded before any rows came is not lost when the
/// first rows name a block far above the loss window. Its window starts
/// at that block.
#[test]
fn the_loss_window_starts_at_the_first_rows_after_the_forward() {
    let mut f = Admission::new(cfg(), Instant::now());
    let mut out = Vec::new();
    f.on_lookup(sender(1), state(0, 10 * COST, 0), &mut out);
    f.offer(tx(1, 0, 1), &mut out);
    f.on_rows(100, [(sender(2), state(0, COST, 5))], &mut out);
    f.sweep(Instant::now(), &mut out);
    assert_eq!(
        offer(&mut f, tx(1, 0, 2)),
        ["R0:InFlight { expected_nonce: 1 }"],
        "still forwarded"
    );
    let window = cfg().loss_blocks.get();
    f.on_rows(100 + window, [(sender(2), state(0, COST, 6))], &mut out);
    f.sweep(Instant::now(), &mut out);
    assert_eq!(offer(&mut f, tx(1, 0, 3)), ["F0"], "lost after the window");
}

/// A state from a lookup can be behind the chain. While a transaction
/// parks behind a gap over it, the sender asks again when its relook is
/// due, and the newer answer frees the transaction.
#[test]
fn a_gap_over_a_lookup_state_asks_again() {
    let start = Instant::now();
    let mut a: Admission<u64> = Admission::new(cfg(), start);
    let mut out = Vec::new();
    a.on_lookup(sender(1), state(48, 10 * COST, 390), &mut out);
    offer(&mut a, tx(1, 49, 1));
    a.sweep(start + Duration::from_millis(100), &mut out);
    assert!(out.is_empty(), "the relook is not due yet");
    a.sweep(start + Duration::from_millis(600), &mut out);
    assert_eq!(names(&out), ["L"], "the sender asks again");
    out.clear();
    a.on_lookup(sender(1), state(48, 10 * COST, 390), &mut out);
    a.sweep(start + Duration::from_millis(1200), &mut out);
    assert_eq!(names(&out), ["L"], "the same stale answer asks again");
    out.clear();
    a.on_lookup(sender(1), state(49, 10 * COST, 392), &mut out);
    assert_eq!(names(&out), ["F49*"]);
}

/// A gap over rows is a missing transaction: the sender does not ask again.
#[test]
fn a_gap_over_rows_does_not_ask_again() {
    let start = Instant::now();
    let mut a: Admission<u64> = Admission::new(cfg(), start);
    let mut out = Vec::new();
    a.on_rows(1, [(sender(1), state(48, 10 * COST, 390))], &mut out);
    offer(&mut a, tx(1, 50, 1));
    a.sweep(start + Duration::from_secs(2), &mut out);
    assert!(out.is_empty(), "no lookup for a gap over rows");
}

/// A run that the sealer refused ends at once: a resend at the refused
/// nonce goes out, with no wait for the loss window.
#[test]
fn a_refused_transaction_can_be_sent_again_at_once() {
    let mut a = known(state(5, 2 * COST, 1));
    offer(&mut a, tx(1, 5, 1));
    assert!(offer(&mut a, tx(1, 5, 2))[0].starts_with("R5:InFlight"));
    let mut out = Vec::new();
    a.on_refused(sender(1), 5, &mut out);
    assert_eq!(names(&out), [] as [&str; 0]);
    assert_eq!(offer(&mut a, tx(1, 5, 3)), ["F5"]);
}

/// A refusal of a nonce that the run does not hold changes nothing.
#[test]
fn a_refusal_of_another_nonce_keeps_the_run() {
    let mut a = known(state(5, 2 * COST, 1));
    offer(&mut a, tx(1, 5, 1));
    a.on_refused(sender(1), 9, &mut Vec::new());
    a.on_refused(sender(2), 5, &mut Vec::new());
    assert!(offer(&mut a, tx(1, 5, 2))[0].starts_with("R5:InFlight"));
}

/// The filter keeps each forwarded transaction until a state confirms it.
/// A lost run keeps nothing.
#[test]
fn the_filter_keeps_the_unconfirmed_run_until_a_state_confirms_it() {
    let mut a = known(state(5, 9 * COST, 1));
    (5..8).for_each(|n| {
        offer(&mut a, tx(1, n, n));
    });
    let payloads =
        |a: &Admission<u64>| -> Vec<u64> { a.unconfirmed().map(|c| c.payload).collect() };
    assert_eq!(payloads(&a), [5, 6, 7]);
    assert_eq!(a.unconfirmed_at(sender(1), 6).map(|c| c.nonce), Some(6));
    assert!(a.unconfirmed_at(sender(1), 8).is_none());
    assert!(a.unconfirmed_at(sender(2), 6).is_none());

    a.on_rows(1, [(sender(1), state(6, 9 * COST, 2))], &mut Vec::new());
    assert_eq!(payloads(&a), [6, 7]);
    assert!(a.unconfirmed_at(sender(1), 5).is_none());

    a.on_refused(sender(1), 7, &mut Vec::new());
    assert_eq!(payloads(&a), [] as [u64; 0]);
}

/// The floor seam: a row of a sender with no state sets nothing. The
/// sender keeps its lookup, and a lookup answer sets the first state.
#[cfg(feature = "floor-seams")]
#[test]
fn with_the_floor_seam_a_row_sets_no_first_state_and_only_a_lookup_does() {
    let mut a = Admission::new(cfg(), Instant::now());
    let mut out = Vec::new();
    assert!(
        a.waits_for_lookup(sender(1), &mut out),
        "an unknown sender waits"
    );
    assert!(out.is_empty(), "a sender with no parked work asks nothing");
    assert_eq!(offer(&mut a, tx(1, 3, 1)), ["L"]);
    assert!(a.waits_for_lookup(sender(1), &mut out));
    assert!(out.is_empty(), "one lookup is out");
    a.on_lookup(sender(1), state(3, 10 * COST, 1), &mut out);
    assert_eq!(names(&out), ["F3"]);
    out.clear();
    assert!(
        !a.waits_for_lookup(sender(1), &mut out),
        "a known sender takes its rows"
    );
}

/// The floor seam after a rollback: the cleared sender waits for a lookup
/// again, and its parked work asks one.
#[cfg(feature = "floor-seams")]
#[test]
fn with_the_floor_seam_a_rollback_makes_the_sender_wait_for_a_lookup_again() {
    let mut a = known(state(5, 10 * COST, 1));
    assert_eq!(offer(&mut a, tx(1, 7, 1)), ["A7"]);
    a.on_rollback();
    let mut out = Vec::new();
    assert!(a.waits_for_lookup(sender(1), &mut out));
    assert_eq!(names(&out), ["L"]);
}
