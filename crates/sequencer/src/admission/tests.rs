use std::num::{NonZeroU64, NonZeroUsize};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, U256};

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
        cost: U256::from(COST),
        image: ImagePos {
            lane: 0,
            session: 1,
            unit,
        },
        ingress: u16::from(who) + 100,
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

/// The verdicts as short strings: `F<nonce>`, `R<nonce>:<refusal>`, `L`.
fn names(out: &[Verdict<u64>]) -> Vec<String> {
    out.iter()
        .map(|v| match v {
            Verdict::Forward(c) => format!("F{}", c.nonce),
            Verdict::Reject(c, r) => format!("R{}:{r:?}", c.nonce),
            Verdict::Lookup(_) => "L".to_owned(),
        })
        .collect()
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
    assert!(offer(&mut a, tx(1, 7, 1)).is_empty());
    assert!(offer(&mut a, tx(1, 6, 2)).is_empty());
    assert_eq!(offer(&mut a, tx(1, 5, 3)), ["F5", "F6", "F7"]);
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
    assert_eq!(names(&out), ["R7:NonceTooLow { chain_nonce: 8 }"]);
}

#[test]
fn an_account_with_delegated_code_gets_one_transaction_at_a_time() {
    let mut a = known(AccountState {
        delegated: true,
        ..state(5, 10 * COST, 1)
    });
    assert_eq!(offer(&mut a, tx(1, 5, 1)), ["F5"]);
    assert!(offer(&mut a, tx(1, 6, 2)).is_empty(), "the next one waits");
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
    assert_eq!(names(&out), ["F6"]);
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
    assert_eq!(names(&out), ["R9:Expired { expected_nonce: 5 }"]);
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
    assert_eq!(names(&out), ["R9:Expired { expected_nonce: 5 }"]);
}

#[test]
fn a_forwarded_transaction_names_its_ingress_until_a_row_confirms_it() {
    let mut f = Admission::new(cfg(), Instant::now());
    let mut out = Vec::new();
    f.on_lookup(sender(1), state(5, 10 * COST, 1), &mut out);
    f.offer(tx(1, 5, 1), &mut out);
    f.offer(tx(1, 6, 2), &mut out);
    assert_eq!(f.ingress_of(sender(1), 5), Some(101));
    assert_eq!(f.ingress_of(sender(1), 6), Some(101));
    assert_eq!(f.ingress_of(sender(1), 7), None, "not forwarded");
    assert_eq!(f.ingress_of(sender(2), 5), None, "unknown sender");

    f.on_rows(1, [(sender(1), state(6, 9 * COST, 2))], &mut out);
    assert_eq!(
        f.ingress_of(sender(1), 5),
        None,
        "the row confirmed nonce 5"
    );
    assert_eq!(f.ingress_of(sender(1), 6), Some(101));
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
    assert!(matches!(out.as_slice(), [Verdict::Forward(c)] if c.nonce == 1));
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
    assert_eq!(f.ingress_of(sender(1), 0), Some(101), "still forwarded");
    let window = cfg().loss_blocks.get();
    f.on_rows(100 + window, [(sender(2), state(0, COST, 6))], &mut out);
    f.sweep(Instant::now(), &mut out);
    assert_eq!(f.ingress_of(sender(1), 0), None, "lost after the window");
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
    assert_eq!(names(&out), ["F49"]);
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
