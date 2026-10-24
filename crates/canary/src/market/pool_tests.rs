use super::*;

/// A landed swap in direction `eth_in` that returns `amount_out`.
fn landed(eth_in: bool, amount_out: u64) -> SwapEvent {
    let reserves = Reserves {
        eth: U256::from(1_000_000),
        token: U256::from(50_000_000),
    };
    SwapEvent {
        eth_in,
        amount_in: U256::from(10_000),
        amount_out: U256::from(amount_out),
        before: reserves,
        after: reserves,
        seq: U256::from(1),
    }
}

#[test]
fn a_buy_gives_a_sell_of_its_tokens_and_the_sell_gives_a_buy() {
    let mut bought = Bought::default();
    assert_eq!(bought.next_swap(), Side::Buy);
    bought.note_swap(&landed(true, 497));
    assert_eq!(bought.next_swap(), Side::Sell(U256::from(497)));
    bought.note_swap(&landed(false, 9_940));
    assert_eq!(bought.next_swap(), Side::Buy);
}

#[test]
fn a_failed_sell_gives_a_buy_next() {
    let mut bought = Bought::default();
    bought.note_swap(&landed(true, 497));
    assert_eq!(bought.next_swap(), Side::Sell(U256::from(497)));
    // The sell fails before a receipt shows it: no note follows.
    assert_eq!(bought.next_swap(), Side::Buy);
}

#[test]
fn a_buy_of_zero_tokens_gives_a_buy_next() {
    let mut bought = Bought::default();
    bought.note_swap(&landed(true, 0));
    assert_eq!(bought.next_swap(), Side::Buy);
}
