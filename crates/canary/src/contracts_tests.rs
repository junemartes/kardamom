use super::*;

fn log(address: Address, signature: &str, words: &[U256]) -> Log {
    Log {
        address,
        topics: vec![keccak256(signature), B256::ZERO],
        data: Bytes::from(
            words
                .iter()
                .flat_map(U256::to_be_bytes::<32>)
                .collect::<Vec<u8>>(),
        ),
        log_index: None,
    }
}

fn receipt(logs: Vec<Log>) -> Receipt {
    Receipt {
        status: alloy_primitives::U64::from(1),
        logs,
        ..Receipt::default()
    }
}

fn u(n: u64) -> U256 {
    U256::from(n)
}

#[test]
fn the_creation_input_ends_with_the_writers_array() {
    let writers = [Address::repeat_byte(1), Address::repeat_byte(2)];
    let input = Counter::creation(&writers);
    let tail = &input[input.len() - 128..];
    assert_eq!(U256::from_be_slice(&tail[..32]), u(32));
    assert_eq!(U256::from_be_slice(&tail[32..64]), u(2));
    assert_eq!(&tail[76..96], writers[0].as_slice());
    assert_eq!(&tail[108..128], writers[1].as_slice());
}

#[test]
fn the_count_comes_from_this_counters_event() {
    let counter = Counter(Address::repeat_byte(9));
    let sig = "Incremented(address,uint256)";
    let r = receipt(vec![
        log(Address::repeat_byte(8), sig, &[u(7)]),
        log(counter.0, sig, &[u(7)]),
    ]);
    assert_eq!(counter.count_in(&r), Some(u(7)));
    assert_eq!(Counter(Address::ZERO).count_in(&r), None);
}

#[test]
fn the_rust_formula_matches_the_pools_test_vector() {
    // 0.00001 ETH into 0.002 ETH and 100,000 tokens, as the pool's
    // Foundry test computes it.
    let eth = u(2_000_000_000_000_000);
    let tok = U256::from(100_000u64) * U256::from(10).pow(u(18));
    let amount_in = u(10_000_000_000_000);
    let out = Pool::amount_out(amount_in, eth, tok).unwrap();
    let kept = amount_in * u(997);
    assert_eq!(out, kept * tok / (eth * u(1000) + kept));
    // The token amount that buys the same ETH back from the new reserves.
    let (eth_after, tok_after) = (eth + amount_in, tok - out);
    let back = Pool::amount_in_for(amount_in, tok_after, eth_after).unwrap();
    assert!(Pool::amount_out(back, tok_after, eth_after).unwrap() >= amount_in);
    assert!(back > out, "a round trip pays the fee twice");
}

fn swap(amount_out: U256) -> SwapEvent {
    let before = Reserves {
        eth: u(1_000_000),
        token: u(50_000_000),
    };
    let amount_in = u(10_000);
    SwapEvent {
        eth_in: true,
        amount_in,
        amount_out,
        before,
        after: Reserves {
            eth: before.eth + amount_in,
            token: before.token - amount_out,
        },
        seq: u(1),
    }
}

#[test]
fn a_swap_checks_the_formula_and_the_product() {
    let right = Pool::amount_out(u(10_000), u(1_000_000), u(50_000_000)).unwrap();
    assert_eq!(swap(right).check(), Ok(()));
    assert_eq!(swap(right - u(1)).check(), Err(Outcome::SwapMismatch));
    let mut greedy = swap(right);
    greedy.amount_out = u(2_000_000);
    greedy.after.token = greedy.before.token - greedy.amount_out;
    assert_eq!(greedy.check(), Err(Outcome::SwapMismatch));
    let mut shrunk = swap(right);
    shrunk.after.eth = shrunk.before.eth;
    assert_eq!(shrunk.check(), Err(Outcome::SwapMismatch));
}

#[test]
fn a_liquidity_change_checks_shares_and_amounts() {
    let before = Reserves {
        eth: u(2_000),
        token: u(100_000),
    };
    let add = LiquidityEvent {
        added: true,
        eth: u(100),
        token: u(5_001),
        shares: u(100),
        before,
        shares_before: u(2_000),
        seq: u(4),
    };
    assert_eq!(add.check(), Ok(()));
    assert_eq!(
        LiquidityEvent {
            shares: u(101),
            ..add
        }
        .check(),
        Err(Outcome::LiquidityMismatch)
    );
    let remove = LiquidityEvent {
        added: false,
        eth: u(100),
        token: u(5_000),
        ..add
    };
    assert_eq!(remove.check(), Ok(()));
    assert_eq!(
        LiquidityEvent {
            token: u(5_001),
            ..remove
        }
        .check(),
        Err(Outcome::LiquidityMismatch)
    );
}

#[test]
fn the_pool_events_decode() {
    let pool = Pool(Address::repeat_byte(3));
    let sig = "Swap(address,bool,uint256,uint256,uint256,uint256,uint256,uint256,uint256)";
    let words: Vec<U256> = (1..=8).map(u).collect();
    let event = pool
        .swap_in(&receipt(vec![log(pool.0, sig, &words)]))
        .unwrap();
    assert!(event.eth_in);
    assert_eq!(event.after.token, u(7));
    assert_eq!(event.seq, u(8));
    let rwa = Rwa(Address::repeat_byte(4));
    let supply = receipt(vec![log(rwa.0, "Supply(uint256)", &[u(42)])]);
    assert_eq!(rwa.supply_in(&supply), Some(u(42)));
}
