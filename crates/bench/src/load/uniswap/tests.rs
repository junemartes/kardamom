use alloy_consensus::Transaction as _;
use alloy_eips::eip2718::Decodable2718 as _;
use alloy_primitives::TxKind;

use super::*;
use crate::ANVIL_MNEMONIC;

const CHAIN_ID: u64 = 412_346;
const SINK: Address = Address::repeat_byte(0xEE);

/// The signed run of `senders` senders that start at `starts`, with
/// `per_sender` load transactions each.
struct Fixture {
    signers: SignerSet,
    contracts: UniswapContracts,
    run: UniswapRun,
}

impl Fixture {
    fn new(starts: &[u64], mix: UniswapMix, per_sender: usize) -> Self {
        let count = u32::try_from(starts.len()).unwrap();
        let signers = SignerSet::derive(ANVIL_MNEMONIC, count).unwrap();
        let params = TxPlanParams {
            chain_id: CHAIN_ID,
            nonce_start: 0,
            gas_price: 1_000_000_000,
        };
        let plan = UniswapPlan::new(&signers, starts, params, mix).unwrap();
        let run = plan
            .sign(&TxShape::plain(SINK, U256::from(1u64)), per_sender)
            .unwrap();
        let contracts = plan.contracts().clone();
        Self {
            signers,
            contracts,
            run,
        }
    }
}

fn decode(tx: &PlannedTx) -> alloy_consensus::TxEnvelope {
    alloy_consensus::TxEnvelope::decode_2718(&mut tx.raw.as_ref()).unwrap()
}

fn mix(pct: u8, pairs: usize) -> UniswapMix {
    UniswapMix {
        swap_share: SwapShare::new(pct).unwrap(),
        pairs: NonZeroUsize::new(pairs).unwrap(),
    }
}

#[test]
fn the_router_targets_the_pairs_the_factory_creates() {
    let pair_code = hex::decode(PAIR_CREATION_HEX).unwrap();
    let init_hash = hex::encode(keccak256(&pair_code));
    assert!(
        ROUTER_CREATION_HEX.contains(&init_hash),
        "UniswapV2Library.pairFor must use the init code hash {init_hash} of the core build"
    );
    assert!(
        FACTORY_CREATION_HEX.contains(PAIR_CREATION_HEX),
        "the factory must create the pair code that the hash covers"
    );
}

#[test]
fn each_hundred_places_hold_exactly_the_swap_share() {
    let shares = [0u8, 1, 33, 50, 99, 100];
    let cases = shares
        .into_iter()
        .flat_map(|pct| [0u128, 7, 1_000_003].map(move |start| (pct, start)));
    let counts: Vec<(u8, u128)> = cases
        .map(|(pct, start)| {
            let share = SwapShare::new(pct).unwrap();
            (
                pct,
                share.swaps_before(start + 100) - share.swaps_before(start),
            )
        })
        .collect();
    let want: Vec<(u8, u128)> = shares
        .into_iter()
        .flat_map(|pct| [(pct, u128::from(pct)); 3])
        .collect();
    assert_eq!(counts, want);
}

#[test]
fn a_swap_share_above_100_or_not_a_number_is_an_error() {
    assert!(SwapShare::new(101).is_err());
    assert!("101".parse::<SwapShare>().is_err());
    assert!("half".parse::<SwapShare>().is_err());
    assert_eq!("30".parse::<SwapShare>().unwrap(), SwapShare(30));
}

#[test]
fn addresses_and_nonces_follow_the_deployer_nonces() {
    let fx = Fixture::new(&[5, 0, 9], mix(50, 2), 6);
    let deployer = fx.signers[0].address;
    let expect = UniswapContracts::at(deployer, 5, NonZeroUsize::new(2).unwrap()).unwrap();
    assert_eq!(fx.contracts.factory, expect.factory);
    assert_eq!(fx.contracts.router, expect.router);
    assert_eq!(
        fx.contracts.factory,
        deployer.create(9),
        "4 tokens at 5..=8"
    );
    assert_eq!(fx.contracts.router, deployer.create(10));

    // Each create lands at the address its nonce gives.
    let created: Vec<Address> = fx
        .run
        .deploys
        .iter()
        .filter(|tx| decode(tx).kind() == TxKind::Create)
        .map(|tx| deployer.create(tx.nonce))
        .collect();
    let mut want = fx.contracts.tokens.clone();
    want.extend([fx.contracts.factory, fx.contracts.router]);
    assert_eq!(created, want);
    // Two pairs: create, two liquidity mints, and the LP mint each.
    assert_eq!(fx.run.deploys.len(), 4 + 2 + 2 * 4);

    let firsts = |queues: &[Vec<PlannedTx>]| queues.iter().map(|q| q[0].nonce).collect::<Vec<_>>();
    assert_eq!(fx.run.deploys[0].nonce, 5);
    assert_eq!(firsts(&fx.run.sender_setup), [5 + 14, 0, 9]);
    assert_eq!(firsts(&fx.run.queues), [5 + 14 + 4, 4, 13]);
}

#[test]
fn each_pair_address_is_the_create2_address_of_its_sorted_tokens() {
    let a = Address::repeat_byte(0x22);
    let b = Address::repeat_byte(0x11);
    let factory = Address::repeat_byte(0xFA);
    let hash = B256::repeat_byte(0x77);
    let pair = UniswapPair::new(factory, hash, a, b);
    assert_eq!((pair.token0, pair.token1), (b, a));
    assert_eq!(pair, UniswapPair::new(factory, hash, b, a));
    let salt = keccak256([b.as_slice(), a.as_slice()].concat());
    assert_eq!(pair.address, factory.create2(salt, hash));
}

#[test]
fn the_load_holds_the_swap_share_and_alternates_the_direction() {
    let fx = Fixture::new(&[0; 4], mix(30, 3), 100);
    fx.run
        .queues
        .iter()
        .enumerate()
        .for_each(|(sender, queue)| {
            let me = fx.signers[sender].address;
            let pair = fx.contracts.pairs[sender % 3];
            let swaps: Vec<IUniswapV2Router02::swapExactTokensForTokensCall> = queue
                .iter()
                .filter(|tx| tx.class == TxClass::Swap)
                .map(decode)
                .inspect(|tx| assert_eq!(tx.to(), Some(fx.contracts.router)))
                .map(|tx| {
                    IUniswapV2Router02::swapExactTokensForTokensCall::abi_decode(tx.input())
                        .unwrap()
                })
                .collect();
            assert_eq!(swaps.len(), 30, "sender {sender}");
            swaps.iter().enumerate().for_each(|(k, swap)| {
                assert_eq!(swap.path, pair.path(k % 2 == 0), "sender {sender} swap {k}");
                assert_eq!((swap.to, swap.amountIn), (me, SWAP_IN));
                assert!(swap.amountOutMin > U256::ZERO);
                assert!(swap.amountOutMin < SWAP_IN * U256::from(997u64) / U256::from(1000u64));
            });
            queue
                .iter()
                .filter(|tx| tx.class == TxClass::Transfer)
                .map(decode)
                .for_each(|tx| assert_eq!((tx.to(), tx.input().len()), (Some(SINK), 0)));
        });
}

#[test]
fn the_minimum_output_holds_after_every_planned_swap_goes_one_way() {
    let fx = Fixture::new(&[0; 3], mix(100, 1), 40);
    let min_out = IUniswapV2Router02::swapExactTokensForTokensCall::abi_decode(
        decode(&fx.run.queues[0][0]).input(),
    )
    .unwrap()
    .amountOutMin;
    // All 120 swaps of the run on the one pair, in one direction.
    let last = (0..120).fold(
        Reserves {
            input: PAIR_LIQUIDITY,
            output: PAIR_LIQUIDITY,
        },
        |r, _| {
            let out = r.amount_out(SWAP_IN);
            assert!(out >= min_out, "{out} < {min_out}");
            Reserves {
                input: r.input + SWAP_IN,
                output: r.output - out,
            }
        },
    );
    assert!(last.amount_out(SWAP_IN) < min_out + U256::from(10u64).pow(U256::from(9u64)));
}

#[test]
fn each_sender_mints_and_approves_on_its_home_pair() {
    let fx = Fixture::new(&[0; 3], mix(50, 2), 1);
    fx.run
        .sender_setup
        .iter()
        .enumerate()
        .for_each(|(sender, setup)| {
            let pair = fx.contracts.pairs[sender % 2];
            let targets: Vec<Option<Address>> = setup.iter().map(|tx| decode(tx).to()).collect();
            assert_eq!(
                targets,
                [pair.token0, pair.token1, pair.token0, pair.token1].map(Some)
            );
            let approve = IMintErc20::approveCall::abi_decode(decode(&setup[3]).input()).unwrap();
            assert_eq!(approve.spender, fx.contracts.router);
        });
}
