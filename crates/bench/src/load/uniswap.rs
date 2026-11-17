//! This is the Uniswap v2 bench workload: router swaps mixed with plain
//! value transfers.
//!
//! The contracts are the vendored v2-core factory and pair, the vendored
//! v2-periphery `UniswapV2Router02`, and `MintERC20` tokens from
//! `bench-contracts`. `just bench-embed` embeds their creation code into
//! `uniswap_bytecode.rs`, so the binary needs no repository files at run
//! time.
//!
//! The setup lands in two phases before the load starts:
//!
//! 1. The first sender creates the tokens, the factory, and the router.
//!    Then it creates each pair and adds the liquidity of each pair.
//!    Every address follows from the first sender and its first nonce.
//! 2. Each sender mints the two tokens of its home pair to itself, and
//!    approves the router for both. The senders do this at the same time.
//!
//! Each swap is one `swapExactTokensForTokens` call. The router moves the
//! input from the sender to the pair and swaps in the same transaction,
//! so no sender can use the input of another sender. A sender alternates
//! the direction of its swaps. Each swap is small against the reserves.
//! Its `amountOutMin` is the output after each other planned swap of the
//! run moved the price against it, so each swap succeeds in any order of
//! the senders.

use std::num::NonZeroUsize;

use alloy_primitives::{Address, B256, U256, hex, keccak256, uint};
use alloy_sol_types::{SolCall as _, SolValue as _};
use anyhow::Context as _;

use crate::load::plan::{PlannedTx, SenderNonces, TxClass, TxPlanParams, TxShape};
use crate::signers::SignerSet;

include!("uniswap_bytecode.rs");

alloy_sol_types::sol! {
    interface IMintErc20 {
        function mint(address to, uint256 amount) external;
        function approve(address spender, uint256 amount) external returns (bool);
    }

    interface IUniswapV2Factory {
        function createPair(address tokenA, address tokenB) external returns (address pair);
    }

    interface IUniswapV2Pair {
        function mint(address to) external returns (uint256 liquidity);
    }

    interface IUniswapV2Router02 {
        function swapExactTokensForTokens(
            uint256 amountIn,
            uint256 amountOutMin,
            address[] calldata path,
            address to,
            uint256 deadline
        ) external returns (uint256[] memory amounts);
    }
}

/// The gas limit of a contract creation. The router is the largest: it
/// stores about 22 kB of code.
const CREATE_GAS: u64 = 8_000_000;
/// The gas limit of `createPair`, which creates the pair contract.
const CREATE_PAIR_GAS: u64 = 4_000_000;
/// The gas limit of a mint, an approve, and the liquidity mint.
const SETUP_CALL_GAS: u64 = 300_000;
/// The gas limit of one router swap. A swap uses about 91k gas, and
/// about 45k more when it is the first swap of a pair in a new block and
/// writes the price accumulators. The sender pays the tip on the full
/// limit, so the limit stays close to the use.
const SWAP_GAS: u64 = 200_000;

/// The liquidity of each token of each pair: 1e30. The pair stores a
/// reserve as `uint112`, about 5.2e33, so this leaves room for the
/// input of 1e15 swaps.
const PAIR_LIQUIDITY: U256 = uint!(1_000_000_000_000_000_000_000_000_000_000_U256);
/// The tokens of each kind that each sender mints to itself: 1e27, the
/// input of 1e9 swaps.
const SENDER_FUNDS: U256 = uint!(1_000_000_000_000_000_000_000_000_000_U256);
/// The input of each swap: 1e18, one token.
const SWAP_IN: U256 = uint!(1_000_000_000_000_000_000_U256);

/// The swap share of the uniswap workload: the percentage of swaps among
/// the transactions of each sender, 0 through 100. The other
/// transactions are plain transfers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapShare(u8);

impl SwapShare {
    /// # Errors
    ///
    /// Returns an error if `pct` is above 100.
    pub fn new(pct: u8) -> anyhow::Result<Self> {
        anyhow::ensure!(pct <= 100, "the swap share {pct} is above 100");
        Ok(Self(pct))
    }

    /// The swaps among the places before `place` of the cycle of all
    /// senders. Place `p` holds a swap when this count grows from `p` to
    /// `p + 1`, so each 100 places in a row hold exactly the share. A
    /// place is below `2^65`, so the product stays far under `u128::MAX`.
    fn swaps_before(self, place: u128) -> u128 {
        place * u128::from(self.0) / 100
    }
}

impl std::str::FromStr for SwapShare {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(
            s.parse()
                .context("the swap share is not a number 0 to 100")?,
        )
    }
}

/// The shape of the uniswap workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapMix {
    pub swap_share: SwapShare,
    /// The number of pairs. Each sender swaps on its home pair: sender
    /// `s` uses pair `s % pairs`.
    pub pairs: NonZeroUsize,
}

impl Default for UniswapMix {
    /// Half swaps, half transfers, on four pairs.
    fn default() -> Self {
        Self {
            swap_share: SwapShare(50),
            pairs: NonZeroUsize::new(4).expect("4 is not zero"),
        }
    }
}

/// The reserves of a pair, seen from the input side of one swap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Reserves {
    pub(crate) input: U256,
    pub(crate) output: U256,
}

impl Reserves {
    /// The output of a swap of `amount_in`, with the integer math of
    /// `UniswapV2Library.getAmountOut`: a fee of 0.3%, rounded down.
    /// `U256` arithmetic wraps on overflow. Each caller keeps the
    /// reserves and the input below `2^112`, the `uint112` bound of the
    /// pair, so each product stays below `2^235`.
    #[must_use]
    pub(crate) fn amount_out(self, amount_in: U256) -> U256 {
        let with_fee = amount_in * U256::from(997u64);
        with_fee * self.output / (self.input * U256::from(1000u64) + with_fee)
    }
}

/// One pair: its address, and its tokens in the order the pair sorts
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniswapPair {
    pub address: Address,
    pub token0: Address,
    pub token1: Address,
}

impl UniswapPair {
    /// The pair that `factory.createPair(a, b)` creates. The CREATE2
    /// salt is `keccak(token0 ++ token1)`. `init_hash` is the keccak of
    /// the pair creation code.
    #[must_use]
    pub fn new(factory: Address, init_hash: B256, a: Address, b: Address) -> Self {
        let (token0, token1) = if a < b { (a, b) } else { (b, a) };
        let salt = keccak256([token0.as_slice(), token1.as_slice()].concat());
        Self {
            address: factory.create2(salt, init_hash),
            token0,
            token1,
        }
    }

    /// The router path of a swap from `token0` to `token1`, or back.
    fn path(&self, zero_for_one: bool) -> Vec<Address> {
        if zero_for_one {
            vec![self.token0, self.token1]
        } else {
            vec![self.token1, self.token0]
        }
    }
}

/// A `MintERC20` token.
#[derive(Debug, Clone, Copy)]
struct Token(Address);

impl Token {
    /// Mint `amount` of this token to `to`.
    fn mint(self, to: Address, amount: U256) -> TxShape {
        let mint = IMintErc20::mintCall { to, amount };
        TxShape::call(
            self.0,
            mint.abi_encode().into(),
            SETUP_CALL_GAS,
            TxClass::Call,
        )
    }

    /// Let `spender` move any amount of this token from the signer.
    fn approve(self, spender: Address) -> TxShape {
        let approve = IMintErc20::approveCall {
            spender,
            amount: U256::MAX,
        };
        TxShape::call(
            self.0,
            approve.abi_encode().into(),
            SETUP_CALL_GAS,
            TxClass::Call,
        )
    }
}

/// The addresses of the contracts the setup creates.
#[derive(Debug, Clone)]
pub struct UniswapContracts {
    /// Two tokens for each pair: pair `p` trades tokens `2p` and `2p + 1`.
    pub tokens: Vec<Address>,
    pub factory: Address,
    pub router: Address,
    pub pairs: Vec<UniswapPair>,
}

impl UniswapContracts {
    /// The addresses when `deployer` signs the setup from nonce `first`:
    /// the tokens at `first` through `first + 2 * pairs - 1`, then the
    /// factory, then the router. The factory creates each pair with
    /// CREATE2.
    ///
    /// # Errors
    ///
    /// Returns an error if a nonce overflows `u64`, or if the embedded
    /// pair code is not hex.
    pub fn at(deployer: Address, first: u64, pairs: NonZeroUsize) -> anyhow::Result<Self> {
        let token_count = u64::try_from(pairs.get())
            .ok()
            .and_then(|p| p.checked_mul(2))
            .context("the token count overflows u64")?;
        let create = |offset: u64| {
            first
                .checked_add(offset)
                .map(|nonce| deployer.create(nonce))
                .with_context(|| format!("nonce {first} + {offset} overflows u64"))
        };
        let tokens = (0..token_count)
            .map(create)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let factory = create(token_count)?;
        let router = create(token_count.checked_add(1).context("router nonce")?)?;
        let init_hash = keccak256(hex::decode(PAIR_CREATION_HEX)?);
        let pairs = tokens
            .as_chunks::<2>()
            .0
            .iter()
            .map(|[a, b]| UniswapPair::new(factory, init_hash, *a, *b))
            .collect();
        Ok(Self {
            tokens,
            factory,
            router,
            pairs,
        })
    }

    /// The setup of the deployer: create the tokens, the factory, and the
    /// router, then create each pair and add its liquidity.
    fn deploy_shapes(&self, deployer: Address) -> anyhow::Result<Vec<TxShape>> {
        let token = TxShape::create(hex::decode(TOKEN_CREATION_HEX)?.into(), CREATE_GAS);
        let factory_init = [hex::decode(FACTORY_CREATION_HEX)?, deployer.abi_encode()].concat();
        // The router reads WETH only in its ETH swap and ETH liquidity
        // functions. The workload calls none of them, so the first token
        // stands in for WETH, and the setup creates no WETH contract.
        let router_init = [
            hex::decode(ROUTER_CREATION_HEX)?,
            (self.factory, self.tokens[0]).abi_encode_params(),
        ]
        .concat();
        let tokens = std::iter::repeat_n(token, self.tokens.len());
        let cores = [
            TxShape::create(factory_init.into(), CREATE_GAS),
            TxShape::create(router_init.into(), CREATE_GAS),
        ];
        let pairs = self
            .pairs
            .iter()
            .flat_map(|pair| self.pair_shapes(pair, deployer));
        Ok(tokens.chain(cores).chain(pairs).collect())
    }

    /// Create `pair`, mint the liquidity of both tokens to it, and mint
    /// its LP tokens to `deployer`.
    fn pair_shapes(&self, pair: &UniswapPair, deployer: Address) -> [TxShape; 4] {
        let create = IUniswapV2Factory::createPairCall {
            tokenA: pair.token0,
            tokenB: pair.token1,
        };
        let lp = IUniswapV2Pair::mintCall { to: deployer };
        [
            TxShape::call(
                self.factory,
                create.abi_encode().into(),
                CREATE_PAIR_GAS,
                TxClass::Call,
            ),
            Token(pair.token0).mint(pair.address, PAIR_LIQUIDITY),
            Token(pair.token1).mint(pair.address, PAIR_LIQUIDITY),
            TxShape::call(
                pair.address,
                lp.abi_encode().into(),
                SETUP_CALL_GAS,
                TxClass::Call,
            ),
        ]
    }

    /// The setup of one sender `me` on its home `pair`: mint both tokens
    /// to itself, and approve the router for both.
    fn sender_shapes(&self, pair: &UniswapPair, me: Address) -> [TxShape; 4] {
        let (token0, token1) = (Token(pair.token0), Token(pair.token1));
        [
            token0.mint(me, SENDER_FUNDS),
            token1.mint(me, SENDER_FUNDS),
            token0.approve(self.router),
            token1.approve(self.router),
        ]
    }

    /// One router swap of `SWAP_IN` on `pair` by `me`, with the minimum
    /// output `min_out`.
    fn swap_shape(
        &self,
        pair: &UniswapPair,
        me: Address,
        min_out: U256,
        zero_for_one: bool,
    ) -> TxShape {
        let swap = IUniswapV2Router02::swapExactTokensForTokensCall {
            amountIn: SWAP_IN,
            amountOutMin: min_out,
            path: pair.path(zero_for_one),
            to: me,
            // The router fails a swap after its deadline. A pre-signed
            // swap has no time limit, so the deadline is the maximum.
            deadline: U256::MAX,
        };
        TxShape::call(
            self.router,
            swap.abi_encode().into(),
            SWAP_GAS,
            TxClass::Swap,
        )
    }
}

/// The signed transactions of one uniswap run.
pub struct UniswapRun {
    /// The setup of the deployer, the first sender. It lands first.
    pub deploys: Vec<PlannedTx>,
    /// The setup of each sender. It lands after `deploys`.
    pub sender_setup: Vec<Vec<PlannedTx>>,
    /// The load of each sender.
    pub queues: Vec<Vec<PlannedTx>>,
}

/// The uniswap workload of one run.
pub struct UniswapPlan<'a> {
    signers: &'a SignerSet,
    /// The first nonce of each sender. The deployer is sender 0.
    starts: &'a [u64],
    /// The chain ID and the gas price. The plan reads the nonces from
    /// `starts`, not from `params.nonce_start`.
    params: TxPlanParams,
    mix: UniswapMix,
    contracts: UniswapContracts,
}

impl<'a> UniswapPlan<'a> {
    /// The plan for `signers`, which start at the nonces `starts`.
    ///
    /// # Errors
    ///
    /// Returns an error if a contract address cannot be computed.
    pub fn new(
        signers: &'a SignerSet,
        starts: &'a [u64],
        params: TxPlanParams,
        mix: UniswapMix,
    ) -> anyhow::Result<Self> {
        let contracts = UniswapContracts::at(signers.deployer().address, starts[0], mix.pairs)?;
        Ok(Self {
            signers,
            starts,
            params,
            mix,
            contracts,
        })
    }

    #[must_use]
    pub fn contracts(&self) -> &UniswapContracts {
        &self.contracts
    }

    /// Sign the setup, and `per_sender` load transactions for each
    /// sender. `transfer` is the shape of the plain transfers.
    ///
    /// # Errors
    ///
    /// Returns an error if a nonce overflows or signing fails.
    pub fn sign(&self, transfer: &TxShape, per_sender: usize) -> anyhow::Result<UniswapRun> {
        let deployer = self.signers.deployer();
        let shapes = self.contracts.deploy_shapes(deployer.address)?;
        let deploys = SenderNonces {
            signer: deployer,
            sender: 0,
            first: self.starts[0],
        }
        .sign_all(&shapes, self.params)?;
        let after_deploys = u64::try_from(shapes.len())?;
        let load = LoadShape {
            transfer,
            min_out: self.min_out(per_sender),
            per_sender,
        };
        let (sender_setup, queues) = self
            .signers
            .iter()
            .zip(self.starts)
            .enumerate()
            .map(|(sender, (signer, &start))| {
                // Sender 0 signs its own setup after the deploys.
                let skip = if sender == 0 { after_deploys } else { 0 };
                let first = start
                    .checked_add(skip)
                    .context("the first setup nonce of sender 0 overflows u64")?;
                self.sign_sender(
                    &load,
                    SenderNonces {
                        signer,
                        sender,
                        first,
                    },
                )
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .unzip();
        tracing::info!(
            factory = %self.contracts.factory,
            router = %self.contracts.router,
            pairs = self.contracts.pairs.len(),
            min_out = %load.min_out,
            "uniswap workload: signed the setup and the load"
        );
        Ok(UniswapRun {
            deploys,
            sender_setup,
            queues,
        })
    }

    /// The minimum output of each swap: the output after each planned
    /// swap of the run went in the same direction on the same pair
    /// before it. Each swap keeps `reserve0 * reserve1` at or above
    /// its value before the swap, so the output reserve is at least
    /// `liquidity^2 / input reserve`. The input reserve is at most the
    /// liquidity plus the input of each planned swap.
    fn min_out(&self, per_sender: usize) -> U256 {
        let share = self.mix.swap_share;
        let swaps: u128 = (0..self.signers.len() as u128)
            .map(|s| share.swaps_before(s + per_sender as u128) - share.swaps_before(s))
            .sum();
        let input = PAIR_LIQUIDITY + SWAP_IN * U256::from(swaps);
        Reserves {
            input,
            output: PAIR_LIQUIDITY * PAIR_LIQUIDITY / input,
        }
        .amount_out(SWAP_IN)
    }

    /// The setup and the load of one sender, which signs its setup from
    /// `nonces.first` and its load after that.
    fn sign_sender(
        &self,
        load: &LoadShape<'_>,
        nonces: SenderNonces<'_>,
    ) -> anyhow::Result<(Vec<PlannedTx>, Vec<PlannedTx>)> {
        let me = nonces.signer.address;
        let pair = &self.contracts.pairs[nonces.sender % self.mix.pairs];
        let setup = nonces.sign_all(self.contracts.sender_shapes(pair, me), self.params)?;
        let shapes = SenderShapes {
            share: self.mix.swap_share,
            offset: nonces.sender as u128,
            transfer: load.transfer,
            swaps: [true, false].map(|dir| self.contracts.swap_shape(pair, me, load.min_out, dir)),
        };
        let first = u64::try_from(setup.len())
            .ok()
            .and_then(|n| nonces.first.checked_add(n))
            .context("the first load nonce overflows u64")?;
        let queue = SenderNonces { first, ..nonces }
            .sign_all((0..load.per_sender).map(|i| shapes.at(i)), self.params)?;
        Ok((setup, queue))
    }
}

/// What each sender's load shares: the transfer, the minimum output of a
/// swap, and the queue length.
struct LoadShape<'a> {
    transfer: &'a TxShape,
    min_out: U256,
    per_sender: usize,
}

/// The three transactions one sender signs in its load: a transfer, and
/// a swap in each direction of its home pair.
struct SenderShapes<'a> {
    share: SwapShare,
    /// The place of the first load transaction of this sender in the
    /// swap cycle: the sender index. The senders start at other places,
    /// so their swaps do not line up.
    offset: u128,
    transfer: &'a TxShape,
    swaps: [TxShape; 2],
}

impl SenderShapes<'_> {
    /// The shape of load transaction `i`. The swaps of the sender
    /// alternate their direction.
    fn at(&self, i: usize) -> &TxShape {
        let place = self.offset + i as u128;
        let before = self.share.swaps_before(place);
        if self.share.swaps_before(place + 1) == before {
            return self.transfer;
        }
        let own = before - self.share.swaps_before(self.offset);
        &self.swaps[usize::from(own % 2 == 1)]
    }
}

#[cfg(test)]
mod tests;
