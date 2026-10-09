//! The canary's contracts on the wire: creation inputs, call inputs, and
//! the events the probes check, encoded by hand for the few shapes they
//! use. The formulas of the pool live here too, in checked arithmetic, so
//! a probe compares what the EVM computed with what the formula gives.

use alloy_primitives::{Address, B256, Bytes, U256, keccak256};

use crate::outcome::Outcome;
use crate::rpc::{Log, Receipt};

/// The gas limit of a deploy.
pub const DEPLOY_GAS: u64 = 3_000_000;
/// The gas limit of a counter write: a cold slot and an event, with room.
pub const INCREMENT_GAS: u64 = 100_000;
/// The gas limit of a token or pool call.
pub const CALL_GAS: u64 = 250_000;

/// One ABI word of an address.
#[must_use]
pub fn address_word(address: Address) -> U256 {
    U256::from_be_slice(address.as_slice())
}

/// The input of a call to `signature` with static `args`.
#[must_use]
pub fn call(signature: &str, args: &[U256]) -> Bytes {
    let input: Vec<u8> = keccak256(signature)[..4]
        .iter()
        .copied()
        .chain(args.iter().flat_map(U256::to_be_bytes::<32>))
        .collect();
    Bytes::from(input)
}

/// A creation input: `code`, then the ABI words of the arguments.
fn creation(code: &Bytes, words: impl Iterator<Item = U256>) -> Bytes {
    let input: Vec<u8> = code
        .iter()
        .copied()
        .chain(words.flat_map(|w| w.to_be_bytes::<32>()))
        .collect();
    Bytes::from(input)
}

/// The words of a dynamic `address[]` that is the only argument.
fn address_array(addresses: &[Address]) -> impl Iterator<Item = U256> + '_ {
    [U256::from(32), U256::from(addresses.len())]
        .into_iter()
        .chain(addresses.iter().copied().map(address_word))
}

/// The data words of the first log of `address` with event `signature`.
fn event_words(receipt: &Receipt, address: Address, signature: &str) -> Option<Vec<U256>> {
    let topic: B256 = keccak256(signature);
    receipt
        .logs
        .iter()
        .find(|log| log.address == address && log.topics.first() == Some(&topic))
        .map(log_words)
}

/// The data words of `log`.
fn log_words(log: &Log) -> Vec<U256> {
    let (words, _) = log.data.as_chunks::<32>();
    words.iter().map(|w| U256::from_be_bytes(*w)).collect()
}

/// The counter of the `contract` probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Counter(pub Address);

impl Counter {
    /// The creation input: the bytecode and the `address[] writers`.
    #[must_use]
    pub fn creation(writers: &[Address]) -> Bytes {
        creation(
            &kardamom_deployer::embedded::canary_counter_creation(),
            address_array(writers),
        )
    }

    /// The input of `increment()`.
    #[must_use]
    pub fn increment() -> Bytes {
        call("increment()", &[])
    }

    /// The count an `Incremented` event of this counter in `receipt`
    /// reports.
    #[must_use]
    pub fn count_in(self, receipt: &Receipt) -> Option<U256> {
        event_words(receipt, self.0, "Incremented(address,uint256)")
            .and_then(|w| w.first().copied())
    }
}

/// The test RWA token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rwa(pub Address);

impl Rwa {
    /// The creation input: the bytecode and the initial allowlist.
    #[must_use]
    pub fn creation(allowed: &[Address]) -> Bytes {
        creation(
            &kardamom_deployer::embedded::canary_rwa_creation(),
            address_array(allowed),
        )
    }

    #[must_use]
    pub fn mint(to: Address, amount: U256) -> Bytes {
        call("mint(address,uint256)", &[address_word(to), amount])
    }

    #[must_use]
    pub fn burn(from: Address, amount: U256) -> Bytes {
        call("burn(address,uint256)", &[address_word(from), amount])
    }

    #[must_use]
    pub fn transfer(to: Address, amount: U256) -> Bytes {
        call("transfer(address,uint256)", &[address_word(to), amount])
    }

    #[must_use]
    pub fn approve(spender: Address) -> Bytes {
        call(
            "approve(address,uint256)",
            &[address_word(spender), U256::MAX],
        )
    }

    #[must_use]
    pub fn set_allowed(account: Address) -> Bytes {
        call(
            "setAllowed(address,bool)",
            &[address_word(account), U256::from(1)],
        )
    }

    /// The total supply a `Supply` event of this token reports.
    #[must_use]
    pub fn supply_in(self, receipt: &Receipt) -> Option<U256> {
        event_words(receipt, self.0, "Supply(uint256)").and_then(|w| w.first().copied())
    }
}

/// The two reserves of the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reserves {
    pub eth: U256,
    pub token: U256,
}

impl Reserves {
    /// The product of the reserves. `None` past 256 bits.
    #[must_use]
    pub fn product(self) -> Option<U256> {
        self.eth.checked_mul(self.token)
    }

    /// The price of one token in wei, scaled by 10^18.
    #[must_use]
    pub fn price(self) -> Option<U256> {
        self.eth
            .checked_mul(U256::from(10).pow(U256::from(18)))?
            .checked_div(self.token)
    }
}

/// One `Swap` event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwapEvent {
    pub eth_in: bool,
    pub amount_in: U256,
    pub amount_out: U256,
    pub before: Reserves,
    pub after: Reserves,
    pub seq: U256,
}

impl SwapEvent {
    /// Compare the swap with the formula from the reserves before it:
    /// the output, the reserves after it, and the product, which must
    /// not fall.
    ///
    /// # Errors
    ///
    /// `swap_mismatch` for a wrong output or reserve, `invariant_broken`
    /// for a product that fell.
    pub fn check(&self) -> Result<(), Outcome> {
        let (r_in, r_out) = self.oriented(self.before);
        let expected =
            Pool::amount_out(self.amount_in, r_in, r_out).ok_or(Outcome::SwapMismatch)?;
        let after_in = r_in
            .checked_add(self.amount_in)
            .ok_or(Outcome::SwapMismatch)?;
        let after_out = r_out
            .checked_sub(self.amount_out)
            .ok_or(Outcome::SwapMismatch)?;
        if expected != self.amount_out || self.oriented(self.after) != (after_in, after_out) {
            return Err(Outcome::SwapMismatch);
        }
        let before = self.before.product().ok_or(Outcome::SwapMismatch)?;
        let after = self.after.product().ok_or(Outcome::SwapMismatch)?;
        if after < before {
            return Err(Outcome::InvariantBroken);
        }
        Ok(())
    }

    /// `(reserve in, reserve out)` of `reserves` for this direction.
    fn oriented(&self, reserves: Reserves) -> (U256, U256) {
        if self.eth_in {
            (reserves.eth, reserves.token)
        } else {
            (reserves.token, reserves.eth)
        }
    }
}

/// One `Liquidity` event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiquidityEvent {
    pub added: bool,
    pub eth: U256,
    pub token: U256,
    pub shares: U256,
    pub before: Reserves,
    pub shares_before: U256,
    pub seq: U256,
}

impl LiquidityEvent {
    /// Compare the shares of an add, or the amounts of a remove, with
    /// the pool's formula from the reserves before it.
    ///
    /// # Errors
    ///
    /// `liquidity_mismatch` when they differ.
    pub fn check(&self) -> Result<(), Outcome> {
        let part = |amount: U256, reserve: U256| {
            amount.checked_mul(self.shares_before)?.checked_div(reserve)
        };
        let share = |reserve: U256| {
            self.shares
                .checked_mul(reserve)?
                .checked_div(self.shares_before)
        };
        let fits = if self.added {
            part(self.eth, self.before.eth)
                .zip(part(self.token, self.before.token))
                .map(|(a, b)| a.min(b))
                == Some(self.shares)
        } else {
            share(self.before.eth) == Some(self.eth) && share(self.before.token) == Some(self.token)
        };
        if fits {
            Ok(())
        } else {
            Err(Outcome::LiquidityMismatch)
        }
    }
}

/// The canary pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pool(pub Address);

impl Pool {
    /// The fee: a swap keeps 997 of each 1000 input units.
    const KEPT: u64 = 997;
    const BASE: u64 = 1000;

    /// The creation input: the bytecode and the token.
    #[must_use]
    pub fn creation(token: Address) -> Bytes {
        creation(
            &kardamom_deployer::embedded::canary_pool_creation(),
            std::iter::once(address_word(token)),
        )
    }

    #[must_use]
    pub fn swap_eth_for_token() -> Bytes {
        call("swapEthForToken(uint256)", &[U256::ZERO])
    }

    #[must_use]
    pub fn swap_token_for_eth(amount_in: U256) -> Bytes {
        call("swapTokenForEth(uint256,uint256)", &[amount_in, U256::ZERO])
    }

    #[must_use]
    pub fn add_liquidity(token_amount: U256) -> Bytes {
        call("addLiquidity(uint256)", &[token_amount])
    }

    #[must_use]
    pub fn remove_liquidity(shares: U256) -> Bytes {
        call("removeLiquidity(uint256)", &[shares])
    }

    /// The output of a swap of `amount_in` against `reserve_in` and
    /// `reserve_out`, after the fee: the pool's `getAmountOut`.
    #[must_use]
    pub fn amount_out(amount_in: U256, reserve_in: U256, reserve_out: U256) -> Option<U256> {
        let kept = amount_in.checked_mul(U256::from(Self::KEPT))?;
        let denominator = reserve_in
            .checked_mul(U256::from(Self::BASE))?
            .checked_add(kept)?;
        kept.checked_mul(reserve_out)?.checked_div(denominator)
    }

    /// The input that buys at least `amount_out` against the reserves.
    #[must_use]
    pub fn amount_in_for(amount_out: U256, reserve_in: U256, reserve_out: U256) -> Option<U256> {
        let numerator = reserve_in
            .checked_mul(amount_out)?
            .checked_mul(U256::from(Self::BASE))?;
        let denominator = reserve_out
            .checked_sub(amount_out)?
            .checked_mul(U256::from(Self::KEPT))?;
        numerator
            .checked_div(denominator)?
            .checked_add(U256::from(1))
    }

    /// The `Swap` event of this pool in `receipt`.
    #[must_use]
    pub fn swap_in(self, receipt: &Receipt) -> Option<SwapEvent> {
        let w = event_words(
            receipt,
            self.0,
            "Swap(address,bool,uint256,uint256,uint256,uint256,uint256,uint256,uint256)",
        )?;
        Some(SwapEvent {
            eth_in: !w.first()?.is_zero(),
            amount_in: *w.get(1)?,
            amount_out: *w.get(2)?,
            before: Reserves {
                eth: *w.get(3)?,
                token: *w.get(4)?,
            },
            after: Reserves {
                eth: *w.get(5)?,
                token: *w.get(6)?,
            },
            seq: *w.get(7)?,
        })
    }

    /// The `Liquidity` event of this pool in `receipt`.
    #[must_use]
    pub fn liquidity_in(self, receipt: &Receipt) -> Option<LiquidityEvent> {
        let w = event_words(
            receipt,
            self.0,
            "Liquidity(address,bool,uint256,uint256,uint256,uint256,uint256,uint256,uint256)",
        )?;
        Some(LiquidityEvent {
            added: !w.first()?.is_zero(),
            eth: *w.get(1)?,
            token: *w.get(2)?,
            shares: *w.get(3)?,
            before: Reserves {
                eth: *w.get(4)?,
                token: *w.get(5)?,
            },
            shares_before: *w.get(6)?,
            seq: *w.get(7)?,
        })
    }
}

#[cfg(test)]
#[path = "contracts_tests.rs"]
mod tests;
