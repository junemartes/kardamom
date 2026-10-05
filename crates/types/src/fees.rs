//! Fee arithmetic shared by every role.
//!
//! Three things live here, and every role uses the same copy:
//!
//! - the EIP-1559 base fee schedule ([`BaseFeeSchedule`]): the executor
//!   computes each block's base fee from the previous block, the validator
//!   and the rebuild tool recompute it, the ingress reports it, and the
//!   sequencer checks offers against it;
//! - the chain values of the schedule ([`FeeSchedule`]): the first base fee
//!   and the beneficiary, read from the genesis;
//! - the fee fields of one transaction ([`TxFees`]): the bid the sequencer
//!   orders by, the worst-case cost the admission checks compare with the
//!   balance, and the tip rate the executor charges.
//!
//! The ordering key is the tip as an amount in wei, `rate * gas_limit`,
//! not a rate per gas: the sealer ranks what a sender pays for its place.
//! The executor charges that amount in full at inclusion, whether or not
//! the gas is used.

use core::num::NonZeroU128;

use alloy_primitives::{Address, U256};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use serde::Deserialize;

use crate::wire;

use crate::limits::BLOCK_GAS_LIMIT;

/// The base fee moves by at most one part in this many per block.
pub const BASE_FEE_MAX_CHANGE_DENOMINATOR: u64 = 8;

/// The gas target is the block gas limit divided by this.
pub const ELASTICITY_MULTIPLIER: u64 = 2;

/// The base fee schedule parameters: the gas target a block is measured
/// against, and the change denominator. The chain runs [`Self::CHAIN`].
/// The parameters are explicit so the Ethereum vectors, which use a
/// different target, run through the same function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BaseFeeSchedule {
    pub gas_target: u64,
    pub denominator: u64,
}

impl BaseFeeSchedule {
    /// The chain's schedule: the target is half the block gas limit, the
    /// denominator is eight.
    pub const CHAIN: Self = Self {
        gas_target: BLOCK_GAS_LIMIT / ELASTICITY_MULTIPLIER,
        denominator: BASE_FEE_MAX_CHANGE_DENOMINATOR,
    };

    /// The base fee of the block after one with `base_fee` and `gas_used`,
    /// as EIP-1559 defines it. The result is at most nine eighths of
    /// `base_fee`, so the `U256` intermediate always fits back in a
    /// `u128`; `saturating_to` states that bound.
    #[must_use]
    pub fn next_base_fee(self, base_fee: u128, gas_used: u64) -> u128 {
        let base = U256::from(base_fee);
        let target = U256::from(self.gas_target);
        let denominator = U256::from(self.denominator);
        if gas_used == self.gas_target {
            return base_fee;
        }
        if gas_used > self.gas_target {
            let over = U256::from(gas_used - self.gas_target);
            let delta = (base * over / target / denominator).max(U256::from(1u8));
            return (base + delta).saturating_to();
        }
        let under = U256::from(self.gas_target - gas_used);
        let delta = base * under / target / denominator;
        (base - delta).saturating_to()
    }
}

/// The chain values of the fee schedule, from the genesis `[fees]`
/// section. A genesis without the section runs no schedule: the base fee
/// is zero, and the tip of every transaction burns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeSchedule {
    /// The base fee of the first block, in wei per gas. Never zero: the
    /// schedule only lowers a base fee by one eighth, so a base fee of
    /// one wei is its floor, and a zero base fee means no schedule.
    pub base_fee_initial: NonZeroU128,
    /// The account that collects every tip.
    pub beneficiary: Address,
}

/// The fee values one block executes under: its base fee, and where its
/// tips go. [`Self::NONE`] is the no-schedule chain. A scheduled base fee
/// is never zero (see [`FeeSchedule::base_fee_initial`]), so a zero base
/// fee is the one sign of no schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Archive, RkyvSerialize, RkyvDeserialize)]
#[rkyv(derive(Debug))]
pub struct BlockFees {
    pub base_fee: u128,
    #[rkyv(with = wire::AddressBytes)]
    pub beneficiary: Address,
}

impl BlockFees {
    /// No schedule: a zero base fee, and tips burn at the zero address.
    pub const NONE: Self = Self {
        base_fee: 0,
        beneficiary: Address::ZERO,
    };

    /// The fees of the first block of a chain with `schedule`.
    #[must_use]
    pub fn genesis(schedule: Option<FeeSchedule>) -> Self {
        schedule.map_or(Self::NONE, |s| Self {
            base_fee: s.base_fee_initial.get(),
            beneficiary: s.beneficiary,
        })
    }

    /// The fees of the next block, after this block used `gas_used`. With
    /// no schedule the base fee stays zero and the beneficiary stays the
    /// zero address, so a chain without the section never moves.
    #[must_use]
    pub fn next(self, gas_used: u64) -> Self {
        if !self.scheduled() {
            return self;
        }
        Self {
            base_fee: BaseFeeSchedule::CHAIN.next_base_fee(self.base_fee, gas_used),
            beneficiary: self.beneficiary,
        }
    }

    /// Whether a schedule runs: the tip-in-full settlement applies, and
    /// the receipt reports the base fee as the gas price.
    #[must_use]
    pub fn scheduled(self) -> bool {
        self.base_fee > 0
    }
}

/// The fee fields of one transaction. A legacy transaction carries one
/// price, which Ethereum reads as both the cap and the tip rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxFees {
    pub gas_limit: u64,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    /// A legacy or 2930 transaction: its bid is its price above the base
    /// fee, not a committed tip rate.
    pub legacy: bool,
}

impl TxFees {
    /// Whether the tip rate is within the cap. A bid above the cap can
    /// never be paid.
    #[must_use]
    pub fn valid(&self) -> bool {
        self.max_priority_fee_per_gas <= self.max_fee_per_gas
    }

    /// The tip rate the chain collects at `base_fee`: the committed rate,
    /// bounded by what the cap leaves above the base fee. `None` when the
    /// cap is under the base fee, so the transaction cannot be included
    /// at that price.
    #[must_use]
    pub fn effective_tip_rate(&self, base_fee: u128) -> Option<u128> {
        let room = self.max_fee_per_gas.checked_sub(base_fee)?;
        Some(self.max_priority_fee_per_gas.min(room))
    }

    /// The tip the sender bids for its place, as an amount: the committed
    /// tip rate times the gas limit. A legacy transaction bids its price
    /// above `base_fee`, clamped at zero, since it carries no tip rate.
    /// `None` when the amount overflows `u128`.
    #[must_use]
    pub fn bid(&self, base_fee: u128) -> Option<u128> {
        let rate = if self.legacy {
            self.max_fee_per_gas.saturating_sub(base_fee)
        } else {
            self.max_priority_fee_per_gas
        };
        rate.checked_mul(u128::from(self.gas_limit))
    }

    /// The tip the sender pays at inclusion: the effective tip rate times
    /// the gas limit, unused gas included. `None` when the cap is under
    /// `base_fee`. The product fits: a rate under `u128::MAX` times a gas
    /// limit under `2^24` is under `2^152`, and `U256` holds it.
    #[must_use]
    pub fn tip_amount(&self, base_fee: u128) -> Option<U256> {
        let rate = self.effective_tip_rate(base_fee)?;
        Some(U256::from(rate) * U256::from(self.gas_limit))
    }

    /// The most the sender can be charged: the cap on every gas unit, plus
    /// the value. The tip rate is under the cap, and the tip amount is that
    /// rate times the gas limit, so this bounds the base fee charge and
    /// the full tip together.
    #[must_use]
    pub fn worst_case_cost(&self, value: U256) -> U256 {
        U256::from(self.max_fee_per_gas) * U256::from(self.gas_limit) + value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The go-ethereum `TestCalcBaseFee` vectors: a 20M gas limit (a 10M
    /// target), with the parent at, under, and over the target.
    #[test]
    fn ethereum_base_fee_vectors() {
        let geth = BaseFeeSchedule {
            gas_target: 10_000_000,
            denominator: 8,
        };
        let cases = [
            (1_000_000_000u128, 10_000_000u64, 1_000_000_000u128),
            (1_000_000_000, 9_000_000, 987_500_000),
            (1_000_000_000, 11_000_000, 1_012_500_000),
        ];
        for (parent_base_fee, parent_gas_used, want) in cases {
            assert_eq!(
                geth.next_base_fee(parent_base_fee, parent_gas_used),
                want,
                "parent base fee {parent_base_fee}, gas used {parent_gas_used}"
            );
        }
    }

    /// EIP-1559 bounds: a full block raises by one eighth, an empty block
    /// lowers by one eighth, a block over target raises by at least one
    /// wei, and zero stays zero on an under-target block.
    #[test]
    fn chain_schedule_bounds() {
        let s = BaseFeeSchedule::CHAIN;
        assert_eq!(s.gas_target, 15_000_000);
        assert_eq!(s.next_base_fee(800, BLOCK_GAS_LIMIT), 900);
        assert_eq!(s.next_base_fee(800, 0), 700);
        assert_eq!(s.next_base_fee(0, 15_000_001), 1);
        assert_eq!(s.next_base_fee(0, 0), 0);
        assert_eq!(s.next_base_fee(7, 15_000_000), 7);
        // A one-wei change rounds to zero on the way down.
        assert_eq!(s.next_base_fee(7, 0), 7);
    }

    #[test]
    fn block_fees_without_a_schedule_never_move() {
        assert_eq!(BlockFees::genesis(None), BlockFees::NONE);
        assert_eq!(BlockFees::NONE.next(BLOCK_GAS_LIMIT), BlockFees::NONE);
        assert!(!BlockFees::NONE.scheduled());
        let scheduled = BlockFees::genesis(Some(FeeSchedule {
            base_fee_initial: NonZeroU128::new(800).unwrap(),
            beneficiary: Address::repeat_byte(0xfe),
        }));
        assert!(scheduled.scheduled());
        assert_eq!(scheduled.next(BLOCK_GAS_LIMIT).base_fee, 900);
        assert_eq!(scheduled.next(0).beneficiary, Address::repeat_byte(0xfe));
    }

    fn typed(gas_limit: u64, max_fee: u128, max_priority: u128) -> TxFees {
        TxFees {
            gas_limit,
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: max_priority,
            legacy: false,
        }
    }

    #[test]
    fn typed_bid_is_the_committed_rate_times_the_gas_limit() {
        let fees = typed(21_000, 100, 7);
        assert_eq!(fees.bid(50), Some(147_000));
        // The bid does not move with the base fee.
        assert_eq!(fees.bid(99), Some(147_000));
        assert!(fees.valid());
        assert!(!typed(21_000, 5, 7).valid());
        assert_eq!(typed(1 << 24, u128::MAX, u128::MAX).bid(0), None);
    }

    #[test]
    fn legacy_bid_is_the_price_above_the_base_fee() {
        let fees = TxFees {
            gas_limit: 21_000,
            max_fee_per_gas: 100,
            max_priority_fee_per_gas: 100,
            legacy: true,
        };
        assert_eq!(fees.bid(40), Some(60 * 21_000));
        assert_eq!(fees.bid(100), Some(0));
        assert_eq!(fees.bid(140), Some(0));
    }

    #[test]
    fn effective_tip_is_bounded_by_the_cap() {
        let fees = typed(21_000, 100, 30);
        assert_eq!(fees.effective_tip_rate(50), Some(30));
        assert_eq!(fees.effective_tip_rate(80), Some(20));
        assert_eq!(fees.effective_tip_rate(100), Some(0));
        assert_eq!(fees.effective_tip_rate(101), None);
        assert_eq!(fees.tip_amount(80), Some(U256::from(20u64 * 21_000)));
        assert_eq!(
            fees.worst_case_cost(U256::from(5u64)),
            U256::from(100u64 * 21_000 + 5)
        );
    }
}
