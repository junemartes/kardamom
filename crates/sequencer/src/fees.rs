//! Priority fees at the sequencer: the `[fees]` setting, the base fee
//! view, and the admission gate.
//!
//! With the setting on, every observed envelope passes Ethereum's three
//! fee checks before the nonce state machine parks or releases it:
//!
//! 1. the tip rate is within the fee cap (`FeeInvalid` otherwise);
//! 2. the fee cap covers the base fee of the latest block this replica
//!    has seen (`FeeTooLow` otherwise);
//! 3. the sender's balance, as the local account layer sees it, covers
//!    the worst case the transaction can be charged (`InsufficientFunds`
//!    otherwise). A sender the layer does not hold passes: the ingress
//!    checked a fresh balance, and the executor is the truth.
//!
//! An admitted envelope gets its bid, the tip as an amount in wei, which
//! rides the sealer's guard header. With the setting off every bid is
//! zero, so the sealer's order equals arrival order: the setting changes
//! the data, not the algorithm.
//!
//! The base fee comes from the executor's block boundaries. The view may
//! lag a few blocks, which errs by a few eighths; a transaction admitted
//! on a stale view that cannot pay at its block becomes a skip receipt at
//! execution, as any unpayable transaction does.

use std::sync::Arc;

use alloy_primitives::{Address, U256};
use arc_swap::ArcSwap;
use kardamom_cache::LiveAccounts;
use kardamom_types::{BlockBoundary, TxErrorReason, TxFees, fees::BaseFeeSchedule};
use serde::{Deserialize, Serialize};

/// The `[fees]` config section.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct FeesConfig {
    /// Whether the tip rides the offer and the three fee checks run.
    /// Every replica of a chain runs the same value, and the deploy sets
    /// it together with the sealer's ordering window.
    pub priority: bool,
}

/// The base fee of the next block, as the latest executor boundary
/// implies it. The boundary feed writes it; the publish loop reads it on
/// every envelope. One `Arc` swap per block and one load per envelope,
/// which keeps the hot path free of a lock.
#[derive(Debug, Default)]
pub struct LatestBaseFee {
    next: ArcSwap<u128>,
}

impl LatestBaseFee {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record the boundary of a closed block. The next block's base fee
    /// follows from the closed block's base fee and gas used.
    pub fn on_boundary(&self, boundary: &BlockBoundary) {
        let next = BaseFeeSchedule::CHAIN.next_base_fee(boundary.base_fee, boundary.gas_used);
        self.next.store(Arc::new(next));
    }

    /// The base fee the next block charges, as far as this replica knows.
    /// Zero before the first boundary, so a cold replica admits every cap.
    #[must_use]
    pub fn get(&self) -> u128 {
        **self.next.load()
    }
}

/// The decoded fee fields of one envelope, as the gate checks them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeFields {
    pub fees: TxFees,
    pub value: U256,
}

/// The admission gate. Off, it admits everything with a zero bid.
pub struct FeeGate {
    enabled: bool,
    base_fee: Arc<LatestBaseFee>,
    /// The local account layer, when the binary wired one. `None` admits
    /// every balance.
    balances: Option<Arc<LiveAccounts>>,
}

impl FeeGate {
    /// The gate with the setting off: every envelope passes with a zero
    /// bid, and no base fee or balance is read.
    #[must_use]
    pub fn off() -> Self {
        Self {
            enabled: false,
            base_fee: LatestBaseFee::new(),
            balances: None,
        }
    }

    /// The gate with the setting on, over the base fee view the boundary
    /// feed writes and the account layer the receipts feed writes.
    #[must_use]
    pub fn on(base_fee: Arc<LatestBaseFee>, balances: Option<Arc<LiveAccounts>>) -> Self {
        Self {
            enabled: true,
            base_fee,
            balances,
        }
    }

    #[must_use]
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Run the three checks on one envelope. `Ok` carries the bid.
    ///
    /// # Errors
    ///
    /// Returns the `tx_errors` reason the client gets: `FeeInvalid`,
    /// `FeeTooLow`, or `InsufficientFunds`.
    pub fn admit(&self, sender: Address, fields: &FeeFields) -> Result<u128, TxErrorReason> {
        if !self.enabled {
            return Ok(0);
        }
        let fees = &fields.fees;
        let invalid = TxErrorReason::FeeInvalid {
            max_fee_per_gas: fees.max_fee_per_gas,
            max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
        };
        if !fees.valid() {
            return Err(invalid);
        }
        let base_fee = self.base_fee.get();
        if fees.max_fee_per_gas < base_fee {
            return Err(TxErrorReason::FeeTooLow {
                max_fee_per_gas: fees.max_fee_per_gas,
                base_fee,
            });
        }
        self.check_balance(sender, fees.worst_case_cost(fields.value))?;
        fees.bid(base_fee).ok_or(invalid)
    }

    /// The balance check against the local account layer. A sender the
    /// layer does not hold passes.
    fn check_balance(&self, sender: Address, want: U256) -> Result<(), TxErrorReason> {
        let Some(view) = self.balances.as_ref().and_then(|b| b.get(sender)) else {
            return Ok(());
        };
        if view.balance >= want {
            return Ok(());
        }
        // The error carries the amounts for the client's message. A
        // balance or a cost above `u128::MAX` wei is beyond any real
        // account, so saturation only shortens an impossible number.
        Err(TxErrorReason::InsufficientFunds {
            have: view.balance.saturating_to(),
            want: want.saturating_to(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU64, NonZeroUsize};

    use kardamom_cache::LiveAccountsConfig;
    use kardamom_types::{AccountRow, BPosition};

    use super::*;

    fn fields(max_fee: u128, max_priority: u128, legacy: bool) -> FeeFields {
        FeeFields {
            fees: TxFees {
                gas_limit: 21_000,
                max_fee_per_gas: max_fee,
                max_priority_fee_per_gas: max_priority,
                legacy,
            },
            value: U256::from(10u64),
        }
    }

    fn boundary(base_fee: u128, gas_used: u64) -> BlockBoundary {
        BlockBoundary {
            base_fee,
            gas_used,
            ..BlockBoundary::default()
        }
    }

    #[test]
    fn off_admits_everything_with_a_zero_bid() {
        let gate = FeeGate::off();
        assert_eq!(gate.admit(Address::ZERO, &fields(1, 5, false)), Ok(0));
    }

    #[test]
    fn the_tip_rate_must_be_within_the_cap() {
        let gate = FeeGate::on(LatestBaseFee::new(), None);
        assert_eq!(
            gate.admit(Address::ZERO, &fields(1, 5, false)),
            Err(TxErrorReason::FeeInvalid {
                max_fee_per_gas: 1,
                max_priority_fee_per_gas: 5,
            })
        );
    }

    #[test]
    fn the_cap_must_cover_the_latest_base_fee() {
        let base = LatestBaseFee::new();
        // A full block at base fee 800 makes the next block's 900.
        base.on_boundary(&boundary(800, 30_000_000));
        let gate = FeeGate::on(base, None);
        assert_eq!(
            gate.admit(Address::ZERO, &fields(899, 0, false)),
            Err(TxErrorReason::FeeTooLow {
                max_fee_per_gas: 899,
                base_fee: 900,
            })
        );
        // At the base fee, a zero tip is admitted with a zero bid: there
        // is no floor.
        assert_eq!(gate.admit(Address::ZERO, &fields(900, 0, false)), Ok(0));
        assert_eq!(
            gate.admit(Address::ZERO, &fields(1_000, 7, false)),
            Ok(7 * 21_000)
        );
        // A legacy price bids its part above the base fee.
        assert_eq!(
            gate.admit(Address::ZERO, &fields(1_000, 1_000, true)),
            Ok(100 * 21_000)
        );
    }

    #[test]
    fn the_balance_must_cover_the_worst_case() {
        let cfg = LiveAccountsConfig {
            capacity: NonZeroUsize::new(16).unwrap(),
            ttl_ms: NonZeroU64::new(60_000).unwrap(),
        };
        let (live, mut writer) = LiveAccounts::new(&cfg);
        let poor = Address::repeat_byte(0x01);
        let rich = Address::repeat_byte(0x02);
        writer.apply(
            BPosition::from_index(1),
            &[
                AccountRow {
                    address: poor,
                    nonce: 0,
                    balance: U256::from(21_000u64 * 100 + 9),
                },
                AccountRow {
                    address: rich,
                    nonce: 0,
                    balance: U256::from(21_000u64 * 100 + 10),
                },
            ],
        );
        let gate = FeeGate::on(LatestBaseFee::new(), Some(live));
        assert_eq!(
            gate.admit(poor, &fields(100, 1, false)),
            Err(TxErrorReason::InsufficientFunds {
                have: 21_000 * 100 + 9,
                want: 21_000 * 100 + 10,
            })
        );
        assert_eq!(gate.admit(rich, &fields(100, 1, false)), Ok(21_000));
        // An unknown sender passes: the executor is the truth.
        assert_eq!(
            gate.admit(Address::repeat_byte(0x03), &fields(100, 1, false)),
            Ok(21_000)
        );
    }
}
