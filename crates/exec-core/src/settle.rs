//! The fee settlement under a schedule: the tip in full to the
//! beneficiary, the base fee on the gas used burned.
//!
//! revm settles as Ethereum does: the sender pays `gas_used * price`,
//! unused gas is refunded at the price, and the beneficiary gets the tip
//! on the gas used. The chain's rule differs in one place: the tip is the
//! price of the place in the order, and the place was given at the bid,
//! so the sender pays `tip_rate * gas_limit` whether or not the gas is
//! used. [`TipSettlement::apply`] moves the difference, the tip on the
//! unused gas, from the sender to the beneficiary after revm's own
//! settlement. The base fee charge, `gas_used * base_fee`, is revm's and
//! stays burned.
//!
//! Without a schedule nothing moves: the base fee is zero, and the tip
//! burns at the zero address, as revm leaves it.

use alloy_primitives::{Address, U256};
use kardamom_types::{BlockFees, TxFees};
use revm::state::EvmState;

use crate::error::ExecutorError;

/// The price fields of one receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiptPrice {
    pub effective_gas_price: u128,
    pub priority_fee_per_gas: u128,
    pub priority_fee_paid: u128,
}

impl ReceiptPrice {
    /// The price fields of a transaction that paid no tip: a skip, a
    /// deposit, or any transaction on a chain without a schedule, which
    /// reports the price revm charged.
    #[must_use]
    pub fn flat(effective_gas_price: u128) -> Self {
        Self {
            effective_gas_price,
            priority_fee_per_gas: 0,
            priority_fee_paid: 0,
        }
    }
}

/// One executed transaction's tip under a schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TipSettlement {
    /// The tip rate the chain collected: the committed rate, bounded by
    /// the cap above the base fee.
    pub rate: u128,
    /// The base fee of the block: what the receipt reports as the price.
    pub base_fee: u128,
    /// The tip on the gas the transaction left unused: what revm refunded
    /// and the chain keeps.
    pub extra: U256,
    /// The whole tip, `rate * gas_limit`.
    pub paid: U256,
}

impl TipSettlement {
    /// The settlement of a transaction with `fees` that used `gas_used`
    /// in a block with `block` fees. `None` without a schedule, or when
    /// the cap is under the base fee (revm rejects that transaction
    /// before it runs, so it never settles).
    #[must_use]
    pub fn of(fees: &TxFees, block: BlockFees, gas_used: u64) -> Option<Self> {
        if !block.scheduled() {
            return None;
        }
        let rate = fees.effective_tip_rate(block.base_fee)?;
        let paid = fees.tip_amount(block.base_fee)?;
        // `gas_used <= gas_limit`: revm never spends past the limit.
        let unused = U256::from(fees.gas_limit.saturating_sub(gas_used));
        Some(Self {
            rate,
            base_fee: block.base_fee,
            extra: U256::from(rate) * unused,
            paid,
        })
    }

    /// Move the tip on the unused gas from `caller` to `beneficiary` in
    /// revm's post-transaction state. Both accounts are in the map: revm
    /// touched the caller to charge it and the beneficiary to reward it.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutorError::State`] when an account is missing from
    /// the map, or the caller cannot cover the charge. Both are invariant
    /// breaks: revm refunded the caller at least the tip on its unused
    /// gas, so the balance covers it.
    pub fn apply(
        &self,
        state: &mut EvmState,
        caller: Address,
        beneficiary: Address,
    ) -> Result<(), ExecutorError> {
        if self.extra.is_zero() {
            return Ok(());
        }
        let debit = Self::account(state, caller)?;
        debit.info.balance = debit.info.balance.checked_sub(self.extra).ok_or_else(|| {
            ExecutorError::State(alloc::format!(
                "tip settlement: caller {caller} holds {} under the {} tip on unused gas",
                debit.info.balance, self.extra
            ))
        })?;
        let credit = Self::account(state, beneficiary)?;
        credit.info.balance = credit
            .info
            .balance
            .checked_add(self.extra)
            .ok_or_else(|| {
                ExecutorError::State(alloc::format!(
                    "tip settlement: beneficiary {beneficiary} balance overflows"
                ))
            })?;
        Ok(())
    }

    /// The receipt's price fields: the base fee as the price per gas, and
    /// the tip apart.
    #[must_use]
    pub fn receipt_price(&self) -> ReceiptPrice {
        ReceiptPrice {
            effective_gas_price: self.base_fee,
            priority_fee_per_gas: self.rate,
            // A tip above `u128::MAX` wei exceeds any balance, so revm
            // rejected the transaction before it could settle; saturation
            // never changes a real value.
            priority_fee_paid: self.paid.saturating_to(),
        }
    }

    fn account(
        state: &mut EvmState,
        address: Address,
    ) -> Result<&mut revm::state::Account, ExecutorError> {
        state.get_mut(&address).ok_or_else(|| {
            ExecutorError::State(alloc::format!(
                "tip settlement: account {address} is not in the post-transaction state"
            ))
        })
    }
}

/// Settle one transaction's fees: with a schedule, charge the tip in
/// full and report the base fee as the price; without one, report the
/// price revm charged.
///
/// # Errors
///
/// Returns the error of [`TipSettlement::apply`].
pub fn settle(
    fees: &TxFees,
    block: BlockFees,
    gas_used: u64,
    state: &mut EvmState,
    caller: Address,
) -> Result<ReceiptPrice, ExecutorError> {
    let Some(tip) = TipSettlement::of(fees, block, gas_used) else {
        return Ok(ReceiptPrice::flat(fees.max_fee_per_gas));
    };
    tip.apply(state, caller, block.beneficiary)?;
    Ok(tip.receipt_price())
}
