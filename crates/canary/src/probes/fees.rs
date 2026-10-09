//! `fees`: a transfer with a tip of one wei per gas and unused gas, whose
//! receipt must follow the fee rules: the price is the block's base fee,
//! the tip rate is the bid, and the tip paid is the rate times the gas
//! limit, also for the unused gas. `eth_feeHistory` and
//! `eth_maxPriorityFeePerGas` must answer; the history answers for the
//! receipt's block once the block closes. Without a fee schedule (a base
//! fee of zero) no tip is paid.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Bytes, TxKind, U256};

use super::{Context, Probe};
use crate::outcome::Outcome;
use crate::ring::Call;
use crate::rpc::{Receipt, Rpc};
use crate::wait::Poll;

const NAME: &str = "fees";
/// The tip rate the probe bids, in wei per gas.
const TIP: u128 = 1;
/// A gas limit above the 21,000 a transfer uses, so the tip on unused gas
/// shows.
const GAS_LIMIT: u64 = 30_000;

#[derive(Debug)]
pub struct Fees {
    ctx: Arc<Context>,
    turn: usize,
}

impl Fees {
    #[must_use]
    pub fn new(ctx: Arc<Context>) -> Self {
        Self { ctx, turn: 0 }
    }

    async fn attempt(&self, rpc: &Rpc) -> Outcome {
        match self.check(rpc).await {
            Ok(()) => Outcome::Success,
            Err(outcome) => outcome,
        }
    }

    async fn check(&self, rpc: &Rpc) -> Result<(), Outcome> {
        rpc.max_priority_fee()
            .await
            .map_err(|_| Outcome::FeeMismatch("max_priority_fee"))?;
        let lease = self.ctx.lease(rpc).await?;
        let call = Call {
            tip: TIP,
            ..Call::new(
                TxKind::Call(self.ctx.neighbor(lease.address())),
                U256::from(1),
                Bytes::new(),
                GAS_LIMIT,
            )
        };
        let done = self.ctx.transact(NAME, rpc, lease, call).await?;
        let receipt = done.landed.receipt;
        if !receipt.succeeded() {
            return Err(Outcome::ReceiptStatus0);
        }
        // The receipt can come before its block closes, and the fee
        // history holds closed blocks only.
        let block = receipt.block();
        let base_fee = Poll::within(self.ctx.timing.read_timeout, self.ctx.timing.poll)
            .until(|| async { rpc.base_fee(block).await.ok() })
            .await
            .ok_or(Outcome::FeeMismatch("fee_history"))?;
        Rules { base_fee }.check(&receipt)
    }
}

/// The fee rules of one block.
struct Rules {
    base_fee: U256,
}

impl Rules {
    /// Compare the receipt's price fields with the rules.
    fn check(&self, receipt: &Receipt) -> Result<(), Outcome> {
        let scheduled = !self.base_fee.is_zero();
        let rate = if scheduled {
            U256::from(TIP)
        } else {
            U256::ZERO
        };
        let paid = rate
            .checked_mul(U256::from(GAS_LIMIT))
            .ok_or(Outcome::FeeMismatch("priority_fee_paid"))?;
        let field = |value: Option<U256>| value.unwrap_or_default();
        if scheduled && receipt.effective_gas_price != Some(self.base_fee) {
            return Err(Outcome::FeeMismatch("base_fee"));
        }
        if field(receipt.priority_fee_per_gas) != rate {
            return Err(Outcome::FeeMismatch("priority_fee_per_gas"));
        }
        if field(receipt.priority_fee_paid) != paid {
            return Err(Outcome::FeeMismatch("priority_fee_paid"));
        }
        Ok(())
    }
}

impl Probe for Fees {
    fn interval(&self) -> Duration {
        self.ctx.timing.fees
    }

    async fn run(&mut self) {
        let rpc = self.ctx.endpoint(self.turn).clone();
        self.turn = self.turn.wrapping_add(1);
        let outcome = self.attempt(&rpc).await;
        outcome.record(NAME, &rpc.endpoint.name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(price: u64, rate: u64, paid: u64) -> Receipt {
        Receipt {
            effective_gas_price: Some(U256::from(price)),
            priority_fee_per_gas: Some(U256::from(rate)),
            priority_fee_paid: Some(U256::from(paid)),
            ..Receipt::default()
        }
    }

    #[test]
    fn the_tip_is_the_rate_times_the_gas_limit() {
        let rules = Rules {
            base_fee: U256::from(7),
        };
        assert_eq!(rules.check(&receipt(7, 1, 30_000)), Ok(()));
        assert_eq!(
            rules.check(&receipt(7, 1, 21_000)),
            Err(Outcome::FeeMismatch("priority_fee_paid"))
        );
        assert_eq!(
            rules.check(&receipt(8, 1, 30_000)),
            Err(Outcome::FeeMismatch("base_fee"))
        );
        assert_eq!(
            rules.check(&receipt(7, 2, 30_000)),
            Err(Outcome::FeeMismatch("priority_fee_per_gas"))
        );
    }

    #[test]
    fn without_a_schedule_no_tip_is_paid() {
        let rules = Rules {
            base_fee: U256::ZERO,
        };
        assert_eq!(rules.check(&receipt(1, 0, 0)), Ok(()));
        assert_eq!(
            rules.check(&receipt(1, 1, 30_000)),
            Err(Outcome::FeeMismatch("priority_fee_per_gas"))
        );
    }
}
