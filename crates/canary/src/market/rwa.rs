//! `rwa`: the owner mints one token to a ring account, the account moves
//! it to another ring account, that account tries to send it to an
//! address off the allowlist, which must revert, and the owner burns it.
//! Each mint and burn reports the total supply, which must equal the
//! canary's ledger of its mints less its burns.

use alloy_primitives::TxKind;
use alloy_primitives::{Address, U256, address};

use super::{HOLDER, MarketTask, OWNER, RECEIVER, tokens};
use crate::contracts::{CALL_GAS, Rwa};
use crate::metrics;
use crate::outcome::Outcome;
use crate::ring::Call;
use crate::rpc::Rpc;
use crate::store::SupplyChange;

/// An address the allowlist never holds.
const OFF_LIST: Address = address!("000000000000000000000000000000000000dEaD");

impl MarketTask {
    pub(super) async fn rwa(&mut self, rpc: &Rpc, token: Rwa) -> Result<(), Outcome> {
        let ring = self.ctx.ring.addresses();
        let holder = ring[HOLDER % ring.len()];
        let receiver = ring[RECEIVER % ring.len()];
        let one = tokens(1);
        self.supply_change(rpc, token, holder, SupplyChange::Mint(one))
            .await?;
        self.token_call(rpc, HOLDER, Rwa::transfer(receiver, one))
            .await?;
        self.off_list(rpc, token, one).await?;
        self.supply_change(rpc, token, receiver, SupplyChange::Burn(one))
            .await
    }

    /// A transfer to an address off the allowlist must revert.
    async fn off_list(&self, rpc: &Rpc, token: Rwa, amount: U256) -> Result<(), Outcome> {
        let lease = self.ctx.lease_index(RECEIVER, rpc).await?;
        let call = Call::new(
            TxKind::Call(token.0),
            U256::ZERO,
            Rwa::transfer(OFF_LIST, amount),
            CALL_GAS,
        );
        let done = self.ctx.transact("rwa", rpc, lease, call).await?;
        if done.landed.receipt.succeeded() {
            return Err(Outcome::AllowlistBreach);
        }
        Ok(())
    }

    /// Mint to or burn from `account` as the owner, and compare the
    /// supply the token reports with the ledger. The change is stored as
    /// pending before it is sent, so a change whose receipt a restart
    /// lost still counts when the next supply shows it. A mismatch takes
    /// the token's supply as the new ledger, so one fault reports once.
    pub(super) async fn supply_change(
        &mut self,
        rpc: &Rpc,
        token: Rwa,
        account: Address,
        change: SupplyChange,
    ) -> Result<(), Outcome> {
        let earlier = self.state.pending.replace(change);
        self.save().await;
        let input = match change {
            SupplyChange::Mint(amount) => Rwa::mint(account, amount),
            SupplyChange::Burn(amount) => Rwa::burn(account, amount),
        };
        let done = self.token_call(rpc, OWNER, input).await?;
        let seen = token
            .supply_in(&done.landed.receipt)
            .ok_or(Outcome::SupplyMismatch)?;
        let ledger = self.state.supply;
        let expected = change.apply(ledger);
        let with_earlier = earlier
            .and_then(|e| e.apply(ledger))
            .and_then(|s| change.apply(s));
        self.state.pending = None;
        self.state.supply = seen;
        self.save().await;
        ::metrics::gauge!(metrics::RWA_SUPPLY).set(metrics::wei(seen) / 1e18);
        if Some(seen) == expected || Some(seen) == with_earlier {
            Ok(())
        } else {
            tracing::warn!(%seen, %ledger, ?change, "rwa: the supply is not the ledger");
            Err(Outcome::SupplyMismatch)
        }
    }
}
