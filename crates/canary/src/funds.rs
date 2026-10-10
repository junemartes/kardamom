//! The balances of the ring. A task reads each account's L2 balance on a
//! timer, exports it, and marks an account under the floor unfunded, so
//! the probes report `unfunded` instead of failing sends. The deposits
//! credit the first ring account; the task tops up the others from it
//! when they fall under the floor and it can spare the amount.

use std::sync::Arc;

use alloy_primitives::{Address, Bytes, TxKind, U256};
use futures::StreamExt;

use crate::metrics;
use crate::probes::Context;
use crate::ring::Call;
use crate::rpc::Rpc;

/// The gas of a plain transfer.
const TRANSFER_GAS: u64 = 21_000;

#[derive(Debug)]
pub struct Funds {
    pub ctx: Arc<Context>,
    pub floor: U256,
    pub topup: U256,
}

impl Funds {
    /// Read the balances on the timer, for ever, each time through the
    /// next endpoint.
    pub async fn run(self) {
        ::metrics::gauge!(metrics::BALANCE_FLOOR_WEI, "layer" => "l2")
            .set(metrics::wei(self.floor));
        let mut tick = tokio::time::interval(self.ctx.timing.balance);
        let mut turn = 0usize;
        loop {
            tick.tick().await;
            self.refresh(self.ctx.endpoint(turn)).await;
            turn = turn.wrapping_add(1);
        }
    }

    async fn refresh(&self, rpc: &Rpc) {
        let accounts = self.ctx.ring.addresses().into_iter().enumerate();
        let low: Vec<(usize, Address)> = futures::stream::iter(accounts)
            .filter_map(|(index, address)| self.refresh_one(rpc, index, address))
            .collect()
            .await;
        futures::stream::iter(low.into_iter().filter(|(index, _)| *index > 0))
            .for_each(|(_, address)| self.top_up(rpc, address))
            .await;
    }

    /// Export one balance and mark the account. Returns the account when
    /// it is under the floor.
    async fn refresh_one(
        &self,
        rpc: &Rpc,
        index: usize,
        address: Address,
    ) -> Option<(usize, Address)> {
        let balance = rpc.balance(address).await.ok()?;
        ::metrics::gauge!(metrics::BALANCE_WEI, "layer" => "l2", "account" => format!("{address:#x}"))
            .set(metrics::wei(balance));
        let funded = balance >= self.floor;
        self.ctx.ring.set_funded(index, funded);
        (!funded).then_some((index, address))
    }

    /// Send the top-up to `to` from the first ring account, when that
    /// account keeps at least the floor and one more top-up after it.
    async fn top_up(&self, rpc: &Rpc, to: Address) {
        let first = self.ctx.ring.addresses()[0];
        let spare = self
            .floor
            .saturating_add(self.topup.saturating_mul(U256::from(2)));
        let Ok(balance) = rpc.balance(first).await else {
            return;
        };
        if balance < spare {
            return;
        }
        let outcome = match self.ctx.lease_index(0, rpc).await {
            Ok(lease) => {
                let call = Call::new(TxKind::Call(to), self.topup, Bytes::new(), TRANSFER_GAS);
                match self.ctx.transact("funds", rpc, lease, call).await {
                    Ok(done) if done.landed.receipt.succeeded() => "success",
                    _ => "failure",
                }
            }
            Err(_) => "failure",
        };
        tracing::info!(account = %to, outcome, "funds: top-up");
        ::metrics::counter!(metrics::TOPUPS_TOTAL, "outcome" => outcome).increment(1);
    }
}
