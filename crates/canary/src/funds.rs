//! The balances of the ring. A task reads each account's L2 balance on a
//! timer, exports it, and marks an account under the floor unfunded, so
//! the probes report `unfunded` instead of failing sends.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{Address, U256};
use futures::StreamExt;

use crate::metrics;
use crate::ring::Ring;
use crate::rpc::Rpc;

#[derive(Debug)]
pub struct Funds {
    pub ring: Arc<Ring>,
    pub endpoints: Vec<Rpc>,
    pub floor: U256,
    pub every: Duration,
}

impl Funds {
    /// Read the balances on the timer, for ever, each time through the
    /// next endpoint.
    pub async fn run(self) {
        ::metrics::gauge!(metrics::BALANCE_FLOOR_WEI, "layer" => "l2")
            .set(metrics::wei(self.floor));
        let mut tick = tokio::time::interval(self.every);
        let mut turn = 0usize;
        loop {
            tick.tick().await;
            self.refresh(&self.endpoints[turn % self.endpoints.len()])
                .await;
            turn = turn.wrapping_add(1);
        }
    }

    async fn refresh(&self, rpc: &Rpc) {
        let accounts = self.ring.addresses().into_iter().enumerate();
        futures::stream::iter(accounts)
            .for_each(|(index, address)| self.refresh_one(rpc, index, address))
            .await;
    }

    async fn refresh_one(&self, rpc: &Rpc, index: usize, address: Address) {
        let Ok(balance) = rpc.balance(address).await else {
            return;
        };
        ::metrics::gauge!(metrics::BALANCE_WEI, "layer" => "l2", "account" => format!("{address:#x}"))
            .set(metrics::wei(balance));
        self.ring.set_funded(index, balance >= self.floor);
    }
}
