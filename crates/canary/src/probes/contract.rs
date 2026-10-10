//! `contract`: a write to the canary counter, then a read that must show
//! it. The counter holds one wei per write, and the chain serves no
//! `eth_call`, so the read is `eth_getBalance` of the counter: it must
//! equal the count the write's receipt reports. This probe is the only
//! writer and the only reader of the counter. Its first run deploys the
//! counter when the data directory holds no address.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{TxKind, U256};

use super::{Context, Done, Probe};
use crate::contracts::{Counter, DEPLOY_GAS, INCREMENT_GAS};
use crate::metrics;
use crate::outcome::{Outcome, Stage};
use crate::ring::Call;
use crate::rpc::Rpc;
use crate::store::Contracts;
use crate::wait::Poll;

const NAME: &str = "contract";

#[derive(Debug)]
pub struct Contract {
    ctx: Arc<Context>,
    counter: Option<Counter>,
    turn: usize,
}

impl Contract {
    /// The probe, with the counter the data directory names, if any.
    #[must_use]
    pub fn new(ctx: Arc<Context>, contracts: &Contracts) -> Self {
        Self {
            ctx,
            counter: contracts.counter.map(Counter),
            turn: 0,
        }
    }

    /// Lease an account, send `call`, and wait for a successful receipt.
    async fn transact(&self, rpc: &Rpc, call: Call) -> Result<Done, Outcome> {
        let lease = self.ctx.lease(rpc).await?;
        let done = self.ctx.transact(NAME, rpc, lease, call).await?;
        if !done.landed.receipt.succeeded() {
            return Err(Outcome::ReceiptStatus0);
        }
        Ok(done)
    }

    async fn deploy(&mut self, rpc: &Rpc) -> Result<Done, Outcome> {
        let call = Call::new(
            TxKind::Create,
            U256::ZERO,
            Counter::creation(&self.ctx.ring.addresses()),
            DEPLOY_GAS,
        );
        let done = self.transact(rpc, call).await?;
        let address = done
            .landed
            .receipt
            .contract_address
            .ok_or(Outcome::StateMismatch)?;
        let contracts = Contracts {
            counter: Some(address),
        };
        if let Err(e) = self.ctx.store.save_contracts(&contracts).await {
            tracing::warn!(error = %e, "store the counter address");
        }
        tracing::info!(counter = %address, "canary counter deployed");
        self.counter = Some(Counter(address));
        Ok(done)
    }

    async fn write_and_read(&self, rpc: &Rpc, counter: Counter) -> Result<Done, Outcome> {
        let call = Call::new(
            TxKind::Call(counter.0),
            U256::from(1),
            Counter::increment(),
            INCREMENT_GAS,
        );
        let done = self.transact(rpc, call).await?;
        let count = counter
            .count_in(&done.landed.receipt)
            .ok_or(Outcome::StateMismatch)?;
        let seen = Poll::within(self.ctx.timing.read_timeout, self.ctx.timing.poll)
            .until(|| async {
                rpc.balance(counter.0)
                    .await
                    .ok()
                    .filter(|balance| *balance >= count)
            })
            .await
            .ok_or(Outcome::Timeout(Stage::Read))?;
        if seen != count {
            return Err(Outcome::StateMismatch);
        }
        metrics::stage(NAME, &rpc.endpoint.name, "read", done.landed.at.elapsed());
        Ok(done)
    }

    async fn attempt(&mut self, rpc: &Rpc) -> Outcome {
        let done = match self.counter {
            Some(counter) => self.write_and_read(rpc, counter).await,
            None => self.deploy(rpc).await,
        };
        match done {
            Ok(Done { gap: true, .. }) => Outcome::NonceGap,
            Ok(Done { gap: false, .. }) => Outcome::Success,
            Err(outcome) => outcome,
        }
    }
}

impl Probe for Contract {
    fn interval(&self) -> Duration {
        self.ctx.timing.contract
    }

    async fn run(&mut self) {
        let rpc = self.ctx.endpoint(self.turn).clone();
        self.turn = self.turn.wrapping_add(1);
        let outcome = self.attempt(&rpc).await;
        outcome.record(NAME, &rpc.endpoint.name);
    }
}
