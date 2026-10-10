//! `transfer`: an EIP-1559 transfer between two ring accounts, through
//! each ingress endpoint in turn. It times the submit, the feed stages,
//! and the receipt.

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::{TxKind, U256};

use super::safe::Safe;
use super::{Context, Probe, record_stages};
use crate::metrics;
use crate::outcome::Outcome;
use crate::ring::Call;
use crate::rpc::Rpc;

const NAME: &str = "transfer";
/// The gas of a plain transfer.
const TRANSFER_GAS: u64 = 21_000;

#[derive(Debug)]
pub struct Transfer {
    ctx: Arc<Context>,
    turn: usize,
    /// Landed transfers since the last `safe` sample.
    since_sample: u32,
}

impl Transfer {
    #[must_use]
    pub fn new(ctx: Arc<Context>) -> Self {
        Self {
            ctx,
            turn: 0,
            since_sample: 0,
        }
    }

    /// Hand one landed transfer in `safe_sample` to a `safe` check.
    fn sample(&mut self, rpc: &Rpc, landed: &super::Landed) {
        self.since_sample = self.since_sample.saturating_add(1);
        if self.since_sample < self.ctx.safe_sample.get() {
            return;
        }
        self.since_sample = 0;
        let check = Safe {
            ctx: Arc::clone(&self.ctx),
            rpc: rpc.clone(),
            block: landed.receipt.block(),
            landed: landed.at,
        };
        tokio::spawn(check.check());
    }

    async fn attempt(&mut self, rpc: &Rpc) -> Outcome {
        let mut lease = match self.ctx.lease(rpc).await {
            Ok(lease) => lease,
            Err(outcome) => return outcome,
        };
        let call = Call::new(
            TxKind::Call(self.ctx.neighbor(lease.address())),
            U256::from(1),
            alloy_primitives::Bytes::new(),
            TRANSFER_GAS,
        );
        let sent = match lease.send(rpc, call).await {
            Ok(sent) => sent,
            Err(outcome) => return outcome,
        };
        let name = rpc.endpoint.name.as_str();
        metrics::stage(NAME, name, "submit", sent.took);
        let hashed = sent.at + sent.took;
        let landed = match self.ctx.landed(rpc, sent.hash).await {
            Ok(landed) => landed,
            Err(outcome) => return outcome,
        };
        let gap = lease.found_gap();
        lease.settle().await;
        metrics::stage(
            NAME,
            name,
            "receipt",
            landed.at.saturating_duration_since(hashed),
        );
        record_stages(NAME, name, hashed, &self.ctx.stages(sent.hash).await);
        if !landed.receipt.succeeded() {
            return Outcome::ReceiptStatus0;
        }
        self.keep_anchor(sent.hash).await;
        self.sample(rpc, &landed);
        if gap {
            Outcome::NonceGap
        } else {
            Outcome::Success
        }
    }

    /// The first landed transfer becomes the anchor of the `read` probe.
    async fn keep_anchor(&self, hash: alloy_primitives::B256) {
        if self.ctx.anchor.borrow().is_some() {
            return;
        }
        if let Err(e) = self.ctx.store.save_anchor(hash).await {
            tracing::warn!(error = %e, "store the anchor");
            return;
        }
        self.ctx.anchor.send_replace(Some(hash));
    }
}

impl Probe for Transfer {
    fn interval(&self) -> Duration {
        self.ctx.timing.transfer
    }

    async fn run(&mut self) {
        let rpc = self.ctx.endpoint(self.turn).clone();
        self.turn = self.turn.wrapping_add(1);
        let outcome = self.attempt(&rpc).await;
        outcome.record(NAME, &rpc.endpoint.name);
    }
}
