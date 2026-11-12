//! Chaos transfers sign a bounded prefix ahead of the pacer on a blocking worker.

use anyhow::Context;
use tokio::sync::mpsc;

use super::{BuiltQueues, LoadConfig, QueuePlan, Queues};
use crate::signers::SignerSet;

/// One initial batch, two queued batches and one signing batch bound unused work.
const BATCH_PER_SENDER: usize = 256;
const BUFFERED_BATCHES: usize = 2;

pub(super) struct SignedBatches {
    receiver: mpsc::Receiver<anyhow::Result<Queues>>,
    signers: SignerSet,
}

impl SignedBatches {
    pub(super) fn prepare(
        cfg: LoadConfig,
        signers: SignerSet,
        mut plan: QueuePlan,
    ) -> anyhow::Result<BuiltQueues> {
        plan.per_sender = BATCH_PER_SENDER;
        let initial = cfg.build_queues(&signers, &plan)?;
        let (sender, receiver) = mpsc::channel(BUFFERED_BATCHES);
        let producer = Producer {
            cfg,
            signers: signers.clone(),
            plan,
            sender,
        };
        tokio::task::spawn_blocking(move || producer.run());
        Ok(BuiltQueues {
            queues: initial.queues.with_refill(Self { receiver, signers }),
            defi_deploys: None,
        })
    }

    pub(super) async fn receive(&mut self) -> anyhow::Result<Queues> {
        self.receiver
            .recv()
            .await
            .context("chaos signing worker stopped before the submission window closed")?
    }

    pub(super) fn signers(&self) -> &SignerSet {
        &self.signers
    }
}

struct Producer {
    cfg: LoadConfig,
    signers: SignerSet,
    plan: QueuePlan,
    sender: mpsc::Sender<anyhow::Result<Queues>>,
}

impl Producer {
    fn next(&mut self) -> anyhow::Result<Queues> {
        let count = u64::try_from(self.plan.per_sender)?;
        self.plan.starts = self
            .plan
            .starts
            .iter()
            .map(|nonce| {
                nonce
                    .checked_add(count)
                    .context("chaos transfer nonce overflows")
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(self.cfg.build_queues(&self.signers, &self.plan)?.queues)
    }

    fn step(&mut self) -> bool {
        let batch = self.next();
        let failed = batch.is_err();
        self.sender.blocking_send(batch).is_ok() && !failed
    }

    fn run(mut self) {
        while !self.sender.is_closed() && self.step() {}
    }
}

#[cfg(test)]
mod tests;
