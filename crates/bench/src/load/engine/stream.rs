//! Chaos transfers sign bounded batches while the previous batch is paced.

use tokio::sync::mpsc;

use super::{PlannedTx, Queues};
use crate::load::{LoadConfig, QueuePlan, TransferMix, TransferShape, TxPlanParams};
use crate::signers::SignerSet;

type Batch = Vec<Vec<PlannedTx>>;
type Message = anyhow::Result<Option<Batch>>;

/// At most one received, one queued and one signing batch are resident.
/// Rounding to whole sender rotations adds less than one extra transfer per sender.
const BATCH_SIZE: usize = 4096;

pub(super) struct Stream {
    receiver: mpsc::Receiver<Message>,
    pub(super) signers: SignerSet,
}

impl Stream {
    pub(super) async fn receive(&mut self) -> Message {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("chaos transfer signer exited before its final batch"))?
    }
}

impl Queues {
    pub(in crate::load) async fn stream_transfers(
        cfg: &LoadConfig,
        signers: &SignerSet,
        plan: QueuePlan,
    ) -> anyhow::Result<Self> {
        let producer = Producer {
            mix: TransferMix::single(TransferShape::plain(cfg.to, cfg.value)),
            signers: signers.clone(),
            params: TxPlanParams {
                chain_id: plan.chain_id,
                nonce_start: 0,
                gas_price: cfg.gas_price,
            },
            plan,
            offset: 0,
        };
        let mut stream = producer.start();
        let first = stream
            .receive()
            .await?
            .ok_or_else(|| anyhow::anyhow!("chaos transfer plan has no initial batch"))?;
        let mut queues = Self::new(first);
        queues.more = Some(stream);
        Ok(queues)
    }
}

struct Producer {
    mix: TransferMix,
    signers: SignerSet,
    params: TxPlanParams,
    plan: QueuePlan,
    offset: usize,
}

impl Producer {
    fn start(self) -> Stream {
        let (sender, receiver) = mpsc::channel(1);
        let signers = self.signers.clone();
        // Signing runs off the async workers. Dropping the receiving queues
        // releases a blocked send; cancellation leaves at most one batch's work.
        tokio::task::spawn_blocking(move || self.produce(&sender));
        Stream { receiver, signers }
    }

    fn produce(mut self, sender: &mpsc::Sender<Message>) {
        while !sender.is_closed() {
            let std::ops::ControlFlow::Continue(()) = self.send_next(sender) else {
                return;
            };
        }
    }

    fn send_next(&mut self, sender: &mpsc::Sender<Message>) -> std::ops::ControlFlow<()> {
        let batch = self.next();
        let done = !matches!(&batch, Ok(Some(_)));
        if sender.blocking_send(batch).is_err() || done {
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    }

    fn next(&mut self) -> Message {
        if self.offset == self.plan.per_sender {
            return Ok(None);
        }
        // The minimum is one transaction per sender, including a wide sender set.
        let depth = BATCH_SIZE.div_ceil(self.signers.len());
        let count = depth.min(self.plan.per_sender - self.offset);
        let offset = u64::try_from(self.offset)?;
        let starts = self
            .plan
            .starts
            .iter()
            .map(|start| {
                start
                    .checked_add(offset)
                    .ok_or_else(|| anyhow::anyhow!("chaos transfer nonce overflows u64"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let batch = self
            .mix
            .pregenerate(&self.signers, count, self.params, &starts)?;
        self.offset += count;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests;
