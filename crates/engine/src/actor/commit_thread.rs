//! The commit thread drains the exec-to-commit channel into adaptive receipt
//! batches. It publishes each batch, then any boundary, on `tx_receipts`.
//! It uses must-deliver retry logic.

use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::Receiver;
use tracing::warn;

use kardamom_types::{BlockBoundary, Receipt};

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

use super::ports::TxReceiptsPublication;
use super::types::ExecToCommit;

/// Maximum number of receipts in one published batch.
///
/// This limit bounds the wire frame. Receipt sizes vary because of logs.
/// A value of 64 keeps the worst-case frame well inside the term-buffer
/// message limit.
///
/// This is not a latency knob. The batch holds only what queued in the
/// exec-to-commit channel during the previous publish. At low rates, each
/// batch has one receipt, and nothing waits.
const RECEIPT_BATCH_MAX: usize = 64;

/// Must-deliver retry state for one publish call. The receipt-batch and
/// boundary publishes each own one of these for their own retry loop.
///
/// A proven divergence from the sink (the validator's receipt cross-check)
/// is fatal, not transient. Do not retry it. A retry would defeat the
/// fail-stop: the failed publish already consumed the buffered receipt, so
/// the retry finds nothing, waits out the receipt window, and lands in the
/// "unverified" arm. The pipeline would then keep committing past a proven
/// mismatch. Propagate the error instead, so `Executor::run` returns it and
/// the process halts.
///
/// Any other error is transient. Warn on attempt 1 and then every 20th
/// attempt. Sleep 50 ms and let the caller retry.
struct MustDeliver {
    attempts: u32,
}

impl MustDeliver {
    fn new() -> Self {
        Self { attempts: 0 }
    }

    fn retry(&mut self, e: ExecutorError, msg: &'static str) -> Result<(), ExecutorError> {
        if matches!(e, ExecutorError::Divergence(_)) {
            return Err(e);
        }
        // A wrap back toward 0 would only re-fire the "attempt 1" log line
        // on a long-running retry; saturate instead.
        self.attempts = self.attempts.saturating_add(1);
        if self.attempts == 1 || self.attempts.is_multiple_of(20) {
            warn!(error = %e, attempts = self.attempts, "{msg}");
        }
        thread::sleep(Duration::from_millis(50));
        Ok(())
    }
}

/// One drained batch: the receipts, an optional closing boundary, and
/// whether the channel closed while draining.
struct Batch {
    receipts: Vec<Receipt>,
    boundary: Option<BlockBoundary>,
    closed: bool,
}

/// Drains the exec-to-commit channel into adaptive receipt batches, and
/// must-deliver publishes each one on `tx_receipts`.
struct CommitLoop<C> {
    tx_receipts_pub: C,
    rx: Receiver<ExecToCommit>,
}

impl<C: TxReceiptsPublication> CommitLoop<C> {
    /// Block for the next message. Then drain any other queued messages
    /// into one batch (adaptive batching). The batch size matches the
    /// arrivals during the previous publish: 1 at a low rate, larger under
    /// load, with no added latency in either case. A boundary flushes the
    /// receipts gathered so far, and this keeps the stream order. Returns
    /// `None` when the channel is already closed with nothing queued.
    fn collect_batch(&self) -> Option<Batch> {
        let mut receipts: Vec<Receipt> = Vec::new();
        let mut boundary = None;
        match self.rx.recv() {
            Ok(ExecToCommit::Receipt(r)) => receipts.push(r),
            Ok(ExecToCommit::Boundary(b)) => boundary = Some(b),
            Err(_) => return None,
        }
        let mut closed = false;
        while boundary.is_none() && receipts.len() < RECEIPT_BATCH_MAX {
            match self.rx.try_recv() {
                Ok(ExecToCommit::Receipt(r)) => receipts.push(r),
                Ok(ExecToCommit::Boundary(b)) => boundary = Some(b),
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    closed = true;
                    break;
                }
            }
        }
        Some(Batch {
            receipts,
            boundary,
            closed,
        })
    }

    /// Must-deliver publish of a receipt batch.
    ///
    /// `tx_receipts` is must-deliver. A transaction's receipt must reach the
    /// ingress that is parking the client. A transient publish failure (for
    /// example `NOT_CONNECTED`, while the ingress subscription is still
    /// forming during multi-host startup) must not drop the receipt or stop
    /// this thread. Stopping the thread would close the bounded
    /// exec-to-commit channel. This would back-pressure the exec thread,
    /// stop `tx_ordering` consumption, and freeze all state progress.
    ///
    /// So retry until the receipt lands. Resume at the unpublished suffix
    /// of the batch. Re-publishing a delivered prefix is a harmless
    /// duplicate on the wire. But the validator's verifying sink consumes
    /// its buffer per receipt, so the suffix resume starts at the first
    /// unpublished receipt.
    ///
    /// Deploy order brings the ingress up first, so this usually succeeds
    /// on the first attempt.
    fn publish_batch(&mut self, receipts: &[Receipt]) -> Result<(), ExecutorError> {
        let mut retry = MustDeliver::new();
        // `from` carries the must-deliver resume point across retries: each
        // retry starts at the first unpublished receipt. A slice iterator
        // cannot express this, since a failed attempt must rewind to `from`
        // instead of advancing.
        let mut from = 0usize;
        while from < receipts.len() {
            let (published, err) = self.tx_receipts_pub.publish_receipts(&receipts[from..]);
            from = from.saturating_add(published);
            if let Some(e) = err {
                retry.retry(e, "tx_receipts publish failed; retrying (must-deliver)")?;
            }
        }
        Ok(())
    }

    /// Must-deliver publish of one boundary. Same retry rule as
    /// [`Self::publish_batch`].
    fn publish_boundary(&mut self, b: &BlockBoundary) -> Result<(), ExecutorError> {
        let mut retry = MustDeliver::new();
        while let Err(e) = self
            .tx_receipts_pub
            .publish(CMessage::BlockBoundary(b.clone()))
        {
            retry.retry(e, "tx_receipts boundary publish failed; retrying")?;
        }
        Ok(())
    }

    fn run(mut self) -> Result<(), ExecutorError> {
        loop {
            let Some(batch) = self.collect_batch() else {
                return Ok(());
            };
            self.publish_batch(&batch.receipts)?;
            if let Some(b) = &batch.boundary {
                self.publish_boundary(b)?;
            }
            if batch.closed {
                return Ok(());
            }
        }
    }
}

pub(crate) fn spawn_commit<C>(
    tx_receipts_pub: C,
    rx: Receiver<ExecToCommit>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    C: TxReceiptsPublication + 'static,
{
    thread::Builder::new()
        .name("executor-commit".into())
        .spawn(move || {
            CommitLoop {
                tx_receipts_pub,
                rx,
            }
            .run()
        })
        .expect("spawn commit")
}
