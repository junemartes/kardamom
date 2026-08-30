//! The commit thread drains the exec-to-commit channel into adaptive receipt
//! batches. It publishes each batch, then any boundary, on tx_receipts.
//! It uses must-deliver retry logic.

use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::Receiver;
use tracing::warn;

use kardamom_types::Receipt;

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

/// One step of the must-deliver retry loop. The receipt-batch and boundary
/// publishes share this step.
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
fn retry_must_deliver(
    attempts: &mut u32,
    e: ExecutorError,
    msg: &'static str,
) -> Result<(), ExecutorError> {
    if matches!(e, ExecutorError::Divergence(_)) {
        return Err(e);
    }
    *attempts += 1;
    if *attempts == 1 || attempts.is_multiple_of(20) {
        warn!(error = %e, attempts = *attempts, "{msg}");
    }
    thread::sleep(Duration::from_millis(50));
    Ok(())
}

pub(crate) fn spawn_commit<C>(
    mut tx_receipts_pub: C,
    rx: Receiver<ExecToCommit>,
) -> JoinHandle<Result<(), ExecutorError>>
where
    C: TxReceiptsPublication + 'static,
{
    thread::Builder::new()
        .name("executor-commit".into())
        .spawn(move || -> Result<(), ExecutorError> {
            loop {
                // Block for the next message. Then drain any other queued
                // messages into one receipt batch (adaptive batching). The
                // batch size matches the arrivals during the previous
                // publish: 1 at a low rate, larger under load, with no
                // added latency in either case. A boundary flushes the
                // receipts gathered so far, and this keeps the stream order.
                let mut receipts: Vec<Receipt> = Vec::new();
                let mut boundary = None;
                match rx.recv() {
                    Ok(ExecToCommit::Receipt(r)) => receipts.push(r),
                    Ok(ExecToCommit::Boundary(b)) => boundary = Some(b),
                    Err(_) => return Ok(()),
                }
                let mut closed = false;
                while boundary.is_none() && receipts.len() < RECEIPT_BATCH_MAX {
                    match rx.try_recv() {
                        Ok(ExecToCommit::Receipt(r)) => receipts.push(r),
                        Ok(ExecToCommit::Boundary(b)) => boundary = Some(b),
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            closed = true;
                            break;
                        }
                    }
                }

                // tx_receipts is must-deliver. A transaction's receipt must reach
                // the ingress that is parking the client. A transient publish
                // failure (for example NOT_CONNECTED, while the ingress
                // subscription is still forming during multi-host startup) must
                // not drop the receipt or stop this thread. Stopping the thread
                // would close the bounded exec-to-commit channel. This would
                // back-pressure the exec thread, stop tx_ordering consumption,
                // and freeze all state progress.
                //
                // So retry until the receipt lands. Resume at the unpublished
                // suffix of the batch. Re-publishing a delivered prefix is a
                // harmless duplicate on the wire. But the validator's verifying
                // sink consumes its buffer per receipt, so the suffix resume
                // keeps the same semantics as the old one-publish-per-receipt
                // loop.
                //
                // Deploy order brings the ingress up first, so this usually
                // succeeds on the first attempt.
                let mut attempts: u32 = 0;
                let mut from = 0usize;
                while from < receipts.len() {
                    let (published, err) = tx_receipts_pub.publish_receipts(&receipts[from..]);
                    from += published;
                    if let Some(e) = err {
                        retry_must_deliver(
                            &mut attempts,
                            e,
                            "tx_receipts publish failed; retrying (must-deliver)",
                        )?;
                    }
                }
                if let Some(b) = boundary {
                    let mut attempts: u32 = 0;
                    while let Err(e) = tx_receipts_pub.publish(CMessage::BlockBoundary(b.clone())) {
                        retry_must_deliver(
                            &mut attempts,
                            e,
                            "tx_receipts boundary publish failed; retrying",
                        )?;
                    }
                }
                if closed {
                    return Ok(());
                }
            }
        })
        .expect("spawn commit")
}
