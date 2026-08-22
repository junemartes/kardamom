//! The commit thread: drains the exec → commit channel into adaptive receipt
//! batches and publishes them (then any boundary) on tx_receipts with
//! must-deliver retry semantics.

use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::Receiver;
use tracing::warn;

use kardamom_types::Receipt;

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

use super::ports::TxReceiptsPublication;
use super::types::ExecToCommit;

/// Max receipts per published batch. Bounds the wire frame (receipts with
/// logs vary in size; 64 keeps worst-case frames well inside the term-buffer
/// message limit) — NOT a latency knob: the batch is whatever accumulated in
/// the exec→commit channel while the previous publish was in flight, so at
/// low rates batches are size 1 and nothing waits.
const RECEIPT_BATCH_MAX: usize = 64;

/// One step of the must-deliver retry loop, shared by the receipts-batch and
/// boundary publishes: a PROVEN divergence from the sink (the validator's
/// receipt cross-check) is fatal, not transient. Retrying would silently
/// defeat the fail-stop: the first failing publish already consumed the
/// buffered receipt, so a retry finds nothing, waits out the receipt window,
/// lands in the "unverified" arm and the pipeline keeps committing past a
/// proven mismatch. Propagate instead so `Executor::run` returns the error
/// and the process halts. Any other error is transient: warn (at attempt 1,
/// then every 20th), sleep 50ms, and let the caller retry.
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
                // Block for the next message, then opportunistically drain
                // whatever else is already queued into ONE receipt batch
                // (adaptive batching: batch size ≈ arrivals during the
                // previous publish — 1 at low rate, larger under load, no
                // added latency in either regime). A boundary flushes the
                // receipts gathered so far first, preserving stream order.
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

                // tx_receipts is MUST-DELIVER: a transaction's receipt has to make
                // it back to the ingress that is parking the client. A transient
                // publish failure — NOT_CONNECTED while the ingress's subscription
                // is still forming during multi-host bring-up — must NOT drop the
                // receipt or kill this thread. Killing it would close the bounded
                // exec→commit channel, back-pressure the exec thread, stop
                // tx_ordering consumption, and freeze ALL state progress. So retry
                // until it lands, resuming at the unpublished SUFFIX of the batch
                // (re-publishing a delivered prefix is only harmless-duplicate on
                // the wire, but the validator's verifying sink consumes its buffer
                // per receipt — the suffix resume keeps its semantics identical to
                // the old one-publish-per-receipt loop). (Deploy order brings the
                // ingress up first, so this normally succeeds on the first
                // attempt.)
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
