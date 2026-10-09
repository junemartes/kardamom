//! The commit thread drains the exec-to-commit channel into adaptive receipt
//! batches. It publishes each batch, then any boundary, on `tx_receipts`.
//! It uses must-deliver retry logic with a bounded escalation.

use std::ops::ControlFlow;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use tracing::{error, warn};

use kardamom_types::{BlockBoundary, ReceiptRows};

use crate::error::ExecutorError;
use crate::exec_types::CMessage;

use super::escalation::{Escalation, PUBLICATION_DEAD_EXIT_CODE, PublicationHealth, Step};
use super::ports::TxReceiptsPublication;
use super::types::{ExecToCommit, ExecutorConfig};

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

/// The topic this thread publishes, for the log lines and the metric
/// labels.
const TOPIC: &str = "tx_receipts";

/// The pause between two must-deliver attempts.
const RETRY_PAUSE: Duration = Duration::from_millis(50);

/// Must-deliver retry state of the publication. The receipt-batch and
/// boundary publishes share it, so the escalation clock spans every
/// attempt of one unconnected period.
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
/// attempt. Sleep [`RETRY_PAUSE`] and let the caller retry. A
/// not-connected error while a subscriber is known also runs the
/// [`Escalation`] clock: a reopen of the publication after one stall
/// budget, and a [`Step::Exit`] after five, which this thread turns into
/// [`ExecutorError::PublicationDead`]. Any other transient error proves a
/// subscriber and resets the clock. A not-connected error while no
/// subscriber is known resets it too: nothing to reopen for.
struct MustDeliver {
    attempts: u32,
    escalation: Escalation,
    health: PublicationHealth,
}

impl MustDeliver {
    fn new(escalation: Escalation) -> Self {
        Self {
            attempts: 0,
            escalation,
            health: PublicationHealth::new(TOPIC),
        }
    }

    /// One failed attempt, with `listed` saying whether a subscriber is
    /// known. Returns the step the caller takes next: [`Step::Wait`]
    /// after the pause, or [`Step::Reopen`].
    fn failed(
        &mut self,
        e: ExecutorError,
        listed: bool,
        msg: &'static str,
    ) -> Result<Step, ExecutorError> {
        if matches!(e, ExecutorError::Divergence(_)) {
            return Err(e);
        }
        // A wrap back toward 0 would only re-fire the "attempt 1" log line
        // on a long-running retry; saturate instead.
        self.attempts = self.attempts.saturating_add(1);
        if self.attempts == 1 || self.attempts.is_multiple_of(20) {
            warn!(error = %e, attempts = self.attempts, "{msg}");
        }
        let step = self.escalate(&e, listed);
        if let Step::Exit { unconnected } = step {
            return Err(self.dead(unconnected));
        }
        thread::sleep(RETRY_PAUSE);
        Ok(step)
    }

    /// The escalation step of one failure, with the gauges updated. The
    /// clock runs on a not-connected failure while a subscriber is
    /// known.
    fn escalate(&mut self, e: &ExecutorError, listed: bool) -> Step {
        let now = Instant::now();
        let not_connected = matches!(e, ExecutorError::NotConnected { .. });
        let step = if not_connected && listed {
            self.escalation.unconnected(now)
        } else {
            self.escalation.connected();
            Step::Wait
        };
        self.health
            .report(!not_connected, self.escalation.unconnected_for(now));
        step
    }

    /// The error that ends the thread after `unconnected` without a
    /// subscriber. The line names the topic, the durations and the exit
    /// code, so an operator reads the cause in the last line.
    fn dead(&self, unconnected: Duration) -> ExecutorError {
        let reopen_after_s = self.escalation.reopen_after().as_secs();
        let unconnected_s = unconnected.as_secs();
        error!(
            topic = TOPIC,
            unconnected_s,
            reopen_after_s,
            exit_after_s = self.escalation.exit_after().as_secs(),
            exit_code = PUBLICATION_DEAD_EXIT_CODE,
            "the publication stayed unconnected past its budget; the process exits so the supervisor restarts it"
        );
        ExecutorError::PublicationDead {
            topic: TOPIC.into(),
            unconnected_s,
            reopen_after_s,
        }
    }

    /// A publish landed: the attempt count and the clock reset.
    fn delivered(&mut self) {
        self.attempts = 0;
        self.escalation.connected();
        self.health.report(true, Duration::ZERO);
    }
}

/// One drained batch: the receipts with their account rows, an optional
/// closing boundary, and whether the channel closed while draining.
struct Batch {
    receipts: Vec<ReceiptRows>,
    boundary: Option<BlockBoundary>,
    closed: bool,
}

impl Batch {
    fn new() -> Self {
        Self {
            receipts: Vec::new(),
            boundary: None,
            closed: false,
        }
    }

    /// Fold one drained item into the batch. Returns whether the caller
    /// should keep draining. The `while` loop in
    /// [`CommitLoop::collect_batch`] stays free of a branch.
    fn absorb(&mut self, drained: Drained) -> ControlFlow<()> {
        match drained {
            Drained::Receipt(r) => {
                self.receipts.push(*r);
                ControlFlow::Continue(())
            }
            Drained::Boundary(b) => {
                self.boundary = Some(b);
                ControlFlow::Continue(())
            }
            Drained::Empty => ControlFlow::Break(()),
            Drained::Closed => {
                self.closed = true;
                ControlFlow::Break(())
            }
        }
    }
}

/// One non-blocking receive result from the exec-to-commit channel.
enum Drained {
    Receipt(Box<ReceiptRows>),
    Boundary(BlockBoundary),
    Empty,
    Closed,
}

/// The commit thread's state: the `tx_receipts` publication, the
/// exec-to-commit channel it drains into adaptive receipt batches, and
/// the must-deliver retry state.
pub(crate) struct CommitLoop<C> {
    tx_receipts_pub: C,
    rx: Receiver<ExecToCommit>,
    retry: MustDeliver,
}

impl<C: TxReceiptsPublication + 'static> CommitLoop<C> {
    /// A commit loop with the escalation of the default stall budget.
    pub(crate) fn new(tx_receipts_pub: C, rx: Receiver<ExecToCommit>) -> Self {
        let escalation = Escalation::from_stall_budget(ExecutorConfig::default().stall_budget);
        Self {
            tx_receipts_pub,
            rx,
            retry: MustDeliver::new(escalation),
        }
    }

    /// The commit loop with `escalation` in place of the default.
    pub(crate) fn escalating(mut self, escalation: Escalation) -> Self {
        self.retry = MustDeliver::new(escalation);
        self
    }

    /// Spawn the commit thread.
    ///
    /// # Panics
    ///
    /// Panics if the OS refuses to spawn the thread.
    pub(crate) fn spawn(self) -> JoinHandle<Result<(), ExecutorError>> {
        thread::Builder::new()
            .name("executor-commit".into())
            .spawn(move || self.run())
            .expect("spawn commit")
    }

    /// Block for the next message. Then drain any other queued messages
    /// into one batch (adaptive batching). The batch size matches the
    /// arrivals during the previous publish: 1 at a low rate, larger under
    /// load, with no added latency in either case. A boundary flushes the
    /// receipts gathered so far, and this keeps the stream order. Returns
    /// `None` when the channel is already closed with nothing queued.
    fn collect_batch(&self) -> Option<Batch> {
        let mut batch = Batch::new();
        match self.rx.recv() {
            Ok(ExecToCommit::Receipt(r)) => batch.receipts.push(*r),
            Ok(ExecToCommit::Boundary(b)) => batch.boundary = Some(b),
            Err(_) => return None,
        }
        while batch.boundary.is_none()
            && batch.receipts.len() < RECEIPT_BATCH_MAX
            && batch.absorb(self.try_drain_one()).is_continue()
        {}
        Some(batch)
    }

    /// Try one non-blocking receive. The `while` loop in
    /// [`Self::collect_batch`] matches the result and stays free of a
    /// nested branch.
    fn try_drain_one(&self) -> Drained {
        match self.rx.try_recv() {
            Ok(ExecToCommit::Receipt(r)) => Drained::Receipt(r),
            Ok(ExecToCommit::Boundary(b)) => Drained::Boundary(b),
            Err(crossbeam_channel::TryRecvError::Empty) => Drained::Empty,
            Err(crossbeam_channel::TryRecvError::Disconnected) => Drained::Closed,
        }
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
    /// on the first attempt. A publication that stays unconnected runs
    /// the escalation of [`MustDeliver`]: one reopen, then the exit.
    fn publish_batch(&mut self, receipts: &[ReceiptRows]) -> Result<(), ExecutorError> {
        // `from` carries the must-deliver resume point across retries: each
        // retry starts at the first unpublished receipt. A slice iterator
        // cannot express this, since a failed attempt must rewind to `from`
        // instead of advancing.
        let mut from = 0usize;
        while from < receipts.len() {
            from = self.publish_batch_step(receipts, from)?;
        }
        Ok(())
    }

    /// Publish one must-deliver attempt starting at `from`, retrying on
    /// failure. Returns the resume point for the next attempt. The `while`
    /// loop in [`Self::publish_batch`] stays free of a branch.
    fn publish_batch_step(
        &mut self,
        receipts: &[ReceiptRows],
        from: usize,
    ) -> Result<usize, ExecutorError> {
        let (published, err) = self.tx_receipts_pub.publish_receipts(&receipts[from..]);
        let from = from.saturating_add(published);
        match err {
            Some(e) => self.recover(e, "tx_receipts publish failed; retrying (must-deliver)")?,
            None => self.retry.delivered(),
        }
        Ok(from)
    }

    /// Must-deliver publish of one boundary. Same retry rule as
    /// [`Self::publish_batch`].
    fn publish_boundary(&mut self, b: &BlockBoundary) -> Result<(), ExecutorError> {
        while let Err(e) = self
            .tx_receipts_pub
            .publish(CMessage::BlockBoundary(b.clone()))
        {
            self.recover(e, "tx_receipts boundary publish failed; retrying")?;
        }
        self.retry.delivered();
        Ok(())
    }

    /// Recover from one failed attempt: wait, or open the publication
    /// again when it stayed unconnected for one stall budget. The loops
    /// in [`Self::publish_batch`] and [`Self::publish_boundary`] stay free
    /// of a branch.
    fn recover(&mut self, e: ExecutorError, msg: &'static str) -> Result<(), ExecutorError> {
        let listed = self.tx_receipts_pub.subscribers_listed();
        if self.retry.failed(e, listed, msg)? == Step::Reopen {
            self.reopen()?;
        }
        Ok(())
    }

    /// Open the publication again on a new session. The subscribers
    /// attach the new session, and the pending receipts ride it.
    fn reopen(&mut self) -> Result<(), ExecutorError> {
        warn!(
            topic = TOPIC,
            unconnected_s = self
                .retry
                .escalation
                .unconnected_for(Instant::now())
                .as_secs(),
            exit_after_s = self.retry.escalation.exit_after().as_secs(),
            "the publication stays unconnected; opening it again on a new session"
        );
        self.tx_receipts_pub.reopen()
    }

    fn run(mut self) -> Result<(), ExecutorError> {
        let mut closed = false;
        while !closed && let Some(batch) = self.collect_batch() {
            closed = self.publish_one_batch(&batch)?;
        }
        Ok(())
    }

    /// Publish one collected batch's receipts and boundary. Returns
    /// whether the batch closes the stream. The loop in [`Self::run`]
    /// stays free of a branch.
    fn publish_one_batch(&mut self, batch: &Batch) -> Result<bool, ExecutorError> {
        self.publish_batch(&batch.receipts)?;
        if let Some(b) = &batch.boundary {
            self.publish_boundary(b)?;
        }
        Ok(batch.closed)
    }
}
