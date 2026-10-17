//! The process loop: one run of the pipeline after another, the
//! bookkeeping at the end of each run, the repairs between two runs, and
//! the verdict that decides the next step.

use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use anyhow::Result;
use kardamom_engine::ExecutorError;
use kardamom_engine::bin_support;
use kardamom_state::BlockRange;
use kardamom_types::ReceiptRollback;

use crate::Boot;
use crate::marker::{Cause, Markers, RunLedger};
use crate::rollback::RollbackRepair;

/// The widest comparison window. A divergence below it needs the operator.
const MAX_COMPARE_BLOCKS: NonZeroU64 = NonZeroU64::new(4096).unwrap();
/// The factor of each widening of the comparison window.
const WIDEN_FACTOR: NonZeroU64 = NonZeroU64::new(4).unwrap();
/// The pause before the first run after a restart of the stream source.
const FIRST_RESTART_PAUSE: Duration = Duration::from_secs(1);
/// The longest pause before a run after a restart of the stream source.
const MAX_RESTART_PAUSE: Duration = Duration::from_secs(30);
/// A run that lasts this long ends the series of restarts: the next pause
/// is the first one again.
const STABLE_RUN: Duration = Duration::from_secs(60);

/// What the process does after one run of the pipeline.
#[derive(Debug)]
pub(crate) enum Verdict {
    /// The engine returned cleanly: the operator asked for shutdown.
    Done,
    /// Run the pipeline again at once. A staged peer checkpoint, or the
    /// checkpoint of a rollback, waits with the stale state parked, and the
    /// next run restores it.
    Revolve,
    /// The stream source restarted: run the pipeline again from the state
    /// DB as it is, after a pause.
    Restart,
    /// The divergence can be below the comparison window: run again with a
    /// wider window.
    Widen(ExecutorError),
    /// The pipeline failed and the process cannot repair it.
    Failed(ExecutorError),
}

impl Verdict {
    /// The verdict for one engine result and the results of the repairs.
    /// `repaired` is the resync outcome label the fallback returned, or
    /// `None` when the error was not a refused replay, or the fallback is
    /// not configured. `staged` tells whether a rollback is staged.
    pub(crate) fn of(
        engine_error: Option<ExecutorError>,
        repaired: Option<&str>,
        staged: bool,
    ) -> Self {
        match (engine_error, repaired, staged) {
            (None, _, _) => Self::Done,
            (Some(_), _, true) | (Some(_), Some("peer-checkpoint"), false) => Self::Revolve,
            (Some(ExecutorError::SourceRestarted { .. }), _, false) => Self::Restart,
            (Some(e @ ExecutorError::DivergedBelowWindow { widen: true, .. }), _, false) => {
                Self::Widen(e)
            }
            (Some(e), _, false) => Self::Failed(e),
        }
    }

    /// Whether the process ends after this verdict.
    fn ends(&self) -> bool {
        matches!(self, Self::Done | Self::Failed(_))
    }
}

/// What one run needs from the loop.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunInput {
    /// The first run of the process: it checks the clean marker.
    pub(crate) first: bool,
    /// The committed blocks that the reader compares with the stream.
    pub(crate) window: NonZeroU64,
}

/// The state of one run at its start.
pub(crate) struct RunStart {
    pub(crate) prepared: crate::state::PreparedState,
    /// The record that the run publishes before its first receipt.
    pub(crate) rollback: Option<ReceiptRollback>,
    pub(crate) ledger: RunLedger,
}

/// What one run hands to the loop at its end.
pub(crate) struct RunEnd {
    pub(crate) engine_error: Option<ExecutorError>,
    /// The last committed block after the writer stopped.
    pub(crate) committed: u64,
    pub(crate) ledger: RunLedger,
    pub(crate) checkpointer: Option<tokio::task::JoinHandle<()>>,
    pub(crate) expected_genesis: Option<alloy_primitives::B256>,
}

impl RunEnd {
    /// Stop the state writer thread, and return the last committed block.
    /// The stop closes the delta channel, joins the thread, and logs its
    /// final error. The engine has finished, so its clones of the delta
    /// sender are already gone.
    pub(crate) fn stop_writer(mut writer: kardamom_state::WriterHandle, start_block: u64) -> u64 {
        if let Err(e) = writer.shutdown() {
            tracing::error!(error = %e, "state writer shutdown returned an error");
        }
        writer
            .snapshot_rx
            .current()
            .map_or(start_block, |snapshot| snapshot.block_number())
    }
}

/// The state that passes from one run of the process loop to the next.
pub(crate) struct Turns {
    revolutions: u32,
    /// The restarts of the stream source in a row.
    restarts: u32,
    window: NonZeroU64,
    first: bool,
}

impl Turns {
    pub(crate) fn new(window: NonZeroU64) -> Self {
        Self {
            revolutions: 0,
            restarts: 0,
            window,
            first: true,
        }
    }

    /// One turn of the process loop: a run, then its verdict. `Continue`
    /// means run again; `Break` means the process is done. A failure the
    /// process cannot repair returns `Err`, so the process exits non-zero:
    /// the orchestrator tells a dead pipeline from a clean shutdown by the
    /// exit status, which the whole "fail loudly, resume from the cursor"
    /// loop relies on.
    pub(crate) async fn turn(&mut self, boot: &Boot) -> Result<ControlFlow<()>> {
        let started = Instant::now();
        let input = RunInput {
            first: std::mem::take(&mut self.first),
            window: self.window,
        };
        match Box::pin(crate::run_once(boot, input)).await? {
            Verdict::Done => return Ok(ControlFlow::Break(())),
            Verdict::Failed(e) => {
                return Err(anyhow::Error::new(e).context("executor pipeline failed"));
            }
            Verdict::Revolve => {}
            Verdict::Restart => self.pause(boot, started).await,
            Verdict::Widen(e) => self.widen(e)?,
        }
        if boot.stop.is_cancelled() {
            tracing::info!("shutdown signal received during the repair; not revolving");
            return Ok(ControlFlow::Break(()));
        }
        self.revolutions += 1;
        tracing::info!(
            revolutions = self.revolutions,
            window = self.window,
            "the pipeline starts again in-process from the state DB or a staged checkpoint"
        );
        Ok(ControlFlow::Continue(()))
    }

    /// Wait before the run after a restart of the stream source: a member
    /// that restarts again and again must not spin the process. The pause
    /// doubles with each restart in a row, and a long run ends the series.
    async fn pause(&mut self, boot: &Boot, started: Instant) {
        if started.elapsed() >= STABLE_RUN {
            self.restarts = 0;
        }
        let pause = FIRST_RESTART_PAUSE
            .checked_mul(2u32.saturating_pow(self.restarts))
            .map_or(MAX_RESTART_PAUSE, |p| p.min(MAX_RESTART_PAUSE));
        self.restarts = self.restarts.saturating_add(1);
        tracing::info!(
            ?pause,
            restarts = self.restarts,
            "pausing before the next run"
        );
        tokio::select! {
            () = tokio::time::sleep(pause) => {}
            () = boot.stop.cancelled() => {}
        }
    }

    /// Widen the comparison window for the next run.
    ///
    /// # Errors
    ///
    /// Returns `below`, the divergence below the window, when the window
    /// is at its widest: the operator restores the executor.
    fn widen(&mut self, below: ExecutorError) -> Result<()> {
        if self.window >= MAX_COMPARE_BLOCKS {
            return Err(anyhow::Error::new(below)
                .context("the divergence is below the widest comparison window"));
        }
        self.window = self
            .window
            .saturating_mul(WIDEN_FACTOR)
            .min(MAX_COMPARE_BLOCKS);
        tracing::warn!(window = self.window, "comparing again with a wider window");
        Ok(())
    }
}

impl Boot {
    /// Open the state of one run: the restore range of a pending rollback,
    /// the state DB, the record that the run publishes before its first
    /// receipt (see [`Markers::open`]), and the ledger of its receipts.
    /// A pending rollback restores only a state at or below its block.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a marker does not read or write, or the state
    /// does not open.
    pub(crate) fn start_run(
        &self,
        input: RunInput,
        expected_genesis: Option<alloy_primitives::B256>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<RunStart> {
        let args = &self.args;
        let markers = Markers::new(&args.state_dir);
        let restore = markers.pending()?.map_or(BlockRange::ANY, |r| BlockRange {
            max: r.to_block,
            ..BlockRange::ANY
        });
        let prepared = crate::state::prepare_state(args, expected_genesis, restore, shutdown)?;
        let rollback = markers.open(input.first, prepared.start.block, self.restart_cause())?;
        Ok(RunStart {
            prepared,
            rollback,
            ledger: RunLedger::new(markers),
        })
    }

    fn restart_cause(&self) -> Cause {
        Cause::Restart {
            recorder: self.args.recorder_id,
        }
    }

    /// The bookkeeping at the end of a run, the repairs between two runs,
    /// and the verdict. Every state handle is gone before this runs.
    ///
    /// - Receipts published above the committed block can be void, so the
    ///   next run publishes a rollback to the committed block.
    /// - The checkpoint task ends before a repair touches the checkpoints.
    /// - A replay-window overrun stages a peer checkpoint, so the next run
    ///   restores it instead of re-requesting the same refused
    ///   `REPLAY_FROM`. See `bin_support::replay_unavailable_fallback`.
    /// - A divergence of the canonical stream stages a checkpoint below the
    ///   divergence block. See [`RollbackRepair`].
    /// - A run that ends the process leaves the clean marker.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a marker does not write, or a staged checkpoint
    /// exists but the stale state fails to park.
    pub(crate) async fn finish(&self, end: RunEnd) -> Result<Verdict> {
        let args = &self.args;
        let markers = Markers::new(&args.state_dir);
        end.ledger.settle(end.committed, self.restart_cause())?;
        if let Some(checkpointer) = end.checkpointer {
            checkpointer.await?;
        }
        let repaired = bin_support::replay_unavailable_fallback(
            end.engine_error.as_ref(),
            args.checkpoint_dir.as_deref(),
            &args.checkpoint_peers,
            &args.state_dir,
            end.expected_genesis,
            false,
        )?;
        if let Some(outcome) = repaired {
            metrics::counter!(kardamom_engine::metrics::RESYNC_TOTAL, "outcome" => outcome)
                .increment(1);
        }
        let staged = RollbackRepair {
            checkpoint_dir: args.checkpoint_dir.as_deref(),
            checkpoint_peers: &args.checkpoint_peers,
            state_dir: &args.state_dir,
            expected_genesis: end.expected_genesis,
            markers: &markers,
        }
        .stage(end.engine_error.as_ref())?;
        let verdict = Verdict::of(end.engine_error, repaired, staged);
        if verdict.ends() {
            markers.mark_clean()?;
        }
        Ok(verdict)
    }
}

#[cfg(test)]
#[path = "turns_tests.rs"]
mod tests;
