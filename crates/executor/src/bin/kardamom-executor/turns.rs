//! The process loop: one run of the pipeline after another, the
//! bookkeeping at the end of each run, the repairs between two runs, and
//! the verdict that decides the next step.

use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use anyhow::Result;
use kardamom_engine::ExecutorError;
use kardamom_engine::bin_support;
use kardamom_engine::reader::carried::LocalHistory;
use kardamom_state::BlockRange;
use kardamom_types::ReceiptRollback;

use crate::Boot;
use crate::ledger::ReceiptLedger;
use crate::marker::Markers;
use crate::rollback::RollbackRepair;

/// The widest comparison window. A divergence below it needs the operator.
const MAX_COMPARE_BLOCKS: NonZeroU64 = NonZeroU64::new(4096).unwrap();
/// The factor of each widening of the comparison window.
const WIDEN_FACTOR: NonZeroU64 = NonZeroU64::new(4).unwrap();
/// The pause before the first run after a restart of the stream source.
const FIRST_RESTART_PAUSE: Duration = Duration::from_secs(1);
/// The longest pause before a run after a restart of the stream source.
const MAX_RESTART_PAUSE: Duration = Duration::from_secs(30);
/// The doublings of the first pause, at most: 1 s doubled 5 times is 32 s,
/// above the longest pause.
const MAX_DOUBLINGS: u32 = 5;
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
}

/// The state of one run at its start.
pub(crate) struct RunStart {
    pub(crate) prepared: crate::state::PreparedState,
    /// The record that the run publishes before its first receipt.
    pub(crate) rollback: Option<ReceiptRollback>,
    /// The receipt ledger, with `--carry-bytes` only.
    pub(crate) ledger: Option<ReceiptLedger>,
    /// The history that the reader of the member feed compares with the
    /// stream: the committed blocks of the window, and the receipts that
    /// the runs before published above them. With `--carry-bytes` only,
    /// and `None` at genesis.
    pub(crate) history: Option<LocalHistory>,
}

/// What one run hands to the loop at its end.
pub(crate) struct RunEnd {
    pub(crate) engine_error: Option<ExecutorError>,
    pub(crate) ledger: Option<ReceiptLedger>,
    pub(crate) checkpointer: Option<tokio::task::JoinHandle<()>>,
    pub(crate) expected_genesis: Option<alloy_primitives::B256>,
}

/// The state that passes from one run of the process loop to the next.
pub(crate) struct Turns {
    revolutions: u32,
    /// The restarts of the stream source in a row, up to
    /// [`MAX_DOUBLINGS`].
    restarts: u32,
    /// The committed blocks that the reader compares with the stream.
    window: NonZeroU64,
    /// The window of the configuration. A run after a widened run that
    /// did not end below the window compares this window again.
    base: NonZeroU64,
}

impl Turns {
    pub(crate) fn new(window: NonZeroU64) -> Self {
        Self {
            revolutions: 0,
            restarts: 0,
            window,
            base: window,
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
        let verdict = Box::pin(crate::run_once(boot, self.window)).await?;
        self.settle(&verdict);
        match verdict {
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

    /// Compare the configured window again after a run that did not end
    /// below the window: the comparison of that run ended, or a rollback
    /// is staged. A wide window reads many blocks at each start.
    fn settle(&mut self, verdict: &Verdict) {
        if !matches!(verdict, Verdict::Widen(_)) {
            self.window = self.base;
        }
    }

    /// The pause before the next run after a restart of the stream source.
    /// It doubles with each restart in a row, up to the longest pause.
    fn next_pause(&mut self) -> Duration {
        // `restarts` stays at or below MAX_DOUBLINGS, so the shift and the
        // product stay at or below 32 s.
        let pause = (FIRST_RESTART_PAUSE * (1u32 << self.restarts)).min(MAX_RESTART_PAUSE);
        self.restarts = MAX_DOUBLINGS.min(self.restarts + 1);
        pause
    }

    /// Wait before the run after a restart of the stream source: a member
    /// that restarts again and again must not spin the process. A long run
    /// ends the series of restarts.
    async fn pause(&mut self, boot: &Boot, started: Instant) {
        if started.elapsed() >= STABLE_RUN {
            self.restarts = 0;
        }
        let pause = self.next_pause();
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
    /// Open the state of one run. With `--carry-bytes`, also the markers
    /// and the receipt ledger:
    ///
    /// - A pending rollback restores only a state at or below its block,
    ///   and the run publishes it before its first receipt.
    /// - The running marker tells whether the ledger is whole, and the
    ///   ledger gives the receipts that the runs before published above the
    ///   committed block. The history of `window` committed blocks takes
    ///   them.
    ///
    /// # Errors
    ///
    /// Returns `Err` when a marker or the ledger does not read or write,
    /// or the state does not open.
    pub(crate) fn start_run(
        &self,
        window: NonZeroU64,
        expected_genesis: Option<alloy_primitives::B256>,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<RunStart> {
        let args = &self.args;
        if !args.carry_bytes {
            let prepared =
                crate::state::prepare_state(args, expected_genesis, BlockRange::ANY, shutdown)?;
            return Ok(RunStart {
                prepared,
                rollback: None,
                ledger: None,
                history: None,
            });
        }
        let markers = Markers::new(&args.state_dir);
        let rollback = markers.pending()?;
        let restore = rollback.map_or(BlockRange::ANY, |r| BlockRange {
            max: r.to_block,
            ..BlockRange::ANY
        });
        let prepared = crate::state::prepare_state(args, expected_genesis, restore, shutdown)?;
        let whole = markers.start_run()?;
        let ledger = ReceiptLedger::open(markers)?;
        let cursor = kardamom_engine::bin_support::cluster_replay_cursor(&prepared.start);
        let tail = ledger.tail(cursor.position().0, whole)?;
        let history = prepared.history(window)?.map(|h| h.with_tail(tail));
        Ok(RunStart {
            prepared,
            rollback,
            ledger: Some(ledger),
            history,
        })
    }

    /// The bookkeeping at the end of a run, the repairs between two runs,
    /// and the verdict. Every state handle is gone before this runs.
    ///
    /// - The receipt ledger goes to disk, and the running marker goes.
    /// - The checkpoint task ends before a repair touches the checkpoints.
    /// - A replay-window overrun stages a peer checkpoint, so the next run
    ///   restores it instead of re-requesting the same refused
    ///   `REPLAY_FROM`. See `bin_support::replay_unavailable_fallback`.
    /// - A divergence of the canonical stream stages a rollback. See
    ///   [`RollbackRepair`].
    ///
    /// # Errors
    ///
    /// Returns `Err` when a marker or the ledger does not write, or a
    /// staged checkpoint exists but the stale state fails to park.
    pub(crate) async fn finish(&self, end: RunEnd) -> Result<Verdict> {
        let args = &self.args;
        // First the ledger: a kill during the wait for the checkpoint task
        // then finds no running marker, and the next start trusts the
        // ledger. A repair below syncs its own change of the ledger.
        end.ledger
            .as_ref()
            .map(ReceiptLedger::end_run)
            .transpose()?;
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
        let staged = end
            .ledger
            .as_ref()
            .map(|ledger| {
                RollbackRepair {
                    checkpoint_dir: args.checkpoint_dir.as_deref(),
                    checkpoint_peers: &args.checkpoint_peers,
                    state_dir: &args.state_dir,
                    expected_genesis: end.expected_genesis,
                    ledger,
                }
                .stage(end.engine_error.as_ref())
            })
            .transpose()?
            .unwrap_or(false);
        Ok(Verdict::of(end.engine_error, repaired, staged))
    }
}

#[cfg(test)]
#[path = "turns_tests.rs"]
mod tests;
