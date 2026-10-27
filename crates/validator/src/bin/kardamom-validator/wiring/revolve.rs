//! One pipeline run and its next action. A source reset reopens from
//! committed state. A refused replay can stage a peer checkpoint and park
//! stale state only after every pipeline handle closes. Startup adopts
//! the checkpoint through the same path as a fresh process. A proven
//! divergence holds for the operator and never takes a recovery shortcut.

use std::ops::ControlFlow;

use anyhow::{Context, Result};
use kardamom_engine::ExecutorError;

use super::opening::{ActivePipeline, OpenMode, Opening};

use super::run::{EngineOutcome, RunEnd};
use super::startup::Boot;

/// What the process does after one revolution.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The engine returned cleanly: the operator asked for shutdown.
    Done,
    /// A peer checkpoint is staged and the stale state parked: run the
    /// pipeline again, which adopts it.
    Revolve,
    /// Reopen from the committed state after a stream-source reset.
    Restart,
    /// A proven divergence, with its reason: the process holds, halted,
    /// until an operator clears it, then runs the pipeline again from
    /// its cursor.
    Halt(String),
    /// Leave the process with this status: 1, an availability failure
    /// the process cannot repair, for the orchestrator to restart.
    Exit(i32),
}

/// The verdict for one outcome and the repair's result. `repaired` is the
/// resync outcome label the fallback returned, or `None` when the outcome
/// was not a refused replay, or the fallback is not configured.
pub(crate) fn verdict(outcome: &EngineOutcome, repaired: Option<&str>) -> Verdict {
    match outcome {
        EngineOutcome::Clean => Verdict::Done,
        // A proven divergence is a state of the chain, not a property of
        // one process, so it halts the process instead of ending it: an
        // exit loses the reason and invites a restart against the same
        // fault. Any other engine failure is an availability problem,
        // not an integrity one, and must not look like one.
        EngineOutcome::Diverged(reason) => Verdict::Halt(reason.clone()),
        EngineOutcome::Failed(ExecutorError::SourceRestarted { .. }) if repaired.is_none() => {
            Verdict::Restart
        }
        EngineOutcome::Failed(_) | EngineOutcome::Panicked => match repaired {
            Some("peer-checkpoint") => Verdict::Revolve,
            _ => Verdict::Exit(1),
        },
    }
}

/// One revolution: build the pipeline over `boot`, run the engine to its
/// end, repair a refused replay, and return the verdict. The run future
/// carries every open handle by value, so it lives on the heap instead of
/// the main task's stack. Every handle, the interop feed server's clone
/// of the state env included, is gone before the repair parks the state.
///
/// # Errors
///
/// Returns an error if a startup step fails, or a staged checkpoint
/// fails to park.
async fn revolution(boot: &Boot, mode: OpenMode) -> Result<Verdict> {
    let opening = Opening::new(mode, boot.stop.clone());
    let Some(ready) = opening.ready(boot).await? else {
        return Ok(Verdict::Done);
    };
    let end = {
        let _active = ActivePipeline::new();
        Box::pin(ready.run()).await?
    };
    persist_divergence(boot, &end.outcome);
    let repaired = repair(boot, &end)?;
    Ok(verdict(&end.outcome, repaired))
}

/// Record a proven divergence beside the state, so the next start runs
/// halted. A failed write is logged, not fatal: the exit status still
/// carries the verdict, and the log line stays.
fn persist_divergence(boot: &Boot, outcome: &EngineOutcome) {
    let EngineOutcome::Diverged(reason) = outcome else {
        return;
    };
    kardamom_validator::metrics::set_verdict_standing(true);
    let file = boot.verdict_file();
    match file.record(reason) {
        Ok(()) => tracing::error!(path = %file.path().display(), "divergence verdict recorded"),
        Err(e) => tracing::error!(
            error = %e,
            path = %file.path().display(),
            "divergence verdict could not be recorded"
        ),
    }
}

/// The repair step of a failed revolution: the peer-checkpoint fallback
/// for a refused replay.
fn repair(boot: &Boot, end: &RunEnd) -> Result<Option<&'static str>> {
    let cause = match &end.outcome {
        EngineOutcome::Failed(e) => Some(e),
        EngineOutcome::Panicked => None,
        EngineOutcome::Clean | EngineOutcome::Diverged(_) => return Ok(None),
    };
    let args = &boot.args;
    crate::adoption::resync_after_engine_error(
        cause,
        args.checkpoint_dir.as_deref(),
        &args.checkpoint_peers,
        &args.state_dir,
        end.expected_genesis,
    )
}

/// The process owns its revolution count and the mode of its next open.
#[derive(Default)]
pub(crate) struct Process {
    revolutions: u32,
    opening: OpenMode,
}

impl Process {
    /// One pipeline run and its verdict. Startup errors retain their result
    /// on a normal open; source resets reopen from committed state.
    pub(crate) async fn turn(&mut self, boot: &Boot) -> Result<ControlFlow<()>> {
        if super::halted::Halted::hold(boot).await?.is_break() {
            return Ok(ControlFlow::Break(()));
        }
        match revolution(boot, self.opening).await? {
            Verdict::Done => return Ok(ControlFlow::Break(())),
            Verdict::Exit(status) => exit(status),
            Verdict::Halt(reason) => {
                self.opening = OpenMode::Normal;
                if super::halted::Halted::of(boot)
                    .hold_on(boot, reason)
                    .await
                    .is_break()
                {
                    return Ok(ControlFlow::Break(()));
                }
                self.advance()?;
                tracing::info!(
                    revolutions = self.revolutions,
                    "divergence halt cleared; the pipeline starts again from its cursor"
                );
                return Ok(ControlFlow::Continue(()));
            }
            Verdict::Restart => {
                self.opening = OpenMode::Resume;
                self.advance()?;
                tracing::warn!(
                    revolutions = self.revolutions,
                    "the stream source restarted; reopening the validator from committed state"
                );
                return Ok(Opening::new(self.opening, boot.stop.clone()).wait().await);
            }
            Verdict::Revolve => self.opening = OpenMode::Normal,
        }
        if boot.stop.is_cancelled() {
            tracing::info!("shutdown signal received during the resync repair; not revolving");
            return Ok(ControlFlow::Break(()));
        }
        self.advance()?;
        tracing::info!(
            revolutions = self.revolutions,
            "resync: the pipeline starts again in-process and adopts the staged peer checkpoint"
        );
        Ok(ControlFlow::Continue(()))
    }

    fn advance(&mut self) -> Result<()> {
        self.revolutions = self
            .revolutions
            .checked_add(1)
            .context("validator revolution count overflows")?;
        Ok(())
    }
}

/// Leave the process with status 1: an availability problem, for the
/// orchestrator to restart.
fn exit(status: i32) -> ! {
    tracing::error!(
        "validator halted on an engine error (NOT a proven divergence); if the \
         cluster refused replay and no peer checkpoint qualified, rebuild state \
         via kardamom-reconstruct or restore a checkpoint"
    );
    std::process::exit(status);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_source_reset_resumes_without_a_checkpoint_but_does_not_bypass_a_repair() {
        let reset = EngineOutcome::Failed(ExecutorError::SourceRestarted {
            next_index: 28978,
            next_block: 471,
        });
        assert_eq!(verdict(&reset, None), Verdict::Restart);
        assert_eq!(verdict(&reset, Some("peer-checkpoint")), Verdict::Revolve);
        assert_eq!(verdict(&reset, Some("unrecoverable")), Verdict::Exit(1));
    }

    #[test]
    fn verdict_revolves_only_on_a_staged_checkpoint() {
        let refused = EngineOutcome::Failed(ExecutorError::ClusterReplayUnavailable {
            from_index: 10,
            oldest_index: 500,
            oldest_block: 7,
        });
        assert_eq!(verdict(&refused, Some("peer-checkpoint")), Verdict::Revolve);
        assert_eq!(verdict(&refused, Some("unrecoverable")), Verdict::Exit(1));
        assert_eq!(verdict(&refused, None), Verdict::Exit(1));
        assert_eq!(
            verdict(&EngineOutcome::Panicked, Some("peer-checkpoint")),
            Verdict::Revolve
        );
        assert_eq!(verdict(&EngineOutcome::Panicked, None), Verdict::Exit(1));
        assert_eq!(
            verdict(
                &EngineOutcome::Diverged("mismatch".into()),
                Some("peer-checkpoint")
            ),
            Verdict::Halt("mismatch".into())
        );
        assert_eq!(verdict(&EngineOutcome::Clean, None), Verdict::Done);
    }
}
