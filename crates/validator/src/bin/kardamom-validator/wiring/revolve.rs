//! The revolution loop: one pass of the pipeline, from the stream plane
//! to the engine's end, and the verdict on what comes next.
//!
//! A refused replay (`REPLAY_UNAVAILABLE`: the cursor fell below the
//! cluster's retention floor) used to end the process. The repair, a
//! peer checkpoint fetched and the stale state parked, ran at exit, and
//! Nomad's restart adopted it. One revolution then cost the fetch, the
//! restart delay, the adoption, and the age of the checkpoint, and every
//! lost race against the retention window burned a restart attempt
//! (issue #298). The repair now runs in-process: the pipeline is torn
//! down through its normal shutdown, the checkpoint is fetched and the
//! state parked, and the next revolution re-enters the one startup path,
//! which adopts the checkpoint as a fresh start would. The fetch is still
//! an atomic rename, and the park still happens only after a qualifying
//! checkpoint is on disk, so a crash at any point lands in a state the
//! startup path already handles.

use std::ops::ControlFlow;

use anyhow::Result;

use super::run::{EngineOutcome, RunEnd};
use super::startup::{Boot, Startup};

/// What the process does after one revolution.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// The engine returned cleanly: the operator asked for shutdown.
    Done,
    /// A peer checkpoint is staged and the stale state parked: run the
    /// pipeline again, which adopts it.
    Revolve,
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
pub(crate) async fn revolution(boot: &Boot) -> Result<Verdict> {
    let ready = Startup::from_boot(boot)?
        .open_state()?
        .open_streams()
        .await?
        .spawn_pumps()?
        .spawn_writer()?
        .spawn_attester()?
        .build_sink();
    let end = Box::pin(ready.run()).await?;
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

/// One turn of the process loop: the hold on a standing verdict, a
/// revolution, then its verdict. `Continue` means run again; `Break`
/// means the process is done. An exit status leaves the process here.
///
/// # Errors
///
/// Returns the revolution's error.
pub(crate) async fn turn(boot: &Boot, revolutions: &mut u32) -> Result<ControlFlow<()>> {
    if super::halted::Halted::hold(boot).await?.is_break() {
        return Ok(ControlFlow::Break(()));
    }
    match revolution(boot).await? {
        Verdict::Done => return Ok(ControlFlow::Break(())),
        Verdict::Exit(status) => exit(status),
        Verdict::Halt(reason) => {
            if super::halted::Halted::of(boot)
                .hold_on(boot, reason)
                .await
                .is_break()
            {
                return Ok(ControlFlow::Break(()));
            }
            *revolutions += 1;
            tracing::info!(
                revolutions,
                "divergence halt cleared; the pipeline starts again from its cursor"
            );
            return Ok(ControlFlow::Continue(()));
        }
        Verdict::Revolve => (),
    }
    if boot.stop.is_cancelled() {
        tracing::info!("shutdown signal received during the resync repair; not revolving");
        return Ok(ControlFlow::Break(()));
    }
    *revolutions += 1;
    tracing::info!(
        revolutions,
        "resync: the pipeline starts again in-process and adopts the staged peer checkpoint"
    );
    Ok(ControlFlow::Continue(()))
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
    use kardamom_engine::ExecutorError;

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
