//! The halted mode. A standing divergence verdict beside the state means
//! the chain diverged and an operator has not yet looked. The validator
//! then serves its metrics and the verdict, makes no progress, and waits.
//! It leaves the mode when the operator clears the verdict, or on the
//! shutdown signal.

use std::ops::ControlFlow;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use kardamom_validator::verdict::VerdictFile;
use tokio_util::sync::CancellationToken;

use super::startup::Boot;

/// How often the halted validator reads the verdict file.
const VERDICT_POLL: Duration = Duration::from_secs(5);

/// Why the halted wait ended.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HaltedEnd {
    /// The operator cleared the verdict: the validator resumes.
    Cleared,
    /// The shutdown signal arrived: the process ends.
    Stopped,
}

/// The halted wait over one verdict file.
pub(crate) struct Halted {
    file: VerdictFile,
    poll: Duration,
}

impl Halted {
    pub(crate) fn new(file: VerdictFile, poll: Duration) -> Self {
        Self { file, poll }
    }

    /// Hold the process while a verdict stands. `Continue` means the
    /// turn goes on and starts the pipeline; `Break` means the shutdown
    /// signal arrived during the hold.
    ///
    /// # Errors
    ///
    /// Returns an error when the verdict file cannot be read.
    pub(crate) async fn hold(boot: &Boot) -> Result<ControlFlow<()>> {
        let halted = Self::new(boot.verdict_file(), VERDICT_POLL);
        let Some(reason) = halted.file.standing().context("read the verdict file")? else {
            kardamom_validator::metrics::set_verdict_standing(false);
            return Ok(ControlFlow::Continue(()));
        };
        kardamom_validator::metrics::set_verdict_standing(true);
        tracing::error!(
            reason = %reason,
            path = %halted.file.path().display(),
            "validator halted on a standing divergence verdict; clear it to resume"
        );
        match halted.wait(&boot.stop).await {
            HaltedEnd::Stopped => Ok(ControlFlow::Break(())),
            HaltedEnd::Cleared => {
                kardamom_validator::metrics::set_verdict_standing(false);
                tracing::info!("divergence verdict cleared; the validator resumes from its cursor");
                Ok(ControlFlow::Continue(()))
            }
        }
    }

    /// Wait until the verdict is gone or `stop` cancels, whichever comes
    /// first.
    pub(crate) async fn wait(&self, stop: &CancellationToken) -> HaltedEnd {
        tokio::select! {
            () = stop.cancelled() => HaltedEnd::Stopped,
            () = self.until_cleared() => HaltedEnd::Cleared,
        }
    }

    /// Poll the file at the poll interval until it is gone.
    async fn until_cleared(&self) {
        let mut ticks = tokio::time::interval(self.poll);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !self.cleared_after(&mut ticks).await {}
    }

    /// One poll: wait for the tick, then read the file. A read error keeps
    /// the validator halted: an unreadable verdict is not a cleared one.
    async fn cleared_after(&self, ticks: &mut tokio::time::Interval) -> bool {
        ticks.tick().await;
        matches!(self.file.standing(), Ok(None))
    }
}

/// The operator command: remove the verdict beside `state_dir` and report
/// what happened. A halted validator sees the removal on its next poll.
///
/// # Errors
///
/// Returns an error when the file exists and cannot be removed.
pub(crate) fn clear_verdict(state_dir: &Path) -> Result<()> {
    let file = VerdictFile::beside(state_dir);
    let cleared = file
        .clear()
        .with_context(|| format!("clear the verdict at {}", file.path().display()))?;
    if cleared {
        println!(
            "cleared the divergence verdict at {}",
            file.path().display()
        );
    } else {
        println!("no divergence verdict at {}", file.path().display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn halted(dir: &Path) -> Halted {
        Halted::new(VerdictFile::beside(dir), Duration::from_millis(20))
    }

    #[tokio::test]
    async fn a_cleared_verdict_ends_the_wait() {
        let dir = tempfile::tempdir().unwrap();
        let halted = halted(dir.path());
        halted.file.record("mismatch").unwrap();
        let stop = CancellationToken::new();
        let clearer = {
            let file = halted.file.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                file.clear().unwrap();
            })
        };
        assert_eq!(halted.wait(&stop).await, HaltedEnd::Cleared);
        clearer.await.unwrap();
    }

    #[tokio::test]
    async fn the_shutdown_signal_ends_the_wait() {
        let dir = tempfile::tempdir().unwrap();
        let halted = halted(dir.path());
        halted.file.record("mismatch").unwrap();
        let stop = CancellationToken::new();
        let stopper = {
            let stop = stop.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(60)).await;
                stop.cancel();
            })
        };
        assert_eq!(halted.wait(&stop).await, HaltedEnd::Stopped);
        stopper.await.unwrap();
        assert!(halted.file.standing().unwrap().is_some());
    }

    #[test]
    fn clear_verdict_reports_both_cases() {
        let dir = tempfile::tempdir().unwrap();
        clear_verdict(dir.path()).unwrap();
        VerdictFile::beside(dir.path()).record("mismatch").unwrap();
        clear_verdict(dir.path()).unwrap();
        assert!(
            VerdictFile::beside(dir.path())
                .standing()
                .unwrap()
                .is_none()
        );
    }
}
