//! A freeze of a sequencer process: SIGSTOP and SIGCONT, as a container
//! freeze stops and resumes every thread of the process at once.

use std::time::Duration;

use anyhow::{Context, Result};

use super::LocalStack;
use super::proc::Proc;

/// The line of the Aeron fail-fast handler: the process exits on a client
/// error, such as the client timeout that the media driver declares.
const FAIL_FAST: &str = "aeron client error; exiting (fail-fast";

impl LocalStack {
    fn sequencer_proc(&mut self, i: usize) -> Result<&mut Proc> {
        Ok(&mut self
            .sequencers
            .get_mut(i)
            .with_context(|| format!("no sequencer {i}"))?
            .proc)
    }

    /// Stop every thread of sequencer `i` with SIGSTOP.
    ///
    /// # Errors
    /// Returns an error when the stack has no sequencer `i`.
    pub fn freeze_sequencer(&mut self, i: usize) -> Result<()> {
        self.sequencer_proc(i)?.suspend();
        Ok(())
    }

    /// Resume sequencer `i` with SIGCONT.
    ///
    /// # Errors
    /// Returns an error when the stack has no sequencer `i`.
    pub fn thaw_sequencer(&mut self, i: usize) -> Result<()> {
        self.sequencer_proc(i)?.resume();
        Ok(())
    }

    /// Wait until sequencer `i` exits through the Aeron fail-fast handler,
    /// and return its exit code.
    ///
    /// # Errors
    /// Returns an error when the process still runs after `bound`, or when
    /// it exits without the fail-fast line.
    pub fn await_sequencer_fail_fast(&mut self, i: usize, bound: Duration) -> Result<Option<i32>> {
        let proc = self.sequencer_proc(i)?;
        let code = proc
            .wait_exit(bound)
            .with_context(|| format!("sequencer {i} still runs {bound:?} after the thaw"))?;
        let log = std::fs::read_to_string(&proc.log_path).context("read the sequencer log")?;
        anyhow::ensure!(
            log.contains(FAIL_FAST),
            "sequencer {i} exited with {code:?} but not through the client fail-fast handler"
        );
        Ok(code)
    }
}
