//! Process control on a running stack: freeze, crash, suspend, resume,
//! restart, and liveness probes.

use std::time::Duration;

use anyhow::Result;

use super::{LocalStack, services};

impl LocalStack {
    /// SIGSTOP the sealer, so it stops stamping block boundaries. The
    /// chain settles on one final head and stays there. Scenarios that
    /// must reason about "the current block" with no race (the
    /// bridge-withdrawal test matches a withdrawal to the attested output
    /// for its block) freeze the clock first. This suspends the sealer
    /// instead of killing it. Killing it would make the consumers'
    /// cluster clients retry forever and wedge shutdown.
    pub fn freeze_block_clock(&self) {
        for p in &self.sealer.procs {
            p.suspend();
        }
    }

    /// SIGKILL the executor: an unclean crash, with no shutdown hooks and
    /// no final flush. What survives is exactly what mdbx committed.
    pub fn crash_executor(&mut self) {
        self.executor.proc.kill();
    }

    /// Liveness probe for the validator process, for failure diagnostics.
    /// The bridge-withdrawal-test mdbx-read-only failure family depends on
    /// whether the validator was already dead at probe time (an
    /// Aeron driver-timeout abort leaves the environment unsteadily
    /// closed). `None` when the stack runs no validator.
    pub fn validator_alive(&mut self) -> Option<bool> {
        self.validator.as_mut().map(|v| v.proc.is_alive())
    }

    /// SIGSTOP the DA watcher, so no honest epoch competes with an
    /// injected one. This is the same determinism trick that
    /// `suspend_executor` gives the BAL drill. Returns false when the
    /// stack has no watcher (no L1).
    #[must_use]
    pub fn suspend_da_watcher(&self) -> bool {
        match &self.da_watcher {
            Some(w) => {
                w.proc.suspend();
                true
            }
            None => false,
        }
    }

    /// Freeze the executor (SIGSTOP). Its genuine BAL and receipt
    /// publications stop, so an injected corrupt frame faces no
    /// competition (the divergence-detection test).
    pub fn suspend_executor(&self) {
        self.executor.proc.suspend();
    }

    pub fn resume_executor(&self) {
        self.executor.proc.resume();
    }

    /// Restart the executor against the same state directory and metrics
    /// port. This makes it resume from its persisted cursor, instead of
    /// re-syncing from genesis, and lets scenarios keep the address they
    /// already hold.
    ///
    /// # Errors
    /// Returns an error when the executor fails to spawn.
    pub fn restart_executor(&mut self) -> Result<()> {
        let port = self.executor.metrics_addr.port();
        let respawned = services::spawn_executor_at(&self.service_spec(), Some(port))?;
        self.executor = respawned;
        Ok(())
    }

    /// Wait for the validator process to exit on its own. Returns its
    /// exit code (exit code 2 is the divergence fail-stop).
    pub fn wait_validator_exit(&mut self, timeout: Duration) -> Option<Option<i32>> {
        self.validator.as_mut()?.proc.wait_exit(timeout)
    }

    #[must_use]
    pub fn validator_log(&self) -> Option<String> {
        let v = self.validator.as_ref()?;
        std::fs::read_to_string(&v.proc.log_path).ok()
    }
}
