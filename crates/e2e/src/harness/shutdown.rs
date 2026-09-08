//! Graceful teardown: stop the pipeline at a common final block, then
//! shut the executor and validator down cleanly for offline inspection.

use std::time::Duration;

use anyhow::Result;

use super::LocalStack;

/// Which services exited on SIGTERM, instead of needing the SIGKILL
/// fallback, during [`LocalStack::shutdown_graceful`]. `validator` is
/// `None` when the stack runs without one.
#[derive(Debug, Default)]
pub(super) struct ShutdownReport {
    executor: bool,
    validator: Option<bool>,
}

impl LocalStack {
    /// Stop the pipeline so both state databases freeze at the same final
    /// block, then shut the executor and validator down cleanly (with
    /// mdbx environments closed) for offline inspection.
    ///
    /// Order matters here, in two ways:
    /// 1. ingress and sequencers die first, so no new transactions enter.
    /// 2. the sealer is `SIGSTOP`ped, not killed. This halts the boundary
    ///    clock (otherwise it keeps stamping empty blocks, and the two
    ///    consumers never settle on a common head), while leaving the
    ///    cluster sessions intact. Killing it instead would wedge
    ///    shutdown: the Rust cluster client treats a vanished sealer as a
    ///    case to reconnect with backoff, and retries forever. So the
    ///    reader never sees the end of the stream, and the process
    ///    ignores SIGTERM until the harness sends SIGKILL.
    /// 3. only then does SIGTERM go to the consumers, which closes their
    ///    mdbx environments.
    ///
    /// # Errors
    /// Returns an error when the pipeline does not settle within 60s, or
    /// when the executor does not exit on SIGTERM within 20s. Also
    /// returns an error when the validator runs and does not exit on
    /// SIGTERM within 20s (the receipts-pump ownership cycle that used to
    /// make it ignore SIGTERM).
    pub async fn shutdown_graceful(&mut self) -> Result<()> {
        self.ingress.proc.terminate(Duration::from_secs(10));
        if let Some(w) = &mut self.da_watcher {
            w.proc.terminate(Duration::from_secs(10));
        }
        for s in &mut self.sequencers {
            s.proc.terminate(Duration::from_secs(10));
        }
        for p in &self.sealer.procs {
            p.suspend();
        }

        self.drain_until_settled().await?;

        // Both consumers are caught up now, so send SIGTERM to them and
        // require a clean exit from each. This also checks for the
        // Aeron-runtime ownership cycle in the receipts pump: a
        // regression there makes the validator sit through SIGTERM
        // instead of exiting, so a 20-second limit catches it.
        // `terminate` sends SIGKILL on overrun, which keeps the databases
        // comparable either way (commits are atomic), but the checks
        // below make a regression loud instead of silent.
        let honored = ShutdownReport {
            executor: self.executor.proc.terminate(Duration::from_secs(20)),
            validator: self
                .validator
                .as_mut()
                .map(|v| v.proc.terminate(Duration::from_secs(20))),
        };
        self.shutdown_report = honored;
        anyhow::ensure!(
            self.shutdown_report.executor,
            "executor did not exit on SIGTERM within 20s"
        );
        anyhow::ensure!(
            self.shutdown_report.validator != Some(false),
            "validator did not exit on SIGTERM within 20s — the Aeron-runtime \
             ownership cycle in the receipts pump is back (see \
             TxReceiptsSubscriberHandle::into_receiver)"
        );
        Ok(())
    }

    /// Drain: wait until both consumers stop advancing and the validator
    /// is no longer behind. [`Self::shutdown_graceful`] calls this after
    /// the producers are gone and the sealer is `SIGSTOP`ped.
    ///
    /// This does not wait for "the two gauges are equal". `EXEC_BLOCK_NUMBER`
    /// is the newest durable block, set in the inflight sweep as the state
    /// writer settles, and commits are pipelined several blocks deep. A
    /// commit settles "at a later boundary's sweep, or at end of stream".
    /// The caller has `SIGSTOP`ped the sealer, so no later boundary is
    /// coming: the last few executed blocks stay unsettled until the
    /// SIGTERM that follows this drain ends the stream. Requiring
    /// equality here would wait for something that cannot happen until
    /// after this loop, so it would always burn the full 60-second
    /// timeout.
    ///
    /// So the validator legitimately sits ahead of the executor's
    /// durability gauge, and it can never run ahead of what the executor
    /// actually executed, since it verifies off that output. "Both
    /// stable, validator >= executor" is therefore the honest settled
    /// condition. The executor flushes its pipeline on the clean exit
    /// that follows, and the offline phase then compares equal final
    /// blocks from the two persisted databases.
    ///
    /// # Errors
    /// Returns an error when a metrics scrape fails, or when the executor
    /// and validator do not settle within 60s.
    async fn drain_until_settled(&self) -> Result<()> {
        let exec_addr = self.executor.metrics_addr;
        let val_addr = self.validator.as_ref().map(|v| v.metrics_addr);
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut last_exec = -1.0f64;
        let mut last_val = -1.0f64;
        loop {
            let exec_block = super::metrics::scrape(exec_addr)
                .await?
                .value(crate::scenarios::EXEC_BLOCK_NUMBER)
                .unwrap_or(0.0);
            let val_block = match val_addr {
                // No validator: treat its half as already settled.
                None => exec_block,
                Some(addr) => super::metrics::scrape(addr)
                    .await?
                    .value(crate::scenarios::VALIDATOR_COMMITTED_BLOCK)
                    .unwrap_or(0.0),
            };
            #[allow(
                clippy::float_cmp,
                reason = "exact equality is the intended check here: both gauges come from \
                           the same metric source, so an unchanged value scrapes back \
                           bit-identical"
            )]
            let stable = exec_block == last_exec && val_block == last_val;
            if stable && val_block >= exec_block {
                // Both stable across one interval, validator not behind.
                return Ok(());
            }
            last_exec = exec_block;
            last_val = val_block;
            anyhow::ensure!(
                std::time::Instant::now() < deadline,
                "drain: executor/validator did not settle in 60s \
                 (executor durable {exec_block}, validator committed {val_block})"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    pub(super) fn dump_tails(&self) {
        eprintln!("=== stack log tails ({}) ===", self.root.path().display());
        let procs: Vec<&super::proc::Proc> = std::iter::once(&self.ingress.proc)
            .chain(std::iter::once(&self.executor.proc))
            .chain(self.validator.as_ref().map(|v| &v.proc))
            .chain(self.da_watcher.as_ref().map(|w| &w.proc))
            .chain(self.sequencers.iter().map(|s| &s.proc))
            .chain(self.sealer.procs.iter())
            .chain(std::iter::once(&self.driver.proc))
            .collect();
        for p in procs {
            eprintln!("--- {} ---\n{}", p.name, p.log_tail(25));
        }
    }
}

impl Drop for LocalStack {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.dump_tails();
        }
        if self.keep || std::thread::panicking() {
            // Keep the temp root for inspection later (an explicit
            // opt-in, or any failing test). `TempDir::keep` consumes the
            // value, so disable cleanup instead.
            let path = self.root.path().to_path_buf();
            eprintln!("keeping stack root at {}", path.display());
            self.root.disable_cleanup(true);
        }
    }
}
