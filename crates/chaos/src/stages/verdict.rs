//! The validator verdict, the last gate of every shard: the validator
//! answers, it keeps up with the executor (or at least progresses after
//! chaos), it verified blocks against the BAL, it counted no divergence
//! and halted on none, and the incremental trie matched the rebuild.

use std::time::Duration;

use crate::evidence;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget, Outcome};
use crate::probes::VALIDATOR_PORT;
use crate::stages::tail_lines;

const COMMITTED: &str = "validator_committed_block";
const VERIFIED: &str = "validator_blocks_verified_total";
const DIVERGENCE: &str = "validator_divergence_total";
const SHADOW_CHECKS: &str = "kardamom_state_trie_shadow_checks_total";
const SHADOW_MISMATCH: &str = "kardamom_state_trie_shadow_mismatch_total";
/// The validator must answer its exporter within two minutes.
const FOUND_SECS: u64 = 120;
/// After chaos, the validator must commit past its start within a minute.
const PROGRESS_SECS: u64 = 60;
const POLL_EVERY: Duration = Duration::from_secs(5);
const DIVERGENCE_TRIES: u32 = 5;
const DIVERGENCE_RETRY: Duration = Duration::from_secs(3);
const TAIL: usize = 60;
const FLIGHT_RECORDER: &str = "for f in /opt/kardamom/state/divergence-*.json; do [ -f \"$f\" ] && { echo \"== $f\"; head -c 4096 \"$f\"; echo; }; done";

/// What the verdict expects of the validator's position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictMode {
    /// After load or semantics: the validator is within the lag bound of
    /// the executor.
    Sync,
    /// After chaos: the validator committed past where it started; the
    /// load shard asserts the bounded lag.
    Progress,
}

impl Harness {
    /// Run the validator verdict.
    ///
    /// # Errors
    ///
    /// Returns an error on any failed check.
    pub async fn validator_verdict(&self, mode: VerdictMode) -> anyhow::Result<()> {
        crate::log("validator verdict: sync + keep-up + BAL cross-check (no divergence)");
        self.wait_validator_answers().await?;
        crate::log(format!(
            "validator found on {}",
            self.probes.validator.container
        ));
        let start = self.val_or_zero(COMMITTED).await;
        match mode {
            VerdictMode::Sync => self.wait_validator_sync(start).await?,
            VerdictMode::Progress => self.wait_validator_progress(start).await?,
        }
        let verified = self.val_or_zero(VERIFIED).await;
        anyhow::ensure!(
            verified > 0,
            "validator verified 0 blocks against the BAL (tx_bal not flowing?)"
        );
        let diverged = self.divergence_count().await?;
        anyhow::ensure!(diverged == 0, "validator counted {diverged} divergence(s)");
        self.assert_no_divergence_halt().await?;
        let checks = self.val_or_zero(SHADOW_CHECKS).await;
        anyhow::ensure!(checks > 0, "trie shadow-check never ran (checks={checks})");
        let mismatch = self.val_or_zero(SHADOW_MISMATCH).await;
        anyhow::ensure!(
            mismatch == 0,
            "incremental state trie diverged from full rebuild ({mismatch} mismatches)"
        );
        crate::log(format!(
            "validator verdict PASSED: {verified} blocks BAL-verified, 0 divergences, {checks} shadow-checks / 0 mismatches"
        ));
        Ok(())
    }

    async fn wait_validator_answers(&self) -> anyhow::Result<()> {
        let target = self.probes.validator_target();
        let outcome = poll::until(Budget::secs(FOUND_SECS, 5), |_| async {
            Ok(self.probes.scrape().answers(&target).await.then_some(()))
        })
        .await?;
        outcome.or_fail(|t| {
            anyhow::anyhow!(
                "no validator /metrics on :{VALIDATOR_PORT} after {}s (fail-stopped: divergence or session death?)",
                t.as_secs()
            )
        })?;
        Ok(())
    }

    async fn val_or_zero(&self, metric: &str) -> i64 {
        self.probes.val_metric(metric).await.unwrap_or(0)
    }

    async fn wait_validator_sync(&self, start: i64) -> anyhow::Result<()> {
        let lag_max = self.knobs.stages.validator_lag_max;
        let budget = Budget::new(self.knobs.stages.validator_sync_timeout, POLL_EVERY);
        let outcome = poll::until(budget, |_| async {
            let executor = self.probes.executor_progress().await;
            let validator = self.val_or_zero(COMMITTED).await;
            Ok(executor
                .filter(|e| validator > 0 && e.saturating_sub(validator) <= lag_max)
                .map(|e| (e, validator)))
        })
        .await?;
        let Outcome::Ready {
            value: (executor, validator),
            ..
        } = outcome
        else {
            self.dump_validator_tails().await;
            anyhow::bail!(
                "validator did not sync within {}s (started at {start})",
                budget.timeout.as_secs()
            );
        };
        crate::log(format!(
            "validator synced: block {validator} vs executor {executor} (lag {} <= {lag_max})",
            executor.saturating_sub(validator)
        ));
        Ok(())
    }

    async fn wait_validator_progress(&self, start: i64) -> anyhow::Result<()> {
        let outcome = poll::until(Budget::secs(PROGRESS_SECS, 5), |_| async {
            let v = self.val_or_zero(COMMITTED).await;
            Ok((v > start).then_some(v))
        })
        .await?;
        let (now, _) = outcome.or_fail(|_| {
            anyhow::anyhow!("validator made no progress after chaos (stuck at {start})")
        })?;
        crate::log(format!(
            "chaos shard: validator progressing ({start} -> {now}); bounded lag asserted on the load shard"
        ));
        Ok(())
    }

    /// The divergence counter. It does not exist before the first
    /// divergence, so an absent counter on a validator that answers its
    /// committed block is zero.
    async fn divergence_count(&self) -> anyhow::Result<i64> {
        for _ in 0..DIVERGENCE_TRIES {
            if let Some(count) = self.probes.val_metric(DIVERGENCE).await {
                return Ok(count);
            }
            if self.probes.val_metric(COMMITTED).await.is_some() {
                return Ok(0);
            }
            tokio::time::sleep(DIVERGENCE_RETRY).await;
        }
        anyhow::bail!(
            "validator exporter unscrapeable after {DIVERGENCE_TRIES} tries; cannot assert 0 divergences"
        )
    }

    async fn assert_no_divergence_halt(&self) -> anyhow::Result<()> {
        let Some(hit) = self.evidence.divergence_scan().await? else {
            return Ok(());
        };
        evidence::dump_divergence(&hit);
        self.dump_flight_recorder().await;
        anyhow::bail!(
            "validator halted on divergence (found in alloc {} log)",
            hit.alloc
        )
    }

    async fn dump_flight_recorder(&self) {
        eprintln!("----- flight-recorder dumps on validator node (if any) -----");
        if let Ok(out) = self
            .nodes
            .exec(&self.probes.validator.container, FLIGHT_RECORDER)
            .await
        {
            eprintln!("{out}");
        }
    }

    async fn dump_validator_tails(&self) {
        eprintln!("----- validator alloc log tails -----");
        let Ok(allocs) = self.nomad.allocations("validator").await else {
            return;
        };
        for alloc in &allocs {
            let logs = self
                .nomad
                .alloc_logs(alloc, Streams::Both)
                .await
                .unwrap_or_default();
            eprintln!("== alloc {} (tail {TAIL})", alloc.short_id());
            eprintln!("{}", tail_lines(&logs, TAIL));
        }
    }
}
