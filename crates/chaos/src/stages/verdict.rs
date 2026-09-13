//! The validator verdict, the last gate of every shard: the validator
//! answers, it keeps up with the executor, it verified blocks against the BAL, it counted no divergence
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
const POLL_EVERY: Duration = Duration::from_secs(5);
const TAIL: usize = 60;
const FLIGHT_RECORDER: &str = "for f in /opt/kardamom/state/divergence-*.json; do [ -f \"$f\" ] && { echo \"== $f\"; head -c 4096 \"$f\"; echo; }; done";

impl Harness {
    /// Run the validator verdict.
    ///
    /// # Errors
    ///
    /// Returns an error on any failed check.
    pub async fn validator_verdict(&self) -> anyhow::Result<()> {
        crate::log("validator verdict: sync + keep-up + BAL cross-check (no divergence)");
        self.wait_validator_answers().await?;
        crate::log(format!(
            "validator found on {}",
            self.probes.validator.container
        ));
        let start = self
            .probes
            .val_metric_required(COMMITTED, "committed cursor")
            .await?;
        self.wait_validator_sync(start).await?;
        let body = self
            .probes
            .scrape()
            .fetch(&self.probes.validator_target())
            .await
            .ok_or_else(|| anyhow::anyhow!("validator exporter missing at final verdict"))?;
        let sample = Sample::read(&body)?;
        let verified = sample.verified;
        let checks = sample.checks;
        self.assert_no_divergence_halt().await?;
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

    async fn wait_validator_sync(&self, start: i64) -> anyhow::Result<()> {
        let lag_max = self.knobs.stages.validator_lag_max;
        let budget = Budget::new(self.knobs.stages.validator_sync_timeout, POLL_EVERY);
        let outcome = poll::until(budget, |_| async {
            let executor = self.probes.executor_progress().await;
            let validator = self
                .probes
                .val_metric_required(COMMITTED, "committed cursor")
                .await?;
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

/// All success and failure counters come from one exporter response.
struct Sample {
    verified: i64,
    checks: i64,
}

impl Sample {
    fn read(body: &str) -> anyhow::Result<Self> {
        use crate::metrics;
        anyhow::ensure!(
            metrics::first(body, COMMITTED).is_some_and(|v| v > 0),
            "validator committed cursor missing or zero"
        );
        let verified = metrics::first(body, VERIFIED).unwrap_or(0);
        let checks = metrics::first(body, SHADOW_CHECKS).unwrap_or(0);
        anyhow::ensure!(
            verified > 0 && checks > 0,
            "validator has no BAL verification or trie shadow-check evidence"
        );
        anyhow::ensure!(
            metrics::first(body, DIVERGENCE).unwrap_or(0) == 0,
            "validator counted divergence"
        );
        anyhow::ensure!(
            metrics::first(body, SHADOW_MISMATCH).unwrap_or(0) == 0,
            "validator counted trie mismatch"
        );
        Ok(Self { verified, checks })
    }
}

#[cfg(test)]
mod tests;
