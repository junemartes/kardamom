//! The validator verdict, the last gate of every shard: the validator
//! answers, it keeps up with the executor, it verified blocks against the BAL, it counted no divergence
//! and halted on none, and the incremental trie matched the rebuild.

use std::cell::RefCell;
use std::time::Duration;

use crate::evidence;
use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget, Outcome};
use crate::probes::VALIDATOR_PORT;
use crate::stages::{matching_lines, tail_lines};

const COMMITTED: &str = "validator_committed_block";
const VERIFIED: &str = "validator_blocks_verified_total";
const DIVERGENCE: &str = "validator_divergence_total";
const SHADOW_CHECKS: &str = "kardamom_state_trie_shadow_checks_total";
const SHADOW_MISMATCH: &str = "kardamom_state_trie_shadow_mismatch_total";
const BAL_MISSING: &str = "validator_bal_missing_total";
const RECEIPT_MISSING: &str = "validator_receipt_missing_total";
/// The counters that tell a stalled validator from a slow one: a cursor
/// that does not move, and the waits that timed out with no replica data.
const STALL_COUNTERS: [&str; 7] = [
    COMMITTED,
    VERIFIED,
    SHADOW_CHECKS,
    DIVERGENCE,
    SHADOW_MISMATCH,
    BAL_MISSING,
    RECEIPT_MISSING,
];
/// The validator must answer its exporter within two minutes.
const FOUND_SECS: u64 = 120;
const POLL_EVERY: Duration = Duration::from_secs(5);
const TAIL: usize = 60;
/// The tail after a restart holds only startup lines, so the dump also
/// shows the last warnings and errors of the whole log.
const WARN_MARKERS: &[&str] = &[" WARN", " ERROR"];
const WARNINGS: usize = 60;
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
        let sample = self.wait_validator_sync().await?;
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

    async fn wait_validator_sync(&self) -> anyhow::Result<Sample> {
        let lag_max = self.knobs.stages.validator_lag_max;
        let budget = Budget::new(self.knobs.stages.validator_sync_timeout, POLL_EVERY);
        let last = RefCell::new(String::new());
        let outcome = poll::until(budget, |_| async {
            let executor = self.probes.executor_progress().await;
            let Some(body) = self
                .probes
                .scrape()
                .fetch(&self.probes.validator_target())
                .await
            else {
                *last.borrow_mut() = format!("executor={executor:?}, validator exporter missing");
                return Ok(None);
            };
            *last.borrow_mut() = format!(
                "executor={executor:?}, validator={:?}",
                crate::metrics::samples_of_interest(&body, &STALL_COUNTERS)
            );
            Ok(Sample::read(&body)?.and_then(|sample| {
                sample
                    .synced_with(executor, lag_max)
                    .map(|executor| (executor, sample))
            }))
        })
        .await?;
        let Outcome::Ready {
            value: (executor, sample),
            ..
        } = outcome
        else {
            self.dump_validator_tails().await;
            anyhow::bail!(
                "validator does not sync with BAL and trie verification within {}s ({})",
                budget.timeout.as_secs(),
                last.borrow()
            );
        };
        crate::log(format!(
            "validator synced: block {} vs executor {executor} (lag {} <= {lag_max})",
            sample.committed,
            executor.saturating_sub(sample.committed)
        ));
        Ok(sample)
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
        eprintln!("----- validator stall counters -----");
        eprintln!(
            "executor progress = {:?}",
            self.probes.executor_progress().await
        );
        for metric in STALL_COUNTERS {
            eprintln!("{metric} = {:?}", self.probes.val_metric(metric).await);
        }
        eprintln!("----- validator alloc log tails -----");
        let Ok(allocs) = self.nomad.allocations_with_logs("validator").await else {
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
            eprintln!("== alloc {} warnings (last {WARNINGS})", alloc.short_id());
            eprintln!("{}", matching_lines(&logs, WARN_MARKERS, WARNINGS));
        }
    }
}

/// All success and failure counters come from one exporter response.
#[derive(Debug)]
struct Sample {
    committed: i64,
    verified: i64,
    checks: i64,
}

impl Sample {
    fn read(body: &str) -> anyhow::Result<Option<Self>> {
        use crate::metrics;
        anyhow::ensure!(
            metrics::first(body, DIVERGENCE).unwrap_or(0) == 0,
            "validator counted divergence"
        );
        anyhow::ensure!(
            metrics::first(body, SHADOW_MISMATCH).unwrap_or(0) == 0,
            "validator counted trie mismatch"
        );
        let committed = metrics::first(body, COMMITTED).unwrap_or(0);
        let verified = metrics::first(body, VERIFIED).unwrap_or(0);
        let checks = metrics::first(body, SHADOW_CHECKS).unwrap_or(0);
        Ok(
            (committed > 0 && verified > 0 && checks > 0).then_some(Self {
                committed,
                verified,
                checks,
            }),
        )
    }

    fn synced_with(&self, executor: Option<i64>, lag_max: i64) -> Option<i64> {
        executor.filter(|e| e.saturating_sub(self.committed) <= lag_max)
    }
}

#[cfg(test)]
mod tests;
