//! Rebuild-from-L1 parity: the state at the validator's drained head,
//! derived from L1 and the DA layer alone, carries the validator's
//! committed state root. This is the bottom-of-the-stack backstop: it
//! is what an operator runs after every in-cluster copy is gone.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use alloy_primitives::B256;
use anyhow::Context;
use tokio::process::Command;

use crate::harness::Harness;
use crate::nomad::Streams;
use crate::poll::{self, Budget, Outcome};
use crate::stages::first_matching_lines;

/// The DA proxy's API port on the aux node (nomad/da-proxy.nomad.hcl):
/// every posted payload is read back from it by certificate.
const DA_PROXY_PORT: u16 = 3100;

/// The batcher's first failures shown when the posted batches never
/// reach the target block.
const BATCHER_FIRST_FAILURES: usize = 20;

/// What marks a failure line in the batcher log: the tracing levels,
/// the `Error:` line the process prints when it exits, and a thread's
/// panic line.
const BATCHER_FAILURE_MARKERS: &[&str] = &["WARN", "ERROR", "Error:", "panicked"];

/// The genesis the cluster's services start from, in the checkout.
const GENESIS: &str = "deploy/cluster/config/genesis/dev.toml";

/// What the tool prints when L1 does not cover the target block yet: the
/// posted batches end before it, or no batch is posted. Every other
/// failure is final for the L1 record it read.
const NOT_POSTED_YET: &[&str] = &["posted batches end at block", "no BatchPosted events found"];

/// How one run of the tool ended.
#[derive(Debug)]
enum Attempt {
    Rebuilt(Rebuilt),
    /// L1 does not cover the target block yet. The batcher posts it
    /// within a few seconds of its seal, so a later attempt can pass.
    NotPosted(String),
    /// The tool refused the posted record: a block that needs more slots
    /// than its canonical range holds, a root mismatch, a payload that
    /// does not decode. L1 and the DA layer hold the same record on the
    /// next attempt, so the answer does not change.
    Refused(String),
}

impl Attempt {
    /// Classify a failed run by the tool's report line and its error
    /// chain on stderr.
    fn failed(report: &str, stderr: &str) -> Self {
        let chain = stderr
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" / ");
        let why = format!("{report} {chain}").trim().to_string();
        if NOT_POSTED_YET.iter().any(|marker| stderr.contains(marker)) {
            Self::NotPosted(why)
        } else {
            Self::Refused(why)
        }
    }

    /// The rebuilt state, or `None` to retry with the reason kept in
    /// `last`. A refusal ends the wait at once with the tool's reason.
    fn settle(self, last: &RefCell<String>, block: u64) -> anyhow::Result<Option<Rebuilt>> {
        match self {
            Self::Rebuilt(rebuilt) => Ok(Some(rebuilt)),
            Self::NotPosted(why) => {
                *last.borrow_mut() = why;
                Ok(None)
            }
            Self::Refused(why) => {
                let message = format!(
                    "rebuild-from-l1: the tool refused block {block}, and a retry reads the same record: {why}"
                );
                crate::log(&message);
                Err(crate::chaos_fail!("{message}"))
            }
        }
    }
}

/// What a rebuild must reach: the block, and what the state there must
/// carry when the caller knows it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Target {
    pub(crate) block: u64,
    /// The committed state root at the block. `None` when no stopped
    /// writer gives one, in the middle of a case.
    pub(crate) root: Option<B256>,
    /// The resume cursor at the block: the canonical end index a
    /// consumer's state holds there. `None` when the caller has none.
    pub(crate) end_tx_idx: Option<u64>,
}

/// A rebuilt state: its directory, and the tool's report line.
#[derive(Debug, Clone)]
pub(crate) struct Rebuilt {
    pub(crate) state_dir: PathBuf,
    pub(crate) report: String,
}

impl Rebuilt {
    /// The `end_tx_idx=` field of the report: the rebuilt resume cursor.
    /// `None` when the payload of the last block carried none.
    pub(crate) fn end_tx_idx(&self) -> Option<u64> {
        self.field("end_tx_idx=")
            .and_then(|value| value.parse().ok())
    }

    /// The `state_root=` field of the report.
    pub(crate) fn state_root(&self) -> Option<&str> {
        self.field("state_root=")
    }

    /// The value of the report field that starts with `name`.
    fn field(&self, name: &str) -> Option<&str> {
        self.report
            .split_whitespace()
            .find_map(|field| field.strip_prefix(name))
    }
}

/// What a rebuild leaves in its state directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Output {
    /// A parity check: this host reads the state after the tool exits and
    /// then discards it, so the per-block fdatasync buys nothing. On a CI
    /// disk it turned a one-minute rebuild into nine.
    Check,
    /// The image an executor resumes on: the tool removes the trie, the
    /// hashed mirror and the stored root after its checks. The image
    /// outlives the tool, so it keeps every sync.
    ExecutorImage,
    /// A state that keeps the trie, which a validator resumes on. It
    /// outlives the tool, so it keeps every sync.
    ValidatorState,
}

impl Output {
    /// The tool's flag for this output, if it has one.
    fn flag(&self) -> Option<&'static str> {
        match self {
            Self::Check => Some("--no-sync"),
            Self::ExecutorImage => Some("--executor-image"),
            Self::ValidatorState => None,
        }
    }
}

/// One rebuild-from-L1 run against `target`: read the payloads from the
/// DA proxy, run
/// `kardamom-reconstruct` through the target block, and require its
/// root. The batcher posts the target block within a few seconds of
/// the drain, so the run retries while the batches end before it.
///
/// The run passes no `--lockbox`. The suite never deposits, so the L1
/// epochs add no state, and the e2e scenario
/// `da_parity_batcher_matches_validator` proves the deposit path. With
/// the flag, the tool also refuses a block whose L1 origin step names
/// more epochs than the chain holds. A chain that lost an epoch to an
/// earlier fault, such as a da-watcher restart, has fewer.
pub(crate) struct Rebuild<'a> {
    pub(crate) harness: &'a Harness,
    /// The evidence directory; the DA copy and the rebuilt state land
    /// under it.
    pub(crate) evidence: PathBuf,
    pub(crate) target: Target,
    pub(crate) output: Output,
    /// Where the tool writes the seed a wiped sealer cluster starts
    /// from, when the caller wants one.
    pub(crate) sealer_seed: Option<PathBuf>,
}

impl Rebuild<'_> {
    /// Require the reconstructed root at the target block, and the
    /// validator's resume cursor: the payload carries each block's
    /// canonical end index, so the state rebuilt from L1 must hold the
    /// cursor the live chain holds.
    ///
    /// # Errors
    ///
    /// Returns an error if the reconstruction does not reach the target
    /// block within the budget, or its root or its cursor differs.
    pub(crate) async fn assert_parity(&self) -> anyhow::Result<()> {
        let rebuilt = self.run().await?;
        let (Some(live), rebuilt_end) = (self.target.end_tx_idx, rebuilt.end_tx_idx()) else {
            return Ok(());
        };
        anyhow::ensure!(
            rebuilt_end == Some(live),
            "{}: rebuild-from-l1: the rebuilt resume cursor at block {} is {rebuilt_end:?}, the validator holds {live} — a consumer that resumed on the rebuilt state would skip records or apply them twice",
            crate::FAIL_PREFIX,
            self.target.block
        );
        crate::log(format!(
            "rebuild-from-l1: the rebuilt resume cursor equals the validator's ({live})"
        ));
        Ok(())
    }

    /// Rebuild through the target block, within the budget. The batcher
    /// posts a block within a few seconds of its seal, so the run retries
    /// while the posted batches end before the target. It does not retry
    /// any other failure.
    ///
    /// # Errors
    ///
    /// Returns an error if no attempt reached the target block within the
    /// budget, or the tool refused the posted record, for example because
    /// a known root did not match.
    pub(crate) async fn run(&self) -> anyhow::Result<Rebuilt> {
        crate::log(format!(
            "rebuild-from-l1: target block {} root {}",
            self.target.block,
            self.target
                .root
                .map_or("unknown".to_string(), |root| format!("{root:#x}"))
        ));
        let bin = self.harness.release_binary("kardamom-reconstruct")?;
        let settlement = self.harness.settlement_address().await?;
        let last = RefCell::new(String::new());
        let (bin, settlement, last_ref) = (&bin, &settlement, &last);
        let block = self.target.block;
        let outcome = poll::until(Budget::secs(180, 5), |_| async move {
            self.attempt(bin, settlement).await?.settle(last_ref, block)
        })
        .await?;
        if matches!(outcome, Outcome::TimedOut { .. }) {
            self.log_batcher_first_failures().await;
        }
        let (rebuilt, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "rebuild-from-l1: no reconstruction reached block {} within {}s; last: {}",
                self.target.block,
                t.as_secs(),
                last.borrow().trim()
            )
        })?;
        crate::log(format!(
            "rebuild-from-l1 PASS after {}s: {}",
            elapsed.as_secs(),
            rebuilt.report
        ));
        Ok(rebuilt)
    }

    /// Print the batcher's first warnings and errors. A rebuild that
    /// never reaches the target block means the batcher stopped posting;
    /// its first failure names the cause, and the diagnostics dump shows
    /// only the head and the tail of its log. Best-effort: a failed log
    /// read prints its error and the stage keeps its own failure.
    async fn log_batcher_first_failures(&self) {
        let logs = self.harness.nomad.job_logs("batcher", Streams::Both).await;
        let shown = logs.map_or_else(
            |e| format!("(batcher log read failed: {e:#})"),
            |logs| first_matching_lines(&logs, BATCHER_FAILURE_MARKERS, BATCHER_FIRST_FAILURES),
        );
        crate::log(format!(
            "rebuild-from-l1: the batcher's first warnings and errors:\n{shown}"
        ));
    }

    /// One attempt: a fresh state directory, the payloads read from the
    /// DA proxy. The `Err` is a harness failure.
    async fn attempt(&self, bin: &Path, settlement: &str) -> anyhow::Result<Attempt> {
        let state = tempfile::Builder::new()
            .prefix("rebuild-")
            .tempdir_in(&self.evidence)?
            .keep();
        let out = Command::new(bin)
            .args(["--l1-rpc", &self.harness.l1_rpc()?])
            .args(["--settlement", settlement])
            .args(["--da-proxy", &self.da_proxy()])
            .arg("--chain")
            .arg(self.harness.lifecycle.repo_root().join(GENESIS))
            .arg("--state-dir")
            .arg(&state)
            .args(["--through-block", &self.target.block.to_string()])
            .args(self.optional_args())
            .stdin(Stdio::null())
            .output()
            .await
            .with_context(|| format!("spawn {}", bin.display()))?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let report = stdout
            .lines()
            .rfind(|l| l.starts_with("reconstructed "))
            .unwrap_or("")
            .to_string();
        if out.status.success() {
            return Ok(Attempt::Rebuilt(Rebuilt {
                state_dir: state,
                report,
            }));
        }
        Ok(Attempt::failed(
            &report,
            &String::from_utf8_lossy(&out.stderr),
        ))
    }

    /// The arguments that depend on what the caller knows and wants.
    fn optional_args(&self) -> Vec<String> {
        let root = self
            .target
            .root
            .map(|root| vec!["--expect-root".to_string(), format!("{root:#x}")]);
        let output = self.output.flag().map(|flag| vec![flag.to_string()]);
        let seed = self
            .sealer_seed
            .as_ref()
            .map(|path| vec!["--sealer-seed".to_string(), path.display().to_string()]);
        [root, output, seed]
            .into_iter()
            .flatten()
            .flatten()
            .collect()
    }

    /// The DA proxy on the aux node, beside the batcher.
    fn da_proxy(&self) -> String {
        format!(
            "http://{}:{DA_PROXY_PORT}",
            self.harness.probes.validator.ip
        )
    }
}

#[cfg(test)]
mod tests;
