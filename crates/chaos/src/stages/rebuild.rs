//! Rebuild-from-L1 parity: the state at the validator's drained head,
//! derived from L1 and the DA store alone, carries the validator's
//! committed state root. This is the bottom-of-the-stack backstop: it
//! is what an operator runs after every in-cluster copy is gone.

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use alloy_primitives::B256;
use anyhow::Context;
use tokio::process::Command;

use crate::harness::Harness;
use crate::poll::{self, Budget};

/// The DA blob store on the aux node, where the batcher writes every
/// posted blob.
const DA_STORE: &str = "/opt/kardamom/batcher/da";

/// The genesis the cluster's services start from, in the checkout.
const GENESIS: &str = "deploy/cluster/config/genesis/dev.toml";

/// The report line of a successful attempt, or `None` with the failed
/// attempt's reason kept in `last` for the final message.
fn note_attempt(last: &RefCell<String>, attempt: Result<Rebuilt, String>) -> Option<Rebuilt> {
    match attempt {
        Ok(rebuilt) => Some(rebuilt),
        Err(why) => {
            *last.borrow_mut() = why;
            None
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
        self.report
            .split_whitespace()
            .find_map(|field| field.strip_prefix("end_tx_idx="))
            .and_then(|value| value.parse().ok())
    }
}

/// One rebuild-from-L1 run against `target`: copy the DA store, run
/// `kardamom-reconstruct` through the target block, and require its
/// root. The batcher posts the target block within a few seconds of
/// the drain, so the run retries while the batches end before it.
pub(crate) struct Rebuild<'a> {
    pub(crate) harness: &'a Harness,
    /// The evidence directory; the DA copy and the rebuilt state land
    /// under it.
    pub(crate) evidence: PathBuf,
    pub(crate) target: Target,
    /// Write the image an executor resumes on: the tool removes the
    /// trie, the hashed mirror and the stored root after its checks.
    pub(crate) executor_image: bool,
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
    /// while the posted batches end before the target.
    ///
    /// # Errors
    ///
    /// Returns an error if no attempt reached the target block, or a
    /// known root did not match.
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
        let outcome = poll::until(Budget::secs(180, 5), |_| async move {
            Ok(note_attempt(last_ref, self.attempt(bin, settlement).await?))
        })
        .await?;
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

    /// One attempt: a fresh DA copy and a fresh state directory. The
    /// inner `Err` is the tool's report of an attempt that did not
    /// reach the target or did not match; the outer `Err` is a harness
    /// failure.
    async fn attempt(
        &self,
        bin: &Path,
        settlement: &str,
    ) -> anyhow::Result<Result<Rebuilt, String>> {
        let da = self.copy_da_store().await?;
        let state = tempfile::Builder::new()
            .prefix("rebuild-")
            .tempdir_in(&self.evidence)?
            .keep();
        let out = Command::new(bin)
            .args(["--l1-rpc", &self.harness.l1_rpc()?])
            .args(["--settlement", settlement])
            .arg("--da-store")
            .arg(&da)
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
            return Ok(Ok(Rebuilt {
                state_dir: state,
                report,
            }));
        }
        let stderr = String::from_utf8_lossy(&out.stderr);
        let why = stderr
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("");
        Ok(Err(format!("{report} {why}").trim().to_string()))
    }

    /// The arguments that depend on what the caller knows and wants.
    fn optional_args(&self) -> Vec<String> {
        let root = self
            .target
            .root
            .map(|root| vec!["--expect-root".to_string(), format!("{root:#x}")]);
        let image = self
            .executor_image
            .then(|| vec!["--executor-image".to_string()]);
        root.into_iter().chain(image).flatten().collect()
    }

    /// Copy the DA store off the aux node. A blob file is written once
    /// and never changed, so a copy of the live directory is complete
    /// for every batch posted before the copy.
    async fn copy_da_store(&self) -> anyhow::Result<PathBuf> {
        let destination = self.evidence.join("da");
        std::fs::create_dir_all(&destination)?;
        let node = &self.harness.probes.validator.container;
        self.harness
            .nodes
            .docker_ok(&[
                "cp",
                &format!("{node}:{DA_STORE}/."),
                destination.to_str().context("DA copy path")?,
            ])
            .await?;
        Ok(destination)
    }
}
