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
fn note_attempt(last: &RefCell<String>, attempt: Result<String, String>) -> Option<String> {
    match attempt {
        Ok(line) => Some(line),
        Err(why) => {
            *last.borrow_mut() = why;
            None
        }
    }
}

/// A committed state root and the block it was committed at.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RootAt {
    pub(crate) block: u64,
    pub(crate) root: B256,
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
    pub(crate) target: RootAt,
}

impl Rebuild<'_> {
    /// Require the reconstructed root at the target block.
    ///
    /// # Errors
    ///
    /// Returns an error if the reconstruction does not reach the target
    /// block within the budget, or its root differs.
    pub(crate) async fn assert_parity(&self) -> anyhow::Result<()> {
        crate::log(format!(
            "rebuild-from-l1: target block {} root {:#x}",
            self.target.block, self.target.root
        ));
        let bin = self.harness.release_binary("kardamom-reconstruct")?;
        let settlement = self.harness.settlement_address().await?;
        let last = RefCell::new(String::new());
        let (bin, settlement, last_ref) = (&bin, &settlement, &last);
        let outcome = poll::until(Budget::secs(180, 5), |_| async move {
            Ok(note_attempt(last_ref, self.attempt(bin, settlement).await?))
        })
        .await?;
        let (line, elapsed) = outcome.or_fail(|t| {
            crate::chaos_fail!(
                "rebuild-from-l1: no reconstruction reached block {} with root {:#x} within {}s; last: {}",
                self.target.block,
                self.target.root,
                t.as_secs(),
                last.borrow().trim()
            )
        })?;
        crate::log(format!(
            "rebuild-from-l1 PASS after {}s: {line}",
            elapsed.as_secs()
        ));
        Ok(())
    }

    /// One attempt: a fresh DA copy and a fresh state directory. The
    /// inner `Err` is the tool's report of an attempt that did not
    /// reach the target or did not match; the outer `Err` is a harness
    /// failure.
    async fn attempt(
        &self,
        bin: &Path,
        settlement: &str,
    ) -> anyhow::Result<Result<String, String>> {
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
            .args(["--expect-root", &format!("{:#x}", self.target.root)])
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
            return Ok(Ok(report));
        }
        let stderr = String::from_utf8_lossy(&out.stderr);
        let why = stderr
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .unwrap_or("");
        Ok(Err(format!("{report} {why}").trim().to_string()))
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
