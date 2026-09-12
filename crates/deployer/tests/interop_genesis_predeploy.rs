//! Pins the interop predeploys in `chains/dev-interop.toml`:
//!   * `Outbox` exists at `kardamom_types::xchain::OUTBOX`,
//!   * `Inbox` exists at `kardamom_types::xchain::INBOX`,
//!   * each carries runtime bytecode byte-equal to its forge-compiled
//!     artifact.
//!
//! Regression target: editing either contract without regenerating the
//! genesis `code` (or vice versa) breaks this test before it breaks a dev
//! chain — the `withdrawals_genesis_predeploy.rs` pattern, times two.

use std::path::{Path, PathBuf};

use alloy_primitives::{Address, Bytes, hex};
use anyhow::{Context, Result};
use kardamom_types::Genesis;
use kardamom_types::xchain::{INBOX, OUTBOX};

/// Whether a missing forge artifact fails the test or just skips it. CI
/// always builds the artifact (the deployer build script runs
/// `forge build`), so a missing artifact there means the guard checks
/// nothing, and a silent pass would hide bytecode drift. A local run before
/// `forge build` skips instead, like the withdrawals drift-guard.
#[derive(Clone, Copy)]
enum Mode {
    RequireArtifact,
    SkipIfAbsent,
}

impl Mode {
    fn from_env() -> Self {
        if std::env::var_os("CI").is_some() {
            Self::RequireArtifact
        } else {
            Self::SkipIfAbsent
        }
    }

    /// Runtime bytecode from a forge artifact (`deployedBytecode.object`).
    /// `Ok(None)` means the artifact is absent and `self` allows a skip.
    ///
    /// # Errors
    /// Returns an error when the artifact is absent and `self` is
    /// `Mode::RequireArtifact`, or when the artifact's JSON is malformed.
    fn artifact_runtime(self, workspace: &Path, contract: &str) -> Result<Option<Bytes>> {
        let artifact = workspace.join(format!("contracts/out/{contract}.sol/{contract}.json"));
        let Ok(raw) = std::fs::read_to_string(&artifact) else {
            return self.handle_missing_artifact(&artifact);
        };
        let v: serde_json::Value =
            serde_json::from_str(&raw).with_context(|| format!("parse {}", artifact.display()))?;
        let hex_str = v["deployedBytecode"]["object"]
            .as_str()
            .with_context(|| format!("{}: no deployedBytecode.object", artifact.display()))?;
        let bytes = hex::decode(hex_str.trim_start_matches("0x")).with_context(|| {
            format!("{}: deployedBytecode.object is not hex", artifact.display())
        })?;
        Ok(Some(Bytes::from(bytes)))
    }

    /// What to do when `artifact` does not exist: bail under
    /// `RequireArtifact`, or skip (return `Ok(None)`) under
    /// `SkipIfAbsent`.
    fn handle_missing_artifact(self, artifact: &Path) -> Result<Option<Bytes>> {
        match self {
            Mode::RequireArtifact => anyhow::bail!(
                "{} not built on CI; the drift guard must not pass without the artifact",
                artifact.display()
            ),
            Mode::SkipIfAbsent => {
                eprintln!("SKIP: {} not built (run forge build)", artifact.display());
                Ok(None)
            }
        }
    }

    fn assert_predeploy(
        self,
        genesis: &Genesis,
        workspace: &Path,
        contract: &str,
        address: Address,
    ) -> Result<()> {
        let entry = genesis
            .alloc
            .iter()
            .find(|e| e.address == address)
            .with_context(|| format!("no alloc for {contract} predeploy {address}"))?;
        let Some(expected) = self.artifact_runtime(workspace, contract)? else {
            return Ok(());
        };
        anyhow::ensure!(
            entry.code.as_ref() == Some(&expected),
            "chains/dev-interop.toml {contract} bytecode is stale; regenerate with \
             `forge inspect --root contracts {contract} deployedBytecode`",
        );
        anyhow::ensure!(
            entry.nonce == Some(1),
            "{contract} predeploy must carry nonce 1 (the message-passer convention)"
        );
        Ok(())
    }
}

#[test]
fn dev_interop_genesis_predeploys_outbox_and_inbox_with_artifact_bytecode() -> Result<()> {
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
    let mode = Mode::from_env();

    let toml = workspace.join("chains/dev-interop.toml");
    let raw = std::fs::read_to_string(&toml).with_context(|| format!("read {}", toml.display()))?;
    let genesis: Genesis = toml::from_str(&raw).context("dev-interop.toml parses")?;
    genesis.validate().context("dev-interop.toml validates")?;

    mode.assert_predeploy(&genesis, &workspace, "Outbox", OUTBOX)?;
    mode.assert_predeploy(&genesis, &workspace, "Inbox", INBOX)?;
    Ok(())
}
