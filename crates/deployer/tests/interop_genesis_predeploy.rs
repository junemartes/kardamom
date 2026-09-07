//! Pins the interop predeploys in `chains/dev-interop.toml`:
//!   * `Outbox` exists at `kardamom_types::xchain::OUTBOX`,
//!   * `Inbox` exists at `kardamom_types::xchain::INBOX`,
//!   * `CheckpointMarker` exists at `kardamom_types::xchain::CHECKPOINT_MARKER`,
//!   * each carries runtime bytecode byte-equal to its forge-compiled
//!     artifact.
//!
//! Regression target: editing either contract without regenerating the
//! genesis `code` (or vice versa) breaks this test before it breaks a dev
//! chain — the `withdrawals_genesis_predeploy.rs` pattern, times two.

use std::path::{Path, PathBuf};

use alloy_primitives::{Address, Bytes, hex};
use kardamom_types::Genesis;
use kardamom_types::xchain::{CHECKPOINT_MARKER, INBOX, OUTBOX};

/// Runtime bytecode from a forge artifact (`deployedBytecode.object`), or
/// `None` when the artifact has not been built (the suite then skips, like
/// the withdrawals drift-guard).
fn artifact_runtime(workspace: &Path, contract: &str) -> Option<Bytes> {
    let artifact = workspace.join(format!("contracts/out/{contract}.sol/{contract}.json"));
    let Ok(raw) = std::fs::read_to_string(&artifact) else {
        eprintln!("SKIP: {} not built (run forge build)", artifact.display());
        return None;
    };
    let v: serde_json::Value = serde_json::from_str(&raw).expect("artifact json");
    let hex_str = v["deployedBytecode"]["object"]
        .as_str()
        .expect("deployedBytecode.object");
    Some(Bytes::from(
        hex::decode(hex_str.trim_start_matches("0x")).expect("hex"),
    ))
}

fn assert_predeploy(genesis: &Genesis, workspace: &Path, contract: &str, address: Address) {
    let entry = genesis
        .alloc
        .iter()
        .find(|e| e.address == address)
        .unwrap_or_else(|| panic!("no alloc for {contract} predeploy {address}"));
    let Some(expected) = artifact_runtime(workspace, contract) else {
        return;
    };
    assert_eq!(
        entry.code.as_ref(),
        Some(&expected),
        "chains/dev-interop.toml {contract} bytecode is stale; regenerate with \
         `forge inspect --root contracts {contract} deployedBytecode`",
    );
    assert_eq!(
        entry.nonce,
        Some(1),
        "{contract} predeploy must carry nonce 1 (the message-passer convention)"
    );
}

#[test]
fn dev_interop_genesis_predeploys_interop_contracts_with_artifact_bytecode() {
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));

    let toml = workspace.join("chains/dev-interop.toml");
    let raw =
        std::fs::read_to_string(&toml).unwrap_or_else(|e| panic!("read {}: {e}", toml.display()));
    let genesis: Genesis = toml::from_str(&raw).expect("dev-interop.toml parses");
    genesis.validate().expect("dev-interop.toml validates");

    assert_predeploy(&genesis, &workspace, "Outbox", OUTBOX);
    assert_predeploy(&genesis, &workspace, "Inbox", INBOX);
    assert_predeploy(&genesis, &workspace, "CheckpointMarker", CHECKPOINT_MARKER);
}
