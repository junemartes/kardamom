//! Pins the `L2ToL1MessagePasser` predeploy in `chains/dev-withdrawals.toml`:
//!   * exists at the canonical message-passer address,
//!   * carries runtime bytecode byte-equal to the forge-compiled artifact.
//!
//! Regression target: editing the contract without regenerating the genesis
//! `code` (or vice versa) breaks this test before it breaks a dev chain.

use std::path::PathBuf;

use alloy_primitives::{Bytes, hex};
use kardamom_types::Genesis;
use kardamom_types::withdrawals::MESSAGE_PASSER;

#[test]
fn dev_withdrawals_genesis_predeploys_message_passer_with_artifact_bytecode() {
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));

    // Genesis side.
    let toml = workspace.join("chains/dev-withdrawals.toml");
    let raw =
        std::fs::read_to_string(&toml).unwrap_or_else(|e| panic!("read {}: {e}", toml.display()));
    let genesis: Genesis = toml::from_str(&raw).expect("dev-withdrawals.toml parses");
    genesis.validate().expect("dev-withdrawals.toml validates");
    let entry = genesis
        .alloc
        .iter()
        .find(|e| e.address == MESSAGE_PASSER)
        .unwrap_or_else(|| panic!("no alloc for message passer {MESSAGE_PASSER}"));

    // Artifact side: forge's deployedBytecode is the runtime code a predeploy seeds.
    let artifact = workspace.join("contracts/out/L2ToL1MessagePasser.sol/L2ToL1MessagePasser.json");
    let Ok(raw_artifact) = std::fs::read_to_string(&artifact) else {
        eprintln!("SKIP: {} not built (run forge build)", artifact.display());
        return;
    };
    let v: serde_json::Value = serde_json::from_str(&raw_artifact).expect("artifact json");
    let hex_str = v["deployedBytecode"]["object"]
        .as_str()
        .expect("deployedBytecode.object");
    let expected = Bytes::from(hex::decode(hex_str.trim_start_matches("0x")).expect("hex"));

    assert_eq!(
        entry.code.as_ref(),
        Some(&expected),
        "chains/dev-withdrawals.toml message-passer bytecode is stale; regenerate with \
         `forge inspect --root contracts L2ToL1MessagePasser deployedBytecode`",
    );
}
