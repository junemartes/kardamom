//! Checks the `KardamomChainState` predeploy in `chains/dev-withdrawals.toml`.
//! The predeploy:
//!   * exists at the canonical chain-state address,
//!   * has runtime bytecode that is byte-equal to the forge-compiled artifact,
//!   * and matches the compiled ABI for the Rust constants the derivation
//!     rule uses (`SYSTEM_UPGRADER`, the `setFeature` selector).
//!
//! The L2 write-authority check lives in Solidity. The sender that satisfies
//! it is minted in Rust. If the two drift apart, every upgrade reverts
//! on-chain without a clear error, and only the flag fails to activate.

use std::path::PathBuf;

use alloy_primitives::{Address, Bytes, hex};
use kardamom_types::Genesis;
use kardamom_types::upgrades::{CHAIN_STATE, SET_FEATURE_SELECTOR, SYSTEM_UPGRADER};

/// Read the forge artifact, or `None` when `contracts/out/` has not been built.
fn artifact() -> Option<serde_json::Value> {
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
    let path = workspace.join("contracts/out/KardamomChainState.sol/KardamomChainState.json");
    let Ok(raw) = std::fs::read_to_string(&path) else {
        eprintln!("SKIP: {} not built (run forge build)", path.display());
        return None;
    };
    Some(serde_json::from_str(&raw).expect("artifact json"))
}

#[test]
fn dev_withdrawals_genesis_predeploys_chain_state_with_artifact_bytecode() {
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));

    let toml = workspace.join("chains/dev-withdrawals.toml");
    let raw =
        std::fs::read_to_string(&toml).unwrap_or_else(|e| panic!("read {}: {e}", toml.display()));
    let genesis: Genesis = toml::from_str(&raw).expect("dev-withdrawals.toml parses");
    genesis.validate().expect("dev-withdrawals.toml validates");
    let entry = genesis
        .alloc
        .iter()
        .find(|e| e.address == CHAIN_STATE)
        .unwrap_or_else(|| panic!("no alloc for chain state {CHAIN_STATE}"));

    let Some(v) = artifact() else { return };
    let hex_str = v["deployedBytecode"]["object"]
        .as_str()
        .expect("deployedBytecode.object");
    let expected = Bytes::from(hex::decode(hex_str.trim_start_matches("0x")).expect("hex"));

    assert_eq!(
        entry.code.as_ref(),
        Some(&expected),
        "chains/dev-withdrawals.toml chain-state bytecode is stale; regenerate with \
         `forge inspect --root contracts KardamomChainState deployedBytecode`",
    );
}

#[test]
fn rust_system_upgrader_matches_the_contracts_constant() {
    let Some(v) = artifact() else { return };
    // The constant is inlined into the runtime bytecode. Checking the
    // compiled bytecode for it is indirect, so check the literal value in
    // the Solidity source instead. The source is the value a reviewer edits.
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
    let src = std::fs::read_to_string(workspace.join("contracts/src/L2/KardamomChainState.sol"))
        .expect("read KardamomChainState.sol");
    // solc requires the EIP-55 checksummed form for an address literal. This
    // is the exact string that must appear in the source.
    let want = SYSTEM_UPGRADER.to_checksum(None);
    assert!(
        src.contains(&want),
        "KardamomChainState.SYSTEM_UPGRADER does not match \
         kardamom_types::upgrades::SYSTEM_UPGRADER ({want}); the L2 write-authority \
         check would reject every upgrade deposit the derivation rule mints",
    );

    // Also check that the address is a real constant in the ABI surface.
    assert!(
        v["abi"]
            .as_array()
            .expect("abi array")
            .iter()
            .any(|e| e["name"] == "SYSTEM_UPGRADER"),
        "SYSTEM_UPGRADER getter missing from the compiled ABI",
    );
}

#[test]
fn rust_set_feature_selector_matches_the_compiled_abi() {
    let Some(v) = artifact() else { return };
    let ids = &v["methodIdentifiers"];
    let got = ids["setFeature(uint256,uint64)"]
        .as_str()
        .expect("setFeature(uint256,uint64) in methodIdentifiers");
    assert_eq!(
        got,
        hex::encode(SET_FEATURE_SELECTOR),
        "kardamom_types::upgrades::SET_FEATURE_SELECTOR is stale",
    );
}

#[test]
fn chain_state_predeploy_address_is_the_canonical_one() {
    // Checks for a typing error in the TOML file or the constant. The two
    // values live in different files. Without this check, a mismatch stays
    // hidden until a system deposit lands at an empty account and does
    // nothing.
    assert_eq!(
        CHAIN_STATE,
        "0x4200000000000000000000000000000000000017"
            .parse::<Address>()
            .unwrap()
    );
}
