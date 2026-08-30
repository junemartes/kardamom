//! Pins the `KardamomChainState` predeploy in `chains/dev-withdrawals.toml`:
//!   * exists at the canonical chain-state address,
//!   * carries runtime bytecode byte-equal to the forge-compiled artifact,
//!   * and the Rust-side constants the derivation rule encodes with
//!     (`SYSTEM_UPGRADER`, the `setFeature` selector) match the compiled ABI.
//!
//! Regression target: the L2 write-authority check lives in Solidity while the
//! sender that satisfies it is minted in Rust. If those two drift, every
//! upgrade silently reverts on-chain — a failure that would otherwise only
//! surface in the e2e suite, and only as "the flag never activated".

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
    // The constant is inlined into the runtime bytecode, so read it back
    // through the ABI-declared getter's compiled value: the source is the
    // authority, and `forge inspect` exposes it via the deployed source map
    // only indirectly — so assert on the literal in the source instead, which
    // is what a reviewer edits.
    let workspace = PathBuf::from(env!("CARGO_WORKSPACE_DIR"));
    let src = std::fs::read_to_string(workspace.join("contracts/src/L2/KardamomChainState.sol"))
        .expect("read KardamomChainState.sol");
    // solc requires the EIP-55 checksummed form in an address literal, so this
    // is exactly the string that must appear in the source.
    let want = SYSTEM_UPGRADER.to_checksum(None);
    assert!(
        src.contains(&want),
        "KardamomChainState.SYSTEM_UPGRADER does not match \
         kardamom_types::upgrades::SYSTEM_UPGRADER ({want}); the L2 write-authority \
         check would reject every upgrade deposit the derivation rule mints",
    );

    // Belt and braces: the address must be a real constant in the ABI surface.
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
    // Guards a fat-finger in either the TOML or the constant; the two are
    // written in different files and nothing else would catch a mismatch
    // until a system deposit landed at an empty account and silently no-oped.
    assert_eq!(
        CHAIN_STATE,
        "0x4200000000000000000000000000000000000017"
            .parse::<Address>()
            .unwrap()
    );
}
