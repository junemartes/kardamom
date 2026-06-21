//! Pins the ERC-7955 predeploy entry in `chains/dev.toml`:
//!   * exists at the canonical [`ERC7955_FACTORY`] address,
//!   * carries bytecode byte-equal to [`ERC7955_RUNTIME_HEX`].
//!
//! Regression target: anyone editing `chains/dev.toml` (or the rust constants)
//! who drifts the two breaks this test before breaking a real dev chain.
//!
//! Asserts directly against the parsed genesis alloc (the canonical source the
//! cluster executor seeds its state from) rather than round-tripping through a
//! node — `chains/dev.toml` is exactly what gets predeployed.

use std::path::PathBuf;

use alloy_primitives::{Bytes, hex};
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_types::Genesis;

#[test]
fn dev_genesis_predeploys_erc7955_factory_with_expected_bytecode() {
    let dev_toml = PathBuf::from(env!("CARGO_WORKSPACE_DIR")).join("chains/dev.toml");
    let raw = std::fs::read_to_string(&dev_toml)
        .unwrap_or_else(|e| panic!("read {}: {e}", dev_toml.display()));
    let genesis: Genesis = toml::from_str(&raw).expect("dev.toml parses");
    genesis.validate().expect("dev.toml validates");

    let entry = genesis
        .alloc
        .iter()
        .find(|e| e.address == ERC7955_FACTORY)
        .unwrap_or_else(|| {
            panic!("chains/dev.toml has no alloc entry for ERC-7955 factory {ERC7955_FACTORY}")
        });
    let expected = Bytes::from(hex::decode(ERC7955_RUNTIME_HEX).expect("ERC7955_RUNTIME_HEX hex"));
    assert_eq!(
        entry.code.as_ref(),
        Some(&expected),
        "chains/dev.toml ERC-7955 predeploy bytecode does not match \
         kardamom_deployer::addresses::ERC7955_RUNTIME_HEX",
    );
}
