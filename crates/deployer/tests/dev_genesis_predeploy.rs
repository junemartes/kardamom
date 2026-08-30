//! Checks the ERC-7955 predeploy entry in `chains/dev.toml`. The entry:
//!   * exists at the canonical [`ERC7955_FACTORY`] address,
//!   * has bytecode that is byte-equal to [`ERC7955_RUNTIME_HEX`].
//!
//! If `chains/dev.toml` and the Rust constants drift apart, this test fails
//! before a real dev chain breaks.
//!
//! The test checks the parsed genesis alloc directly, instead of round-tripping
//! through a node. The alloc is the canonical source the cluster executor uses
//! to seed its state, and it is exactly what `chains/dev.toml` predeploys.

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
