//! Pins the ERC-7955 predeploy entry in `chains/dev.toml`:
//!   * exists at the canonical [`ERC7955_FACTORY`] address,
//!   * carries bytecode byte-equal to [`ERC7955_RUNTIME_HEX`].
//!
//! Regression target: anyone editing `chains/dev.toml` (or the rust constants)
//! who drifts the two breaks this test before breaking a real dev chain.

use std::path::PathBuf;

use alloy_primitives::{Bytes, hex};
use kardamom_deployer::addresses::{ERC7955_FACTORY, ERC7955_RUNTIME_HEX};
use kardamom_node::{Genesis, Node};

#[tokio::test]
async fn dev_genesis_predeploys_erc7955_factory_with_expected_bytecode() {
    let dev_toml = PathBuf::from(env!("CARGO_WORKSPACE_DIR")).join("chains/dev.toml");
    let raw = std::fs::read_to_string(&dev_toml)
        .unwrap_or_else(|e| panic!("read {}: {e}", dev_toml.display()));
    let genesis: Genesis = toml::from_str(&raw).expect("dev.toml parses");
    genesis.validate().expect("dev.toml validates");

    let node = Node::new(&genesis);
    let code = node.code_at(ERC7955_FACTORY).await;
    let expected = Bytes::from(hex::decode(ERC7955_RUNTIME_HEX).expect("ERC7955_RUNTIME_HEX hex"));
    assert_eq!(
        code, expected,
        "chains/dev.toml ERC-7955 predeploy bytecode does not match kardamom_deployer::addresses::ERC7955_RUNTIME_HEX",
    );
}
