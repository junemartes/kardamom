//! Checks that the FACTORY constant in KardamomUUPSBase.sol matches the
//! address that `addresses::factory_proxy_address(...)` computes for the
//! canonical dev/test owner. A production owner gives a different address
//! and a different KardamomUUPSBase build. This test covers only the dev
//! path used in local and CI testing.

use std::path::PathBuf;

use alloy_primitives::{Address, address};
use kardamom_deployer::addresses::{factory_impl_address, factory_proxy_address};
use kardamom_deployer::embedded;

/// Canonical owner for `deploy_e2e` and other dev/test deployments. It must match
/// the owner in KardamomUUPSBase.FACTORY.
const DEV_OWNER: Address = address!("00000000000000000000000000000000DEAD0001");

#[test]
fn factory_constant_in_source_matches_computed_address() {
    let factory_initcode = embedded::factory_v1_creation();
    let proxy_initcode = embedded::erc1967_proxy_creation();

    let impl_addr = factory_impl_address(&factory_initcode);
    let computed = factory_proxy_address(&proxy_initcode, impl_addr, DEV_OWNER);

    let base_path: PathBuf = PathBuf::from(env!("CARGO_WORKSPACE_DIR"))
        .join("contracts/src/factory/KardamomUUPSBase.sol");
    let src = std::fs::read_to_string(&base_path)
        .unwrap_or_else(|_| panic!("read {}", base_path.display()));

    let computed_hex = format!("{computed:#x}");
    let lc = computed_hex.to_lowercase();
    let src_lc = src.to_lowercase();

    assert!(
        src_lc.contains(&lc),
        "KardamomUUPSBase.FACTORY does not match computed address {computed:#x}. \
         Update the FACTORY constant in {} (compute via: cargo run --bin print-factory-address -- --owner {DEV_OWNER:#x}).",
        base_path.display()
    );
}
