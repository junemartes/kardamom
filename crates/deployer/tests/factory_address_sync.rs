//! Cross-check: the FACTORY constant baked into KardamomUUPSBase.sol matches the
//! address computed by `addresses::factory_proxy_address(...)` for the canonical
//! dev/test owner. Production owners produce different addresses (and a different
//! KardamomUUPSBase build); this test pins the address for the dev path used in
//! local + CI testing.

use std::path::PathBuf;

use alloy_primitives::{Address, address};
use kardamom_deployer::addresses::{factory_impl_address, factory_proxy_address};
use kardamom_deployer::embedded;

/// Canonical owner used by `deploy_e2e` and any other dev/test deployment. Must match
/// the owner baked into KardamomUUPSBase.FACTORY.
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
