//! Cross-check: the FACTORY constant baked into KardamomUUPSBase.sol matches the
//! address that addresses::factory_proxy_address(...) computes from the forge
//! artifact. If either drifts, this test fails — the operator must update the
//! Solidity source.

use std::path::PathBuf;

use kardamom_deployer::addresses::{
    factory_impl_address, factory_init_data, factory_proxy_address,
};
use kardamom_deployer::artifacts::{creation_bytecode, default_contracts_root};

#[test]
fn factory_constant_in_source_matches_computed_address() {
    let root = default_contracts_root();

    let factory_initcode = match creation_bytecode(&root, "KardamomFactoryV1") {
        Ok(b) => b,
        Err(_) => {
            eprintln!("SKIP: KardamomFactoryV1 artifact missing; run forge build in contracts/");
            return;
        }
    };
    // Prefer the raw ERC1967Proxy artifact (matches what the factory uses internally
    // for app proxies). Fall back to ProxyArtifact if forge didn't emit it.
    let proxy_initcode = creation_bytecode(&root, "ERC1967Proxy")
        .or_else(|_| creation_bytecode(&root, "ProxyArtifact"))
        .unwrap_or_else(|_| {
            panic!("neither ERC1967Proxy.json nor ProxyArtifact.json found; run forge build");
        });

    let impl_addr = factory_impl_address(&factory_initcode);
    let init_data = factory_init_data();
    let computed = factory_proxy_address(&proxy_initcode, impl_addr, &init_data);

    let base_path: PathBuf = root.join("src/factory/KardamomUUPSBase.sol");
    let src = std::fs::read_to_string(&base_path)
        .unwrap_or_else(|_| panic!("read {}", base_path.display()));

    // Solidity address literals must match the case-sensitive EIP-55 checksum form,
    // but the formatter is case-insensitive when comparing. Match in both cases.
    let computed_hex = format!("{computed:#x}");
    let lc = computed_hex.to_lowercase();
    let src_lc = src.to_lowercase();

    assert!(
        src_lc.contains(&lc),
        "KardamomUUPSBase.FACTORY does not match computed address {computed:#x}. \
         Update the FACTORY constant in {}.",
        base_path.display()
    );
}
