//! Helper: prints the deterministic factory proxy address derived from
//! KardamomFactoryV1's compiled initcode and the canonical ERC1967Proxy creation code.
//! Run once after `forge build`; bake the printed address into KardamomUUPSBase.sol.

use kardamom_deployer::addresses::{
    factory_impl_address, factory_init_data, factory_proxy_address,
};
use kardamom_deployer::artifacts::{creation_bytecode, default_contracts_root};

fn main() -> anyhow::Result<()> {
    let root = default_contracts_root();
    let factory_initcode = creation_bytecode(&root, "KardamomFactoryV1")?;
    // Prefer the raw ERC1967Proxy artifact (emitted by forge as a transitive
    // dependency of KardamomFactoryV1). If absent, fall back to ProxyArtifact.
    let proxy_initcode = match creation_bytecode(&root, "ERC1967Proxy") {
        Ok(b) => {
            eprintln!("using ERC1967Proxy artifact (matches factory's internal proxy code)");
            b
        }
        Err(_) => {
            eprintln!("ERC1967Proxy artifact missing; falling back to ProxyArtifact wrapper");
            creation_bytecode(&root, "ProxyArtifact")?
        }
    };

    let impl_addr = factory_impl_address(&factory_initcode);
    let init_data = factory_init_data();
    let proxy_addr = factory_proxy_address(&proxy_initcode, impl_addr, &init_data);

    println!("KardamomFactoryV1 impl: {impl_addr:#x}");
    println!("Factory proxy (FACTORY constant): {proxy_addr:#x}");
    Ok(())
}
