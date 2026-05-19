//! Helper: prints the deterministic factory proxy address derived from
//! KardamomFactoryV1's compiled initcode and the canonical ERC1967Proxy creation code.
//! Run once after `forge build`; bake the printed address into KardamomUUPSBase.sol.
//!
//! Usage: print_factory_address <owner>
//!   <owner>  hex owner address (e.g. 0x000...000)

use std::str::FromStr;

use alloy_primitives::Address;
use kardamom_deployer::addresses::{factory_impl_address, factory_proxy_address};
use kardamom_deployer::artifacts::{creation_bytecode, default_contracts_root};

fn main() -> anyhow::Result<()> {
    let owner_str = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("usage: print_factory_address <owner-address>");
        eprintln!("using Address::ZERO as placeholder");
        "0x0000000000000000000000000000000000000000".to_string()
    });
    let owner = Address::from_str(&owner_str)
        .map_err(|e| anyhow::anyhow!("invalid owner address: {e}"))?;

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
    let proxy_addr = factory_proxy_address(&proxy_initcode, impl_addr, owner);

    println!("KardamomFactoryV1 impl: {impl_addr:#x}");
    println!("Factory proxy (owner={owner:#x}): {proxy_addr:#x}");
    Ok(())
}
