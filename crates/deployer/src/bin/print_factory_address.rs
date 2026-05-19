//! Helper: prints the deterministic factory proxy address derived from
//! KardamomFactoryV1's compiled initcode, the ERC1967Proxy creation code, and the
//! supplied owner. Run after `forge build` (and after picking your canonical owner)
//! to bake the address into KardamomUUPSBase.sol.

use alloy_primitives::Address;
use clap::Parser;
use kardamom_deployer::addresses::{factory_impl_address, factory_proxy_address};
use kardamom_deployer::artifacts::{creation_bytecode, default_contracts_root};

#[derive(Parser)]
struct Args {
    /// Canonical owner for the target environment (Safe or EOA).
    #[arg(long)]
    owner: Address,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let root = default_contracts_root();
    let factory_initcode = creation_bytecode(&root, "KardamomFactoryV1")?;
    let proxy_initcode = match creation_bytecode(&root, "ERC1967Proxy") {
        Ok(b) => b,
        Err(_) => creation_bytecode(&root, "ProxyArtifact")?,
    };
    let impl_addr = factory_impl_address(&factory_initcode);
    let proxy_addr = factory_proxy_address(&proxy_initcode, impl_addr, args.owner);

    println!("KardamomFactoryV1 impl: {impl_addr:#x}");
    println!("Factory proxy (FACTORY constant): {proxy_addr:#x}");
    println!("(owner: {})", args.owner);
    Ok(())
}
