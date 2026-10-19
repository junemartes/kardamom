//! Print the deterministic factory proxy address, derived from the embedded
//! `KardamomFactoryV1` and `ERC1967Proxy` creation bytecode and the given
//! owner. Each app implementation that this factory deploys takes the
//! factory proxy address as its constructor argument.

use alloy_primitives::Address;
use clap::Parser;
use kardamom_deployer::addresses::{factory_impl_address, factory_proxy_address};
use kardamom_deployer::embedded;

#[derive(Parser)]
struct Args {
    /// Canonical owner for the target environment (Safe or EOA).
    #[arg(long)]
    owner: Address,
}

fn main() {
    let args = Args::parse();
    let factory_initcode = embedded::factory_v1_creation();
    let proxy_initcode = embedded::erc1967_proxy_creation();
    let impl_addr = factory_impl_address(&factory_initcode);
    let proxy_addr = factory_proxy_address(&proxy_initcode, impl_addr, args.owner);

    println!("KardamomFactoryV1 impl: {impl_addr:#x}");
    println!("Factory proxy: {proxy_addr:#x}");
    println!("(owner: {})", args.owner);
}
