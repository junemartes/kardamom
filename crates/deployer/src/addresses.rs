//! CREATE2 address derivation and ERC-7955/kardamom factory constants.
//!
//! This module does only math, with no I/O. It computes the factory proxy
//! address lazily, from the factory impl creation bytecode embedded at
//! build time (see [`crate::embedded`]) and the canonical owner.

use alloy_primitives::{Address, B256, Bytes, address, b256, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};

// Local binding for the factory's `initialize(address)`. It lives here, not
// in IKardamomFactory.sol, because it belongs to the concrete impl and is
// used only to build bootstrap calldata.
sol! {
    function initialize(address owner) external;
}

/// ERC-7955 permissionless CREATE2 factory.
/// This is the canonical address on every chain that supports EIP-7702
/// (mainnet, since the Pectra upgrade).
/// Spec: https://github.com/safe-research/erc-7955
pub const ERC7955_FACTORY: Address = address!("C0DEb853af168215879d284cc8B4d0A645fA9b0E");

/// ERC-7955 factory runtime bytecode (29 bytes). Tests inject this through
/// `anvil_setCode`. It is not used at runtime.
pub const ERC7955_RUNTIME_HEX: &str =
    "60203d3d3582360380843d373d34f5806019573d813d933efd5b3d52f33d52";

/// Salt for the kardamom factory impl, deployed through ERC-7955.
pub fn factory_impl_salt() -> B256 {
    keccak256(b"kardamom.factory.impl.v1")
}

/// Salt for the kardamom factory proxy, deployed through ERC-7955.
pub fn factory_proxy_salt() -> B256 {
    keccak256(b"kardamom.factory.proxy.v1")
}

/// Init calldata for the kardamom factory: `initialize(address owner)`.
pub fn factory_init_data(owner: Address) -> Bytes {
    Bytes::from(initializeCall { owner }.abi_encode())
}

/// Build the full proxy initcode: `ERC1967Proxy.creationCode` plus
/// `abi.encode(impl, initData)`.
pub fn proxy_full_initcode(
    proxy_creation_code: &Bytes,
    impl_addr: Address,
    init_data: &Bytes,
) -> Bytes {
    let mut full = proxy_creation_code.to_vec();
    let args = (impl_addr, init_data.clone()).abi_encode_params();
    full.extend_from_slice(&args);
    Bytes::from(full)
}

/// Factory impl address: CREATE2 from ERC-7955 with the factory impl salt.
pub fn factory_impl_address(impl_initcode: &Bytes) -> Address {
    ERC7955_FACTORY.create2(factory_impl_salt(), keccak256(impl_initcode))
}

/// Factory proxy address: CREATE2 from ERC-7955 with the factory proxy salt.
/// This depends on the canonical `owner`, baked into the proxy initcode
/// through `initialize(address)`.
pub fn factory_proxy_address(
    proxy_creation_code: &Bytes,
    impl_addr: Address,
    owner: Address,
) -> Address {
    let init_data = factory_init_data(owner);
    let full = proxy_full_initcode(proxy_creation_code, impl_addr, &init_data);
    ERC7955_FACTORY.create2_from_code(factory_proxy_salt(), &full)
}

/// App impl address, deployed through the kardamom factory, not through ERC-7955.
pub fn app_impl_address(factory: Address, impl_salt: B256, impl_initcode: &Bytes) -> Address {
    factory.create2(impl_salt, keccak256(impl_initcode))
}

/// App proxy address. The factory does a CREATE2 of `ERC1967Proxy(impl,
/// initData)` with `proxy_salt`. This mirrors `KardamomFactoryV1._deployUUPS`,
/// so callers can predict a contract's address before it is deployed. For
/// example, use this to pass one contract's address into another contract's
/// init data, within the same atomic batch.
///
/// The proxy address depends on `init_data`, since it is part of the proxy
/// constructor args. Predict with the exact init data the deploy will use.
pub fn app_proxy_address(
    factory: Address,
    proxy_creation_code: &Bytes,
    impl_addr: Address,
    init_data: &Bytes,
    proxy_salt: B256,
) -> Address {
    let full = proxy_full_initcode(proxy_creation_code, impl_addr, init_data);
    factory.create2_from_code(proxy_salt, &full)
}

/// ERC1967 implementation storage slot.
pub const ERC1967_IMPL_SLOT: B256 =
    b256!("360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_salts_are_distinct() {
        assert_ne!(factory_impl_salt(), factory_proxy_salt());
    }

    #[test]
    fn factory_init_data_is_initialize_address_selector() {
        let owner = address!("00000000000000000000000000000000000000aa");
        let data = factory_init_data(owner);
        assert_eq!(data.len(), 36); // 4-byte selector plus a 32-byte address argument
        let sel = &keccak256(b"initialize(address)")[..4];
        assert_eq!(&data[..4], sel);
        // The last 20 bytes of the 32-byte argument are the address.
        assert_eq!(&data[16..36], owner.as_slice());
    }

    #[test]
    fn factory_proxy_address_depends_on_owner() {
        let proxy_init = Bytes::from(vec![0u8; 100]);
        let impl_addr = address!("0000000000000000000000000000000000000001");
        let a = address!("00000000000000000000000000000000000000aa");
        let b = address!("00000000000000000000000000000000000000bb");
        let pa = factory_proxy_address(&proxy_init, impl_addr, a);
        let pb = factory_proxy_address(&proxy_init, impl_addr, b);
        assert_ne!(pa, pb);
    }

    #[test]
    fn erc7955_factory_address_is_correct() {
        assert_eq!(
            ERC7955_FACTORY,
            address!("C0DEb853af168215879d284cc8B4d0A645fA9b0E")
        );
    }
}
