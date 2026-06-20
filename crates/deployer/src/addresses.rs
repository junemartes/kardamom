//! CREATE2 address derivation and ERC-7955 / kardamom factory constants.
//!
//! All pure math, no I/O. The factory proxy address is computed lazily from
//! the build-time-embedded factory impl creation bytecode (see [`crate::embedded`])
//! and the canonical owner.

use alloy_primitives::{Address, B256, Bytes, address, b256, keccak256};
use alloy_sol_types::{SolCall, SolValue, sol};

// Local binding for the factory's `initialize(address)`. Lives here (not in
// IKardamomFactory.sol) because it's on the concrete impl and used only for
// building bootstrap calldata.
sol! {
    function initialize(address owner) external;
}

/// ERC-7955 permissionless CREATE2 factory.
/// Canonical address on every EIP-7702-supporting chain (mainnet since Pectra).
/// Spec: https://github.com/safe-research/erc-7955
pub const ERC7955_FACTORY: Address = address!("C0DEb853af168215879d284cc8B4d0A645fA9b0E");

/// ERC-7955 factory runtime bytecode (29 bytes). Used by tests that inject it via
/// `anvil_setCode`; not used at runtime.
pub const ERC7955_RUNTIME_HEX: &str =
    "60203d3d3582360380843d373d34f5806019573d813d933efd5b3d52f33d52";

/// Salt for the kardamom factory impl, deployed via ERC-7955.
pub fn factory_impl_salt() -> B256 {
    keccak256(b"kardamom.factory.impl.v1")
}

/// Salt for the kardamom factory proxy, deployed via ERC-7955.
pub fn factory_proxy_salt() -> B256 {
    keccak256(b"kardamom.factory.proxy.v1")
}

/// Init calldata for the kardamom factory: `initialize(address owner)`.
pub fn factory_init_data(owner: Address) -> Bytes {
    Bytes::from(initializeCall { owner }.abi_encode())
}

/// Build the full proxy initcode = `ERC1967Proxy.creationCode || abi.encode(impl, initData)`.
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

/// Factory proxy address: CREATE2 from ERC-7955 with the factory proxy salt, parameterized
/// by the canonical `owner` (baked into the proxy initcode via `initialize(address)`).
pub fn factory_proxy_address(
    proxy_creation_code: &Bytes,
    impl_addr: Address,
    owner: Address,
) -> Address {
    let init_data = factory_init_data(owner);
    let full = proxy_full_initcode(proxy_creation_code, impl_addr, &init_data);
    ERC7955_FACTORY.create2_from_code(factory_proxy_salt(), &full)
}

/// App impl address (deployed via the kardamom factory, not via ERC-7955).
pub fn app_impl_address(factory: Address, impl_salt: B256, impl_initcode: &Bytes) -> Address {
    factory.create2(impl_salt, keccak256(impl_initcode))
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
        assert_eq!(data.len(), 36); // 4-byte selector + 32-byte address arg
        let sel = &keccak256(b"initialize(address)")[..4];
        assert_eq!(&data[..4], sel);
        // last 20 bytes of the 32-byte arg are the address itself
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
