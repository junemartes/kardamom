//! CREATE2 address derivation and ERC-7955 / kardamom factory constants.
//!
//! All pure math, no I/O. The factory proxy address is computed lazily from the
//! compiled factory impl initcode (via forge artifact) and the canonical owner.

use alloy_primitives::{Address, B256, Bytes, address, b256, keccak256};
use alloy_sol_types::SolValue;

/// ERC-7955 permissionless CREATE2 factory.
/// Canonical address on every EIP-7702-supporting chain (mainnet since Pectra).
/// Spec: https://github.com/safe-research/erc-7955
pub const ERC7955_FACTORY: Address = address!("C0DEb853af168215879d284cc8B4d0A645fA9b0E");

/// ERC-7955's publicly-known deployer EOA (used to sign the EIP-7702 self-bootstrap).
/// Not used directly by this crate yet — kept for future self-bootstrap support.
pub const ERC7955_DEPLOYER: Address = address!("962560A0333190D57009A0aAAB7Bfa088f58461C");

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
    let selector = &keccak256(b"initialize(address)")[..4];
    let mut buf = Vec::with_capacity(4 + 32);
    buf.extend_from_slice(selector);
    let arg = (owner,).abi_encode();
    buf.extend_from_slice(&arg);
    Bytes::from(buf)
}

/// CREATE2 address: `keccak256(0xff || deployer || salt || keccak256(initcode))[12..32]`.
pub fn create2_address(deployer: Address, salt: B256, init_code_hash: B256) -> Address {
    let mut buf = [0u8; 1 + 20 + 32 + 32];
    buf[0] = 0xff;
    buf[1..21].copy_from_slice(deployer.as_slice());
    buf[21..53].copy_from_slice(salt.as_slice());
    buf[53..85].copy_from_slice(init_code_hash.as_slice());
    let h = keccak256(buf);
    let mut out = [0u8; 20];
    out.copy_from_slice(&h[12..32]);
    Address::from(out)
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
    create2_address(ERC7955_FACTORY, factory_impl_salt(), keccak256(impl_initcode))
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
    create2_address(ERC7955_FACTORY, factory_proxy_salt(), keccak256(&full))
}

/// App impl address (deployed via the kardamom factory, not via ERC-7955).
pub fn app_impl_address(factory: Address, impl_salt: B256, impl_initcode: &Bytes) -> Address {
    create2_address(factory, impl_salt, keccak256(impl_initcode))
}

/// App proxy address (deployed via the kardamom factory).
pub fn app_proxy_address(
    factory: Address,
    proxy_creation_code: &Bytes,
    impl_addr: Address,
    init_data: &Bytes,
    proxy_salt: B256,
) -> Address {
    let full = proxy_full_initcode(proxy_creation_code, impl_addr, init_data);
    create2_address(factory, proxy_salt, keccak256(&full))
}

/// ERC1967 implementation storage slot.
pub const ERC1967_IMPL_SLOT: B256 =
    b256!("360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc");

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;

    #[test]
    fn create2_matches_eip_1014_example_5() {
        let deployer = address!("00000000000000000000000000000000deadbeef");
        let salt = b256!("00000000000000000000000000000000000000000000000000000000cafebabe");
        let initcode = hex::decode("deadbeef").unwrap();
        let h = keccak256(&initcode);
        let addr = create2_address(deployer, salt, h);
        assert_eq!(addr, address!("60f3f640a8508fc6a86d45df051962668e1e8ac7"));
    }

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
