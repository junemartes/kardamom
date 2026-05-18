//! CREATE2 address derivation and kardamom factory address constants.
//!
//! Everything here is pure math — no I/O. Constants encode the choice of the Arachnid
//! SingletonFactory and the kardamom factory's bootstrap salts. The factory's proxy
//! address is computed lazily from the impl creation-code hash (forge artifact), since
//! that depends on the compiled `KardamomFactoryV1` source.

use alloy_primitives::{Address, B256, Bytes, address, b256, keccak256};
use alloy_sol_types::SolValue;

/// Canonical Arachnid SingletonFactory address (deployed via Nick's method on most
/// chains; predeployed in `chains/dev.toml` for anvil).
pub const SINGLETON_FACTORY: Address = address!("4e59b44847b379578588920cA78FbF26c0B4956C");

/// Salt for the factory impl CREATE2 deploy: `keccak256("kardamom.factory.impl.v1")`.
pub fn factory_impl_salt() -> B256 {
    keccak256(b"kardamom.factory.impl.v1")
}

/// Salt for the factory proxy CREATE2 deploy: `keccak256("kardamom.factory.proxy")`.
pub fn factory_proxy_salt() -> B256 {
    keccak256(b"kardamom.factory.proxy")
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
    let args = (impl_addr, init_data.clone()).abi_encode();
    full.extend_from_slice(&args);
    Bytes::from(full)
}

/// Factory impl address: CREATE2 from the singleton with the factory impl salt.
pub fn factory_impl_address(impl_initcode: &Bytes) -> Address {
    create2_address(
        SINGLETON_FACTORY,
        factory_impl_salt(),
        keccak256(impl_initcode),
    )
}

/// Factory proxy address: CREATE2 from the singleton with the factory proxy salt,
/// using the full `ERC1967Proxy(impl, initData)` initcode.
pub fn factory_proxy_address(
    proxy_creation_code: &Bytes,
    impl_addr: Address,
    init_data: &Bytes,
) -> Address {
    let full = proxy_full_initcode(proxy_creation_code, impl_addr, init_data);
    create2_address(SINGLETON_FACTORY, factory_proxy_salt(), keccak256(&full))
}

/// App impl address (deployed via the kardamom factory, not the singleton).
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

/// Init data for the factory's bootstrap proxy: `abi.encodeWithSignature("initialize()")`.
/// Always the same — operator-independent — so the factory proxy address is the same
/// on every chain.
pub fn factory_init_data() -> Bytes {
    let selector = &keccak256(b"initialize()")[..4];
    Bytes::from(selector.to_vec())
}

/// ERC1967 implementation storage slot:
/// `uint256(keccak256("eip1967.proxy.implementation")) - 1`.
pub const ERC1967_IMPL_SLOT: B256 =
    b256!("360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc");

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::hex;

    #[test]
    fn create2_is_deterministic() {
        let deployer = address!("00000000000000000000000000000000DeaDBeef");
        let salt = B256::ZERO;
        let init_code_hash = keccak256(b"");
        let a = create2_address(deployer, salt, init_code_hash);
        let b = create2_address(deployer, salt, init_code_hash);
        assert_eq!(a, b);
    }

    #[test]
    fn create2_matches_eip_1014_example_5() {
        // From EIP-1014 examples table, example 5:
        //   address       = 0x00000000000000000000000000000000deadbeef
        //   salt          = 0x00000000000000000000000000000000000000000000000000000000cafebabe
        //   init_code     = 0xdeadbeef
        //   expected_addr = 0x60f3f640a8508fc6a86d45df051962668e1e8ac7
        let deployer = address!("00000000000000000000000000000000deadbeef");
        let salt = b256!("00000000000000000000000000000000000000000000000000000000cafebabe");
        let initcode = hex::decode("deadbeef").unwrap();
        let h = keccak256(&initcode);
        let addr = create2_address(deployer, salt, h);
        assert_eq!(addr, address!("60f3f640a8508fc6a86d45df051962668e1e8ac7"));
    }

    #[test]
    fn factory_init_data_is_initialize_selector() {
        let data = factory_init_data();
        assert_eq!(data.len(), 4);
        let expected = &keccak256(b"initialize()")[..4];
        assert_eq!(data.as_ref(), expected);
    }

    #[test]
    fn factory_salts_are_distinct() {
        assert_ne!(factory_impl_salt(), factory_proxy_salt());
    }

    #[test]
    fn erc1967_slot_is_canonical() {
        // (uint256(keccak256("eip1967.proxy.implementation")) - 1).to_be_bytes()
        let pre = keccak256(b"eip1967.proxy.implementation");
        let mut val = [0u8; 32];
        val.copy_from_slice(pre.as_slice());
        let mut borrow: i32 = 1;
        for i in (0..32).rev() {
            let v = val[i] as i32 - borrow;
            if v < 0 {
                val[i] = (v + 256) as u8;
                borrow = 1;
            } else {
                val[i] = v as u8;
                borrow = 0;
            }
        }
        assert_eq!(borrow, 0);
        assert_eq!(ERC1967_IMPL_SLOT.as_slice(), &val[..]);
        assert_eq!(
            hex::encode(ERC1967_IMPL_SLOT.as_slice()),
            "360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc"
        );
    }
}
