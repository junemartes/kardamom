//! Known L1 contract ids and their per-contract metadata.
//!
//! Each variant maps to a forge artifact name, a registry id (keccak256 of a
//! canonical label), and an `initialize` signature used to encode init calldata.
//! Adding a new L1 contract is one new variant + four match-arms — no factory edit.

use alloy_primitives::{B256, keccak256};
use alloy_sol_types::SolValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractId {
    EthLockbox,
}

impl ContractId {
    /// Canonical label hashed into the registry key.
    pub fn label(self) -> &'static str {
        match self {
            ContractId::EthLockbox => "kardamom.l1.ETHLockbox",
        }
    }

    /// Forge artifact filename stem (matches `Name.sol` under `contracts/src/`).
    pub fn artifact_name(self) -> &'static str {
        match self {
            ContractId::EthLockbox => "ETHLockbox",
        }
    }

    /// Solidity signature of the impl's `initialize` method.
    pub fn init_signature(self) -> &'static str {
        match self {
            ContractId::EthLockbox => "initialize(address)",
        }
    }

    /// Registry id: `keccak256(label)`.
    pub fn id(self) -> B256 {
        keccak256(self.label().as_bytes())
    }

    /// Proxy salt — includes l2_chain_id so each L2 gets a distinct proxy address.
    /// `keccak256(abi.encode(l2ChainId, id, "proxy"))`.
    pub fn proxy_salt(self, l2_chain_id: u64) -> B256 {
        let encoded = (l2_chain_id, self.id(), "proxy".to_string()).abi_encode();
        keccak256(encoded)
    }

    /// Impl salt — does NOT include l2_chain_id; impl is shared across L2s.
    /// `keccak256(abi.encode(id, "impl", version))`.
    pub fn impl_salt(self, version: u64) -> B256 {
        let encoded = (self.id(), "impl".to_string(), version).abi_encode();
        keccak256(encoded)
    }

    /// 4-byte selector of the init signature.
    pub fn init_selector(self) -> [u8; 4] {
        let h = keccak256(self.init_signature().as_bytes());
        [h[0], h[1], h[2], h[3]]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eth_lockbox_id_is_keccak256_of_label() {
        let id = ContractId::EthLockbox.id();
        let expected = keccak256(b"kardamom.l1.ETHLockbox");
        assert_eq!(id, expected);
    }

    #[test]
    fn init_selector_for_eth_lockbox() {
        let sel = ContractId::EthLockbox.init_selector();
        let expected = &keccak256(b"initialize(address)")[..4];
        assert_eq!(&sel[..], expected);
    }

    #[test]
    fn impl_salt_changes_with_version() {
        let v1 = ContractId::EthLockbox.impl_salt(1);
        let v2 = ContractId::EthLockbox.impl_salt(2);
        assert_ne!(v1, v2);
    }

    #[test]
    fn impl_salt_does_not_depend_on_l2_chain_id() {
        // Important: this property enables impl-sharing across L2s. Don't accidentally
        // mix l2_chain_id into impl_salt.
        let v1 = ContractId::EthLockbox.impl_salt(1);
        // No chain id input at all — same salt no matter the deployment context.
        let v1_again = ContractId::EthLockbox.impl_salt(1);
        assert_eq!(v1, v1_again);
    }

    #[test]
    fn proxy_salt_changes_with_l2_chain_id() {
        let a = ContractId::EthLockbox.proxy_salt(42);
        let b = ContractId::EthLockbox.proxy_salt(43);
        assert_ne!(a, b);
    }

    #[test]
    fn proxy_salt_is_stable_for_same_inputs() {
        let s1 = ContractId::EthLockbox.proxy_salt(42);
        let s2 = ContractId::EthLockbox.proxy_salt(42);
        assert_eq!(s1, s2);
    }
}
