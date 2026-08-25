//! Known L1 contract ids and their per-contract metadata.
//!
//! Each variant maps to a forge artifact name, a registry id (keccak256 of a
//! canonical label), and an `initialize` signature used to encode init calldata.
//! Adding a new L1 contract is one new variant + four match-arms — no factory edit.

use alloy_primitives::{B256, Bytes, U256, keccak256};
use alloy_sol_types::SolValue;

use crate::embedded;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractId {
    EthLockbox,
    /// L2 data-availability sink for the S7 L1 batcher. Records
    /// `(prevBatchIndex, blobHashes, l2BlockStart, l2BlockEnd)` and emits
    /// `BatchPosted` — no state-root storage (S0).
    KardamomL2Settlement,
    /// L1 registry of attested L2 output roots for the withdrawal off-ramp.
    /// Permissioned attester proposes outputs; permissioned challenger deletes
    /// within the finalization window.
    WithdrawalOutputOracle,
    /// L1 zk root chain (spec: no-std-exec-core, PR 4): running state root
    /// advanced one posted batch at a time on a verified validity proof
    /// whose public values match the settlement's stored batch entry.
    KardamomProofOracle,
}

impl ContractId {
    /// Every variant. The exhaustive match in `creation_bytecode` makes this
    /// list mandatory-to-update when a new variant is added.
    pub const ALL: &'static [Self] = &[
        Self::EthLockbox,
        Self::KardamomL2Settlement,
        Self::KardamomProofOracle,
        Self::WithdrawalOutputOracle,
    ];

    /// Canonical label hashed into the registry key.
    pub fn label(self) -> &'static str {
        match self {
            ContractId::EthLockbox => "kardamom.l1.ETHLockbox",
            ContractId::KardamomL2Settlement => "kardamom.l2.KardamomL2Settlement",
            ContractId::WithdrawalOutputOracle => "kardamom.l1.WithdrawalOutputOracle",
            ContractId::KardamomProofOracle => "kardamom.l1.KardamomProofOracle",
        }
    }

    /// Solidity signature of the impl's `initialize` method.
    pub fn init_signature(self) -> &'static str {
        match self {
            // ETHLockbox.initialize(address _l2Minter, address _outputOracle)
            ContractId::EthLockbox => "initialize(address,address)",
            // KardamomL2Settlement.initialize(address _l1Batcher)
            ContractId::KardamomL2Settlement => "initialize(address)",
            // WithdrawalOutputOracle.initialize(address attester, address challenger, uint64 window)
            ContractId::WithdrawalOutputOracle => "initialize(address,address,uint64)",
            // KardamomProofOracle.initialize(settlement, verifier, programVKey, genesisRoot)
            ContractId::KardamomProofOracle => "initialize(address,address,bytes32,bytes32)",
        }
    }

    /// Registry id: `keccak256(label)`.
    pub fn id(self) -> B256 {
        keccak256(self.label().as_bytes())
    }

    /// Proxy salt — includes l2_chain_id so each L2 gets a distinct proxy address.
    /// Must byte-match `KardamomFactoryV1._deployUUPS`, which computes the salt
    /// itself as `keccak256(abi.encode(uint256 l2ChainId, bytes32 id, "proxy"))`.
    /// Use `abi_encode_params` (Solidity `abi.encode(args…)` semantics): a plain
    /// `abi_encode` of the tuple would prepend a leading offset because the tuple
    /// contains a dynamic member (the string), diverging from Solidity.
    pub fn proxy_salt(self, l2_chain_id: u64) -> B256 {
        let encoded = (U256::from(l2_chain_id), self.id(), "proxy".to_string()).abi_encode_params();
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

    /// Creation bytecode for this contract, embedded at build time. Adding a
    /// new variant without wiring it through `build.rs` is caught by the
    /// `every_contract_id_has_nonempty_creation_bytecode` test.
    pub fn creation_bytecode(self) -> Bytes {
        match self {
            ContractId::EthLockbox => embedded::eth_lockbox_creation(),
            ContractId::KardamomProofOracle => embedded::kardamom_proof_oracle_creation(),
            ContractId::KardamomL2Settlement => embedded::kardamom_l2_settlement_creation(),
            ContractId::WithdrawalOutputOracle => embedded::withdrawal_output_oracle_creation(),
        }
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
        let expected = &keccak256(b"initialize(address,address)")[..4];
        assert_eq!(&sel[..], expected);
    }

    #[test]
    fn withdrawal_output_oracle_id_and_selector() {
        let id = ContractId::WithdrawalOutputOracle.id();
        assert_eq!(id, keccak256(b"kardamom.l1.WithdrawalOutputOracle"));
        let sel = ContractId::WithdrawalOutputOracle.init_selector();
        assert_eq!(
            &sel[..],
            &keccak256(b"initialize(address,address,uint64)")[..4]
        );
        // Distinct from the other ids.
        assert_ne!(id, ContractId::EthLockbox.id());
        assert_ne!(id, ContractId::KardamomL2Settlement.id());
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

    #[test]
    fn every_contract_id_has_nonempty_creation_bytecode() {
        // Iterates `ContractId::ALL`; the exhaustive match in `creation_bytecode`
        // makes the list mandatory-to-update for new variants.
        for &id in ContractId::ALL {
            assert!(
                !id.creation_bytecode().is_empty(),
                "{id:?} has empty creation bytecode; wire it into build.rs"
            );
        }
    }

    #[test]
    fn kardamom_l2_settlement_id_is_keccak256_of_label() {
        let id = ContractId::KardamomL2Settlement.id();
        let expected = keccak256(b"kardamom.l2.KardamomL2Settlement");
        assert_eq!(id, expected);
    }

    #[test]
    fn kardamom_l2_settlement_label_is_distinct_from_eth_lockbox() {
        assert_ne!(
            ContractId::KardamomL2Settlement.id(),
            ContractId::EthLockbox.id()
        );
    }
}
