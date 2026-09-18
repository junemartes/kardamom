//! Build `DeploymentSpec` values for `KardamomFactoryV1.applyDeployments`.

use std::num::NonZeroU64;

use alloy_primitives::{Address, B256, Bytes, U256};
use alloy_sol_types::SolValue;

use crate::ids::ContractId;

/// One-to-one with `IKardamomFactory.DeploymentSpec` on the Solidity side.
#[derive(Debug, Clone)]
pub struct DeploymentSpec {
    pub l2_chain_id: u64,
    pub id: B256,
    pub action: Action,
    pub impl_initcode: Bytes,
    pub init_data: Bytes,
    pub impl_salt: B256,
    /// If this is non-zero, the factory skips CREATE2 and reuses this impl.
    /// The deployer sets it when it groups multi-L2 ops. `build_spec` always
    /// returns zero here.
    pub target_impl: Address,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Deploy = 0,
    Upgrade = 1,
}

/// What the operator wants to happen for one contract on one L2.
#[derive(Debug, Clone)]
pub enum Op {
    /// Deploy a fresh contract for `id` on L2 `l2_chain_id`. It must not be
    /// registered yet.
    Deploy {
        l2_chain_id: u64,
        id: ContractId,
        init_args: Bytes,
    },
    /// Upgrade an existing contract on L2 `l2_chain_id` to `new_version`.
    Upgrade {
        l2_chain_id: u64,
        id: ContractId,
        new_version: u64,
        init_args: Bytes,
    },
}

impl Op {
    pub fn l2_chain_id(&self) -> u64 {
        match self {
            Op::Deploy { l2_chain_id, .. } | Op::Upgrade { l2_chain_id, .. } => *l2_chain_id,
        }
    }

    pub fn id(&self) -> ContractId {
        match self {
            Op::Deploy { id, .. } | Op::Upgrade { id, .. } => *id,
        }
    }

    /// Return the version the op targets: 1 for Deploy, `new_version` for Upgrade.
    pub fn version(&self) -> u64 {
        match self {
            Op::Deploy { .. } => 1,
            Op::Upgrade { new_version, .. } => *new_version,
        }
    }
}

/// Build a `DeploymentSpec` for a single `Op`. `target_impl` is always zero
/// here. The deployer's dedup pass fills it in after grouping by
/// `(id, version)`.
pub fn build_spec(op: &Op) -> DeploymentSpec {
    match op {
        Op::Deploy {
            l2_chain_id,
            id,
            init_args,
        } => DeploymentSpec {
            l2_chain_id: *l2_chain_id,
            id: id.id(),
            action: Action::Deploy,
            impl_initcode: id.creation_bytecode(),
            init_data: encode_init_calldata(*id, init_args),
            impl_salt: id.impl_salt(1),
            target_impl: Address::ZERO,
        },
        Op::Upgrade {
            l2_chain_id,
            id,
            new_version,
            init_args,
        } => DeploymentSpec {
            l2_chain_id: *l2_chain_id,
            id: id.id(),
            action: Action::Upgrade,
            impl_initcode: id.creation_bytecode(),
            init_data: init_args.clone(),
            impl_salt: id.impl_salt(*new_version),
            target_impl: Address::ZERO,
        },
    }
}

/// Encode `initialize(args...)` as a 4-byte selector plus the abi-encoded args.
pub fn encode_init_calldata(id: ContractId, abi_encoded_args: &Bytes) -> Bytes {
    let sel = id.init_selector();
    let mut out = Vec::with_capacity(4 + abi_encoded_args.len());
    out.extend_from_slice(&sel);
    out.extend_from_slice(abi_encoded_args);
    Bytes::from(out)
}

/// Abi-encode a single address argument for `initialize(address)`.
#[must_use]
pub fn encode_address_arg(addr: Address) -> Bytes {
    let v = (addr,).abi_encode();
    Bytes::from(v)
}

/// Abi-encode two address arguments, for example
/// `ETHLockbox.initialize(l2Minter, outputOracle)`.
#[must_use]
pub fn encode_address_pair(a: Address, b: Address) -> Bytes {
    Bytes::from((a, b).abi_encode_params())
}

/// Abi-encode `WithdrawalOutputOracle.initialize(attester, challenger, window)`.
#[must_use]
pub fn encode_oracle_init_args(attester: Address, challenger: Address, window: u64) -> Bytes {
    Bytes::from((attester, challenger, window).abi_encode_params())
}

/// Arguments for [`encode_proof_oracle_init_args`]: the v2
/// `KardamomProofOracle.initialize` call. `batch_vkey` is the batch guest,
/// for validity mode; `block_vkey` is the single-block guest, for disputes.
#[derive(Debug, Clone, Copy)]
pub struct ProofOracleInit {
    pub settlement: Address,
    pub verifier: Address,
    pub batch_vkey: B256,
    pub block_vkey: B256,
    pub genesis_root: B256,
    /// The optimistic dispute period. `KardamomProofOracle.initialize`
    /// does not reject 0 itself, and a zero window means a claim
    /// finalizes at once with no dispute period at all — nonzero at the
    /// type level so this crate cannot deploy that by accident.
    pub challenge_window_secs: NonZeroU64,
    pub min_bond_wei: U256,
}

impl ProofOracleInit {
    /// Encode init args for the v2
    /// `KardamomProofOracle.initialize(address,address,bytes32,bytes32,bytes32,uint64,uint96)`.
    #[must_use]
    pub fn encode(self) -> Bytes {
        Bytes::from(
            (
                self.settlement,
                self.verifier,
                self.batch_vkey,
                self.block_vkey,
                self.genesis_root,
                self.challenge_window_secs.get(),
                self.min_bond_wei,
            )
                .abi_encode_params(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    #[test]
    fn op_accessors_return_consistent_values() {
        let op = Op::Deploy {
            l2_chain_id: 42,
            id: ContractId::EthLockbox,
            init_args: Bytes::new(),
        };
        assert_eq!(op.l2_chain_id(), 42);
        assert_eq!(op.id(), ContractId::EthLockbox);
        assert_eq!(op.version(), 1);

        let op = Op::Upgrade {
            l2_chain_id: 43,
            id: ContractId::EthLockbox,
            new_version: 5,
            init_args: Bytes::new(),
        };
        assert_eq!(op.l2_chain_id(), 43);
        assert_eq!(op.version(), 5);
    }

    #[test]
    fn encode_init_calldata_prefixes_selector() {
        let arg = encode_address_arg(address!("00000000000000000000000000000000000000aa"));
        let data = encode_init_calldata(ContractId::EthLockbox, &arg);
        assert_eq!(&data[..4], &ContractId::EthLockbox.init_selector());
        assert_eq!(data.len(), 4 + 32);
    }

    #[test]
    fn encode_address_arg_pads_to_32_bytes() {
        let arg = encode_address_arg(address!("0000000000000000000000000000000000000001"));
        assert_eq!(arg.len(), 32);
        assert!(arg[..12].iter().all(|&b| b == 0));
        assert_eq!(arg[31], 0x01);
    }
}
