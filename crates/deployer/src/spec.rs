//! Build `DeploymentSpec` values consumed by `KardamomFactoryV1.applyDeployments`.

use alloy_primitives::{Address, B256, Bytes};
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
    /// If non-zero, the factory skips CREATE2 and reuses this impl. Set by the deployer
    /// when grouping multi-L2 ops; build_spec always returns zero here.
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
    /// Deploy a fresh contract for `id` on L2 `l2_chain_id` (must not be registered yet).
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
            Op::Deploy { l2_chain_id, .. } => *l2_chain_id,
            Op::Upgrade { l2_chain_id, .. } => *l2_chain_id,
        }
    }

    pub fn id(&self) -> ContractId {
        match self {
            Op::Deploy { id, .. } => *id,
            Op::Upgrade { id, .. } => *id,
        }
    }

    /// Version the op targets — 1 for Deploy, `new_version` for Upgrade.
    pub fn version(&self) -> u64 {
        match self {
            Op::Deploy { .. } => 1,
            Op::Upgrade { new_version, .. } => *new_version,
        }
    }
}

/// Build a `DeploymentSpec` for a single `Op`. `target_impl` is always zero here; the
/// deployer's dedup pass fills it in after grouping by `(id, version)`.
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

/// Encode `initialize(args...)` as 4-byte selector || abi-encoded args.
pub fn encode_init_calldata(id: ContractId, abi_encoded_args: &Bytes) -> Bytes {
    let sel = id.init_selector();
    let mut out = Vec::with_capacity(4 + abi_encoded_args.len());
    out.extend_from_slice(&sel);
    out.extend_from_slice(abi_encoded_args);
    Bytes::from(out)
}

/// Convenience: abi-encode a single address argument for `initialize(address)`.
pub fn encode_address_arg(addr: Address) -> Bytes {
    let v = (addr,).abi_encode();
    Bytes::from(v)
}

/// abi-encode two address arguments, e.g. `ETHLockbox.initialize(l2Minter, outputOracle)`.
pub fn encode_address_pair(a: Address, b: Address) -> Bytes {
    Bytes::from((a, b).abi_encode_params())
}

/// abi-encode `WithdrawalOutputOracle.initialize(attester, challenger, window)`.
pub fn encode_oracle_init_args(attester: Address, challenger: Address, window: u64) -> Bytes {
    Bytes::from((attester, challenger, window).abi_encode_params())
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
