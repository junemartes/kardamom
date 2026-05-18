//! Build `DeploymentSpec` values consumed by `KardamomFactoryV1.applyDeployments`.
//!
//! A spec encodes everything the factory needs to deploy or upgrade one contract:
//! creation bytecode, init calldata, salts. The factory itself is dumb about
//! contract types — the Rust side knows the init signatures and version scheme.

use std::path::Path;

use alloy_primitives::{Address, B256, Bytes};
use alloy_sol_types::SolValue;

use crate::artifacts::{ArtifactError, creation_bytecode};
use crate::ids::ContractId;

/// One-to-one with `IKardamomFactory.DeploymentSpec` on the Solidity side.
#[derive(Debug, Clone)]
pub struct DeploymentSpec {
    pub id: B256,
    pub action: Action,
    pub impl_initcode: Bytes,
    pub init_data: Bytes,
    pub impl_salt: B256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Deploy = 0,
    Upgrade = 1,
}

/// What the operator wants to happen for one contract.
#[derive(Debug, Clone)]
pub enum Op {
    /// Deploy a fresh contract for `id` (must not be registered yet).
    Deploy { id: ContractId, init_args: Bytes },
    /// Upgrade an existing contract to `new_version`. `init_args` is the calldata
    /// passed to `upgradeToAndCall` (empty by default; reserved for future
    /// migration payloads).
    Upgrade {
        id: ContractId,
        new_version: u64,
        init_args: Bytes,
    },
}

/// Build a `DeploymentSpec` for a single `Op` by reading the forge artifact for
/// the contract's impl.
pub fn build_spec(contracts_root: &Path, op: &Op) -> Result<DeploymentSpec, ArtifactError> {
    match op {
        Op::Deploy { id, init_args } => {
            let impl_initcode = creation_bytecode(contracts_root, id.artifact_name())?;
            let init_data = encode_init_calldata(*id, init_args);
            Ok(DeploymentSpec {
                id: id.id(),
                action: Action::Deploy,
                impl_initcode,
                init_data,
                impl_salt: id.impl_salt(1),
            })
        }
        Op::Upgrade {
            id,
            new_version,
            init_args,
        } => {
            let impl_initcode = creation_bytecode(contracts_root, id.artifact_name())?;
            // For upgrades, callers can pass empty init_args to do a plain
            // upgradeToAndCall(..., ""). Non-empty init_args is sent as-is —
            // the caller is responsible for encoding selector + args.
            Ok(DeploymentSpec {
                id: id.id(),
                action: Action::Upgrade,
                impl_initcode,
                init_data: init_args.clone(),
                impl_salt: id.impl_salt(*new_version),
            })
        }
    }
}

/// Encode `initialize(args...)` as 4-byte selector || abi-encoded args.
/// The caller supplies `abi_encoded_args` already abi-encoded; we prepend the
/// selector derived from the contract's known init signature.
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

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

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
        // address goes in the low 20 bytes (right-aligned) — high 12 bytes are zero.
        assert!(arg[..12].iter().all(|&b| b == 0));
        assert_eq!(arg[31], 0x01);
    }
}
