//! Creation bytecode embedded at build time from `contracts/out/`.
//!
//! `build.rs` generates this file with `foundry-compilers` and
//! `include_bytes!`. The CI `bytecode-hash` job pins it against drift. That
//! job rebuilds `KardamomFactoryV1` with `forge build` and checks that the
//! runtime bytecode sha256 matches `contracts/expected_bytecode_hash.txt`.
//! Forge and the cargo-side `foundry-compilers` both compile with
//! `bytecode_hash = "none"` (set in `contracts/foundry.toml`), so they
//! produce byte-identical output. This means the gate that pins forge's
//! output also pins what this module embeds.

use alloy_primitives::Bytes;

include!(concat!(env!("OUT_DIR"), "/embedded_artifacts.rs"));

/// Creation bytecode of `KardamomFactoryV1`. The ERC-7955 self-bootstrap uses it.
pub fn factory_v1_creation() -> Bytes {
    Bytes::from_static(KARDAMOM_FACTORY_V1_CREATION)
}

/// Creation bytecode of OpenZeppelin's `ERC1967Proxy`. It is used to compute
/// the kardamom factory proxy CREATE2 address.
pub fn erc1967_proxy_creation() -> Bytes {
    Bytes::from_static(ERC1967_PROXY_CREATION)
}

/// Creation bytecode of `ETHLockbox`. The factory does a CREATE2 of this on demand.
pub fn eth_lockbox_creation() -> Bytes {
    Bytes::from_static(ETH_LOCKBOX_CREATION)
}

/// Creation bytecode of `KardamomL2Settlement`, the DA sink contract.
pub fn kardamom_l2_settlement_creation() -> Bytes {
    Bytes::from_static(KARDAMOM_L2_SETTLEMENT_CREATION)
}

/// Creation bytecode of `KardamomProofOracle`, the zk root chain.
pub fn kardamom_proof_oracle_creation() -> Bytes {
    Bytes::from_static(KARDAMOM_PROOF_ORACLE_CREATION)
}

/// Creation bytecode of `WithdrawalOutputOracle`, the withdrawal output root
/// registry. The factory does a CREATE2 of this on demand.
pub fn withdrawal_output_oracle_creation() -> Bytes {
    Bytes::from_static(WITHDRAWAL_OUTPUT_ORACLE_CREATION)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_v1_creation_is_nonempty() {
        assert!(!factory_v1_creation().is_empty());
    }

    #[test]
    fn erc1967_proxy_creation_is_nonempty() {
        assert!(!erc1967_proxy_creation().is_empty());
    }

    #[test]
    fn eth_lockbox_creation_is_nonempty() {
        assert!(!eth_lockbox_creation().is_empty());
    }

    #[test]
    fn kardamom_l2_settlement_creation_is_nonempty() {
        assert!(!kardamom_l2_settlement_creation().is_empty());
    }

    #[test]
    fn withdrawal_output_oracle_creation_is_nonempty() {
        assert!(!withdrawal_output_oracle_creation().is_empty());
    }
}
