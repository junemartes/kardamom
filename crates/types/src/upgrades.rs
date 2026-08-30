//! L1-governed upgrades. This module holds the constants and calldata
//! layout shared by the derivation rule, the L1 contracts, and the
//! execution engine.
//!
//! An upgrade transaction is an L1 transaction to
//! `ETHLockbox.initiateUpgrade`, authorized to the factory owner (a Safe in
//! production). It emits `UpgradeInitiated`. The DA watcher picks this up,
//! alongside `DepositInitiated`, and turns it into a system deposit: a
//! deposit with a domain-1 source hash, `is_system_transaction = true`,
//! sender [`SYSTEM_UPGRADER`], and calldata that calls
//! `KardamomChainState.setFeature` on [`CHAIN_STATE`].
//!
//! These definitions are the single source of truth for the byte layouts
//! the L2 predeploy sees. They must stay identical to
//! `contracts/src/L2/KardamomChainState.sol` and
//! `contracts/src/L1/ETHLockbox.sol`.
//! `crates/deployer/tests/chainstate_genesis_predeploy.rs` enforces the
//! tie: it cross-checks the address, bytecode, `SYSTEM_UPGRADER`, and the
//! `setFeature` selector against the compiled artifacts. The end-to-end
//! test scenarios enforce it too, through the live chain.
//!
//! See `docs/specs/2026-08-16-l1-upgrade-feature-flags-design.md`.

use alloc::vec::Vec;

use alloy_primitives::{Address, U256, address};
use bytes::Bytes;

/// Canonical L2 address of the `KardamomChainState` predeploy. This is the
/// feature-flag store. It is seeded into genesis; see `chains/dev-withdrawals.toml`.
pub const CHAIN_STATE: Address = address!("0x4200000000000000000000000000000000000017");

/// Synthetic L2 sender for system upgrade deposits. This is the last 20
/// bytes of `keccak256("kardamom.upgrades.system-sender.v1")`.
///
/// Only [`crate::epoch::derive_epoch`] mints deposits from this address. No
/// one else can forge one. A user deposit carries an aliased L1 sender
/// (`+0x1111...1111`), so producing this sender from L1 would need
/// inverting keccak to find the pre-alias L1 address. No key signs for a
/// hash-derived address on L2, either. `KardamomChainState.setFeature`
/// re-checks the sender on the contract side, as defense in depth.
pub const SYSTEM_UPGRADER: Address = address!("0x454156dAb0518B9244CC7Ff1b0FfFf6c7E031B6D");

/// Gas limit stamped on every system upgrade deposit. `setFeature` costs
/// one `SSTORE` plus an event, about 50k gas. The extra headroom costs
/// nothing: deposits execute with `gas_price = 0` and are not metered
/// against a block budget.
pub const UPGRADE_TX_GAS_LIMIT: u64 = 1_000_000;

/// Selector of `KardamomChainState.setFeature(uint256,uint64)`.
pub const SET_FEATURE_SELECTOR: [u8; 4] = [0x8a, 0xfd, 0xb8, 0x54];

/// ABI-encode `setFeature(featureId, activationTimestamp)`.
///
/// This is hand-rolled, instead of using `sol!`, so this crate stays
/// `no_std` and free of `alloy-sol-types`. The layout is all of the ABI
/// encoding needed for two static words:
/// `selector(4) || pad32(featureId) || pad32(activationTimestamp)`.
///
/// `activation_timestamp` is in epoch milliseconds, this chain's
/// `block.timestamp` unit. `0` means "activate immediately".
pub fn encode_set_feature(feature_id: U256, activation_timestamp: u64) -> Bytes {
    let mut out = Vec::with_capacity(68);
    out.extend_from_slice(&SET_FEATURE_SELECTOR);
    out.extend_from_slice(&feature_id.to_be_bytes::<32>());
    out.extend_from_slice(&U256::from(activation_timestamp).to_be_bytes::<32>());
    Bytes::from(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::keccak256;

    #[test]
    fn system_upgrader_is_the_documented_keccak_suffix() {
        // The address is derived, not chosen. Anyone can recompute it. A typo
        // in the constant cannot silently create an address that someone
        // might hold a key for.
        let h = keccak256("kardamom.upgrades.system-sender.v1");
        assert_eq!(SYSTEM_UPGRADER, Address::from_slice(&h[12..]));
    }

    #[test]
    fn set_feature_selector_matches_the_signature() {
        let h = keccak256("setFeature(uint256,uint64)");
        assert_eq!(SET_FEATURE_SELECTOR, h[..4]);
    }

    #[test]
    fn encodes_set_feature_as_selector_plus_two_words() {
        let calldata = encode_set_feature(U256::from(1u64), 0);
        assert_eq!(calldata.len(), 68);
        assert_eq!(&calldata[..4], &SET_FEATURE_SELECTOR);
        // featureId = 1 in the last byte of the first word.
        assert_eq!(calldata[4 + 31], 1);
        // activationTimestamp = 0 across the whole second word.
        assert!(calldata[36..68].iter().all(|b| *b == 0));
    }

    #[test]
    fn encodes_a_millisecond_activation_timestamp_big_endian() {
        let ts = 1_700_000_000_250u64;
        let calldata = encode_set_feature(U256::from(7u64), ts);
        assert_eq!(calldata[4 + 31], 7);
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&ts.to_be_bytes());
        assert_eq!(&calldata[36..68], &word);
    }

    #[test]
    fn chain_state_sits_next_to_the_message_passer() {
        // The 0x42..00 predeploy namespace is allocated densely, by hand. A
        // collision with the message passer would be silent and severe.
        assert_ne!(CHAIN_STATE, crate::withdrawals::MESSAGE_PASSER);
        assert_eq!(CHAIN_STATE.as_slice()[19], 0x17);
    }
}
