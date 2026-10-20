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
//! tie: it cross-checks the address, [`CHAIN_STATE_CODE`],
//! `SYSTEM_UPGRADER`, and the `setFeature` selector against the compiled
//! artifacts. The end-to-end test scenarios enforce it too, through the
//! live chain.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, U256, address, b256, hex};
use bytes::Bytes;

use crate::deposit::Deposit;

/// Canonical L2 address of the `KardamomChainState` predeploy. This is the
/// feature-flag store. A genesis can hold its code
/// (`chains/dev-withdrawals.toml`). On a genesis without the code, the
/// first `setFeature` system deposit installs [`CHAIN_STATE_CODE`] here
/// before its call (`kardamom_exec_core`, the deposit path).
pub const CHAIN_STATE: Address = address!("0x4200000000000000000000000000000000000017");

/// The runtime code of `KardamomChainState`: the code that [`CHAIN_STATE`]
/// holds. It is the code of the predeploy in
/// `chains/dev-withdrawals.toml` and the `deployedBytecode` of the compiled
/// contract. `crates/deployer/tests/chainstate_genesis_predeploy.rs` checks
/// both.
pub const CHAIN_STATE_CODE: &[u8] = &hex!(
    "6080806040526004361015610012575f80fd5b5f3560e01c9081630d5ea2131461020d575080635a5aa5b0146101df57806371b00f82146101c25780637633a22c1461018557806382afd23b146101445780638afdb854146100885763f5d8ea2214610069575f80fd5b34610084575f36600319011261008457602060405160018152f35b5f80fd5b346100845760403660031901126100845760043560243567ffffffffffffffff81168091036100845773454156dab0518b9244cc7ff1b0ffff6c7e031b6d3303610135578061010c57507f01759fab5a8f86a3d979de3aa967615a8a4044efe11641ec2140223227cbf9d96020425b835f525f82528060405f2055604051908152a2005b60207f01759fab5a8f86a3d979de3aa967615a8a4044efe11641ec2140223227cbf9d9916100f7565b63c54dbc7560e01b5f5260045ffd5b34610084576020366003190112610084576004355f525f602052602060405f2054801515908161017a575b506040519015158152f35b90504210158261016f565b34610084575f36600319011261008457606060015467ffffffffffffffff604051918181168352818160401c16602084015260801c166040820152f35b34610084575f366003190112610084576020600154604051908152f35b34610084575f36600319011261008457602060405173454156dab0518b9244cc7ff1b0ffff6c7e031b6d8152f35b34610084576020366003190112610084576020906004355f525f825260405f20548152f3fea164736f6c634300081a000a"
);

/// `keccak256(CHAIN_STATE_CODE)`: the code hash of the account at
/// [`CHAIN_STATE`]. A test in this module checks it.
pub const CHAIN_STATE_CODE_HASH: B256 =
    b256!("0xe7520549ae68dc696da6eb784926367e7f269129adef8b343a1399a41a339160");

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

/// The nonce burn of a skipped transaction. The executor reads it at each
/// skip (`kardamom_exec_core::executor::Skip::burn_state`): a skip at the
/// chain nonce of its sender then uses that nonce. The sealer runs decision
/// version 3 on every chain and does not read this feature. A replay of a
/// block reads it as the block did, so the executor keeps the check for
/// all time.
pub const FEATURE_SEALER_V3: u64 = 2;

/// Decision version 4 of the sealer: it orders a transaction reference by
/// the replay window of its ingress image, not by its id. Only the sealer
/// follows it: the sequencer sends its activation record in front of the
/// epoch of the `setFeature` deposit, and the sealer switches at the
/// boundary that seals the first block of that epoch. The executor does
/// not read it.
pub const FEATURE_SEALER_V4: u64 = 3;

/// The length of `setFeature` calldata: the selector and two words.
const SET_FEATURE_LEN: usize = 4 + 32 + 32;

/// A `setFeature(featureId, activationTimestamp)` call that a system
/// deposit carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureSchedule {
    pub feature_id: U256,
    /// Epoch milliseconds. `0` means "activate immediately".
    pub activation_timestamp: u64,
}

impl FeatureSchedule {
    /// The schedule of `deposit`: a system deposit to [`CHAIN_STATE`] with
    /// `setFeature` calldata. Any other deposit gives `None`.
    #[must_use]
    pub fn of_deposit(deposit: &Deposit) -> Option<Self> {
        (deposit.is_system_transaction && deposit.to == Some(CHAIN_STATE))
            .then(|| Self::decode(&deposit.input))
            .flatten()
    }

    /// Decode the calldata that [`encode_set_feature`] writes. A word of
    /// `activationTimestamp` above `u64` gives `None`.
    #[must_use]
    pub fn decode(calldata: &[u8]) -> Option<Self> {
        let words = calldata
            .strip_prefix(&SET_FEATURE_SELECTOR)
            .filter(|_| calldata.len() == SET_FEATURE_LEN)?;
        let (feature, timestamp) = words.split_at(32);
        Some(Self {
            feature_id: U256::from_be_slice(feature),
            activation_timestamp: u64::try_from(U256::from_be_slice(timestamp)).ok()?,
        })
    }
}

/// ABI-encode `setFeature(featureId, activationTimestamp)`.
///
/// This is hand-rolled, instead of using `sol!`, so this crate stays
/// `no_std` and free of `alloy-sol-types`. The layout is all of the ABI
/// encoding needed for two static words:
/// `selector(4) || pad32(featureId) || pad32(activationTimestamp)`.
///
/// `activation_timestamp` is in epoch milliseconds, this chain's
/// `block.timestamp` unit. `0` means "activate immediately".
#[must_use]
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
    fn chain_state_code_hash_is_the_hash_of_the_code() {
        assert_eq!(CHAIN_STATE_CODE_HASH, keccak256(CHAIN_STATE_CODE));
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
    fn a_schedule_decodes_from_the_calldata_that_encode_writes() {
        let calldata = encode_set_feature(U256::from(FEATURE_SEALER_V3), 1_700_000_000_250);
        assert_eq!(
            FeatureSchedule::decode(&calldata),
            Some(FeatureSchedule {
                feature_id: U256::from(FEATURE_SEALER_V3),
                activation_timestamp: 1_700_000_000_250,
            })
        );
        assert_eq!(FeatureSchedule::decode(&calldata[..67]), None);
        let mut wide = calldata.to_vec();
        wide[36] = 1;
        assert_eq!(FeatureSchedule::decode(&wide), None, "a time above u64");
        wide[..4].copy_from_slice(&[0; 4]);
        assert_eq!(FeatureSchedule::decode(&wide), None, "another selector");
    }

    #[test]
    fn only_a_system_deposit_to_the_chain_state_carries_a_schedule() {
        let system = Deposit {
            source_hash: alloy_primitives::B256::ZERO,
            from: SYSTEM_UPGRADER,
            to: Some(CHAIN_STATE),
            mint: 0,
            value: U256::ZERO,
            gas_limit: UPGRADE_TX_GAS_LIMIT,
            is_system_transaction: true,
            input: encode_set_feature(U256::from(FEATURE_SEALER_V3), 0),
        };
        assert!(FeatureSchedule::of_deposit(&system).is_some());
        let user = Deposit {
            is_system_transaction: false,
            ..system.clone()
        };
        assert_eq!(FeatureSchedule::of_deposit(&user), None);
        let elsewhere = Deposit {
            to: Some(SYSTEM_UPGRADER),
            ..system
        };
        assert_eq!(FeatureSchedule::of_deposit(&elsewhere), None);
    }

    #[test]
    fn chain_state_sits_next_to_the_message_passer() {
        // The 0x42..00 predeploy namespace is allocated densely, by hand. A
        // collision with the message passer would be silent and severe.
        assert_ne!(CHAIN_STATE, crate::withdrawals::MESSAGE_PASSER);
        assert_eq!(CHAIN_STATE.as_slice()[19], 0x17);
    }
}
