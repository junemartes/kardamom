//! The feature activation that a sequencer derives from an epoch.

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, U256};
use kardamom_types::epoch::{DepositLog, LockboxLog, UpgradeLog, derive_epoch};
use kardamom_types::upgrades::FEATURE_SEALER_V3;

use super::*;

const BLOCK: B256 = B256::repeat_byte(0x77);

fn upgrade(index: u64, feature: u64, activation: u64) -> LockboxLog {
    LockboxLog::Upgrade(UpgradeLog {
        block_number: 9,
        block_hash: BLOCK,
        log_index: index,
        feature_id: U256::from(feature),
        activation_timestamp: activation,
    })
}

fn deposit(index: u64) -> LockboxLog {
    LockboxLog::Deposit(DepositLog {
        block_number: 9,
        block_hash: BLOCK,
        log_index: index,
        from: Address::repeat_byte(0xA1),
        to: Address::repeat_byte(0xB2),
        mint: 10,
        gas_limit: 200_000,
        data: AlloyBytes::new(),
    })
}

fn activation_of(logs: &[LockboxLog]) -> Option<FeatureActivation> {
    FeatureActivation::of(&derive_epoch(9, BLOCK, logs).unwrap(), FEATURE_SEALER_V3)
}

#[test]
fn an_epoch_with_the_upgrade_of_the_feature_carries_its_activation() {
    assert_eq!(
        activation_of(&[deposit(0), upgrade(1, FEATURE_SEALER_V3, 5_000)]),
        Some(FeatureActivation {
            l1_origin: 9,
            feature_id: FEATURE_SEALER_V3,
            activation_ms: 5_000,
        })
    );
}

#[test]
fn the_last_upgrade_of_the_feature_in_the_epoch_wins() {
    let logs = [
        upgrade(0, FEATURE_SEALER_V3, 5_000),
        upgrade(1, FEATURE_SEALER_V3, 9_000),
    ];
    assert_eq!(activation_of(&logs).map(|a| a.activation_ms), Some(9_000));
}

#[test]
fn an_epoch_without_the_upgrade_carries_no_activation() {
    assert_eq!(activation_of(&[deposit(0)]), None);
    assert_eq!(activation_of(&[upgrade(0, 1, 0)]), None, "another feature");
    assert_eq!(activation_of(&[]), None);
}

#[test]
fn the_frame_has_its_fields_at_the_sealer_offsets() {
    let frame = FeatureActivation {
        l1_origin: 5,
        feature_id: 2,
        activation_ms: 1_700_000_000_250,
    }
    .encode();
    assert_eq!(frame.len(), FeatureActivation::LEN);
    assert_eq!(frame[0], KIND_FEATURE_ACTIVATION);
    assert_eq!(frame[1..9], 5_u64.to_le_bytes());
    assert_eq!(frame[9..17], 2_u64.to_le_bytes());
    assert_eq!(frame[17..25], 1_700_000_000_250_u64.to_le_bytes());
}
