use alloy_primitives::{Address, B256, U256};
use serde_json::json;

use super::{Genesis, GenesisAlloc, GenesisError};
use crate::delta::StorageChange;
use crate::upgrades::{
    CHAIN_STATE, CHAIN_STATE_CODE, CHAIN_STATE_CODE_HASH, FEATURE_CARRY_WINDOW, FEATURE_SEALER_V4,
    FEATURE_SEALER_V5, GENESIS_ACTIVATION_MS, GenesisFeatures, activation_slot,
};

fn parse(value: serde_json::Value) -> Result<Genesis, serde_json::Error> {
    serde_json::from_value(value)
}

fn funded(features: &serde_json::Value) -> Genesis {
    parse(json!({
        "chain_id": 7,
        "alloc": [{ "address": Address::repeat_byte(0x11), "balance": "5" }],
        "genesis_features": features,
    }))
    .unwrap()
}

#[test]
fn an_absent_list_is_no_feature_and_keeps_the_allocation() {
    let genesis = parse(json!({ "chain_id": 7, "alloc": [] })).unwrap();
    assert_eq!(genesis.genesis_features, GenesisFeatures::None);
    assert_eq!(genesis.to_alloc(), GenesisAlloc::default());
}

#[test]
fn the_carry_genesis_holds_the_chain_state_code_and_three_words() {
    let alloc = funded(&json!([3, 4, 5])).to_alloc();
    let chain_state = alloc
        .accounts
        .iter()
        .find(|a| a.address == CHAIN_STATE)
        .unwrap();
    assert_eq!((chain_state.nonce, chain_state.balance), (0, U256::ZERO));
    assert_eq!(chain_state.code_hash, CHAIN_STATE_CODE_HASH);
    assert_eq!(alloc.code.len(), 1);
    assert_eq!(alloc.code[0].code.as_ref(), CHAIN_STATE_CODE);
    let word = |id| StorageChange {
        address: CHAIN_STATE,
        key: activation_slot(id),
        value: U256::from(GENESIS_ACTIVATION_MS),
    };
    assert_eq!(
        alloc.storage,
        [
            word(FEATURE_SEALER_V4),
            word(FEATURE_SEALER_V5),
            word(FEATURE_CARRY_WINDOW)
        ]
    );
}

#[test]
fn a_chain_state_entry_keeps_its_balance_and_its_code_once() {
    let with = |code: &str| {
        parse(json!({
            "chain_id": 7,
            "alloc": [{ "address": CHAIN_STATE, "balance": "9", "nonce": 1, "code": code }],
            "genesis_features": [3],
        }))
        .unwrap()
    };
    let bare = with("0x").to_alloc();
    assert_eq!(bare.accounts.len(), 1);
    assert_eq!(bare.accounts[0].balance, U256::from(9u64));
    assert_eq!(bare.accounts[0].nonce, 1);
    assert_eq!(bare.accounts[0].code_hash, CHAIN_STATE_CODE_HASH);
    assert_eq!(bare.code.len(), 1);

    let predeployed = with(&alloy_primitives::hex::encode_prefixed(CHAIN_STATE_CODE)).to_alloc();
    assert_eq!(predeployed.accounts, bare.accounts);
    assert_eq!(predeployed.code, bare.code);
    assert_eq!(
        predeployed.storage,
        [StorageChange {
            address: CHAIN_STATE,
            key: activation_slot(FEATURE_SEALER_V4),
            value: U256::from(GENESIS_ACTIVATION_MS),
        }]
    );
}

#[test]
fn features_refuse_other_code_at_the_chain_state_address() {
    let genesis = parse(json!({
        "chain_id": 7,
        "alloc": [{ "address": CHAIN_STATE, "code": "0x6000" }],
        "genesis_features": [3, 4, 5],
    }))
    .unwrap();
    assert!(matches!(
        genesis.validate(),
        Err(GenesisError::ChainStateCode)
    ));
    let plain = Genesis {
        genesis_features: GenesisFeatures::None,
        ..genesis
    };
    plain.validate().unwrap();
}

#[test]
fn the_activation_slot_follows_the_solidity_mapping_rule() {
    let mut buf = [0u8; 64];
    buf[31] = 5;
    assert_eq!(activation_slot(5), alloy_primitives::keccak256(buf));
    assert_ne!(activation_slot(5), B256::ZERO);
}
