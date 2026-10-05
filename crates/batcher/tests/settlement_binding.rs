//! Smoke test: the settlement sol! binding loads, and its selector matches
//! the contract.

use alloy_primitives::{Address, B256, Bytes, keccak256};
use alloy_sol_types::SolCall;
use kardamom_batcher::settlement::{IKardamomL2Settlement, PostBatchParams};

#[test]
fn post_batch_selector_matches_signature() {
    let computed = &keccak256(b"postBatch(uint64,bytes,uint64,uint64,bytes32)")[..4];
    assert_eq!(
        IKardamomL2Settlement::postBatchCall::SELECTOR.as_slice(),
        computed
    );
}

#[test]
fn initialize_selector_matches_signature() {
    let computed = &keccak256(b"initialize(address)")[..4];
    assert_eq!(
        IKardamomL2Settlement::initializeCall::SELECTOR.as_slice(),
        computed
    );
}

#[test]
fn post_batch_params_constructor_rejects_an_empty_certificate() {
    let res = PostBatchParams::new(
        Address::ZERO,
        0,
        Bytes::new(),
        1,
        2,
        B256::repeat_byte(0x4C),
    );
    assert!(res.is_err());
}

#[test]
fn post_batch_params_rejects_inverted_block_range() {
    let res = PostBatchParams::new(
        Address::ZERO,
        0,
        Bytes::from(vec![0x02, 0xC0, 0xFF, 0xEE]),
        10,
        5,
        B256::repeat_byte(0x4C),
    );
    assert!(res.is_err());
}
