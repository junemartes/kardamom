//! Smoke test: settlement sol! binding loads + selector matches contract.

use alloy_primitives::{Address, B256, keccak256};
use alloy_sol_types::SolCall;
use kardamom_batcher::settlement::{
    IKardamomL2Settlement, PostBatchParams, versioned_hashes_from_commitments,
};

#[test]
fn post_batch_selector_matches_signature() {
    let computed = &keccak256(b"postBatch(uint64,bytes32[],uint64,uint64,bytes32)")[..4];
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
fn versioned_hashes_use_kzg_version_byte() {
    let commitments = [[0u8; 48], [0xAA; 48]];
    let hashes = versioned_hashes_from_commitments(&commitments);
    assert_eq!(hashes.len(), 2);
    for h in &hashes {
        assert_eq!(
            h[0], 0x01,
            "versioned hash must begin with kzg version 0x01"
        );
    }
}

#[test]
fn post_batch_params_constructor_rejects_mismatched_lengths() {
    use alloy_eips::eip4844::Blob;
    let blobs = vec![Blob::default(), Blob::default()];
    let hashes = vec![B256::ZERO];
    let res = PostBatchParams::new(
        Address::ZERO,
        0,
        blobs,
        hashes,
        1,
        2,
        B256::repeat_byte(0x4C),
    );
    assert!(res.is_err());
}

#[test]
fn post_batch_params_rejects_inverted_block_range() {
    use alloy_eips::eip4844::Blob;
    let res = PostBatchParams::new(
        Address::ZERO,
        0,
        vec![Blob::default()],
        vec![B256::ZERO],
        10,
        5,
        B256::repeat_byte(0x4C),
    );
    assert!(res.is_err());
}
