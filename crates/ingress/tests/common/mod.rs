//! Shared test helpers used by the `kardamom-ingress` integration tests.
//! Lives in `tests/common/mod.rs` so cargo treats it as a module (not a
//! test binary).

#![allow(dead_code)]

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_primitives::{Address, Bytes, Signature, TxKind, U256};
use alloy_rlp::Encodable;
use alloy_signer_local::PrivateKeySigner;
use k256::ecdsa::{RecoveryId, signature::hazmat::PrehashSigner};

/// Sign a fresh legacy tx with `signer` at `nonce` and return both the
/// decoded envelope and its RLP bytes plus the signer address.
pub fn sign_legacy_tx(signer: &PrivateKeySigner, nonce: u64) -> (TxEnvelope, Bytes, Address) {
    let addr = signer.address();
    let tx = TxLegacy {
        chain_id: Some(1),
        nonce,
        gas_price: 1_000_000_000,
        gas_limit: 21_000,
        to: TxKind::Call(Address::ZERO),
        value: U256::ZERO,
        input: Default::default(),
    };
    let (k256_sig, rid): (k256::ecdsa::Signature, RecoveryId) = signer
        .credential()
        .sign_prehash(tx.signature_hash().as_slice())
        .unwrap();
    let alloy_sig = Signature::from_signature_and_parity(k256_sig, rid.is_y_odd());
    let signed = tx.into_signed(alloy_sig);
    let env: TxEnvelope = signed.into();
    let mut buf = Vec::new();
    env.encode(&mut buf);
    (env, Bytes::from(buf), addr)
}

/// Sign a fresh legacy tx and return only the RLP bytes.
pub fn sign_legacy(signer: &PrivateKeySigner, nonce: u64) -> Bytes {
    sign_legacy_tx(signer, nonce).1
}
