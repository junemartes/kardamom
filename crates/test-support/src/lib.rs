//! The one copy of the sign-and-wrap test fixture.
//!
//! A test that needs a real signed transaction builds a [`LegacyTx`],
//! then calls [`LegacyTx::sign`]. The result is a
//! [`kardamom_types::TxEnvelope`] with the same shape the proxy hands
//! downstream: the EIP-2718 bytes, the keccak of those bytes as
//! `tx_hash`, and the signer's address as `sender`.
//!
//! The signer helpers give a test a stable key without a real RNG:
//! [`seeded_signer`] for a small integer seed, [`byte_signer`] for a
//! repeated key byte, and [`anvil_signer_0`] for the public Anvil dev
//! key.
//!
//! This crate is a leaf. It depends on `kardamom-types` and nothing above
//! it, so every crate in the workspace can use it as a dev-dependency.

use alloy_consensus::{SignableTransaction, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes as AlloyBytes, TxKind, U256, keccak256};
use alloy_signer_local::PrivateKeySigner;
use bytes::Bytes;
use kardamom_types::TxEnvelope;

/// The fields of a legacy transaction, ready to sign.
///
/// Set the fields a test cares about and fill the rest from
/// [`Default`]: chain id 1, recipient `Address::ZERO`, nonce 0, value 0,
/// gas limit 21,000, gas price 0, empty input, correlation id 0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LegacyTx {
    /// The chain id the signature commits to.
    pub chain_id: u64,
    /// The recipient. The fixture only signs calls, never creates.
    pub to: Address,
    /// The sender nonce.
    pub nonce: u64,
    /// The value in wei.
    pub value: u64,
    /// The gas limit.
    pub gas_limit: u64,
    /// The legacy gas price in wei.
    pub gas_price: u128,
    /// The calldata.
    pub input: AlloyBytes,
    /// The `correlation_id` the envelope carries. It is not signed.
    pub correlation_id: u64,
}

impl Default for LegacyTx {
    fn default() -> Self {
        Self {
            chain_id: 1,
            to: Address::ZERO,
            nonce: 0,
            value: 0,
            gas_limit: 21_000,
            gas_price: 0,
            input: AlloyBytes::new(),
            correlation_id: 0,
        }
    }
}

impl LegacyTx {
    /// Sign this transaction as a call to `to` and wrap it as a
    /// [`TxEnvelope`].
    ///
    /// `raw_tx` holds the EIP-2718 bytes. `tx_hash` is the keccak of
    /// those bytes. `sender` is the signer's address.
    ///
    /// # Panics
    ///
    /// Panics if `signer` cannot sign the transaction. A
    /// `PrivateKeySigner` signs every well-formed `TxLegacy`, so a panic
    /// here is a fixture bug, not a test result.
    #[must_use]
    pub fn sign(self, signer: &PrivateKeySigner) -> TxEnvelope {
        let kind = TxKind::Call(self.to);
        self.signed(kind, signer)
    }

    /// Sign this transaction as a contract creation and wrap it as a
    /// [`TxEnvelope`]. `to` is not used. `input` is the init code.
    ///
    /// # Panics
    ///
    /// Panics if `signer` cannot sign the transaction, as [`Self::sign`]
    /// does.
    #[must_use]
    pub fn sign_create(self, signer: &PrivateKeySigner) -> TxEnvelope {
        self.signed(TxKind::Create, signer)
    }

    /// The one sign-and-wrap body behind [`Self::sign`] and
    /// [`Self::sign_create`].
    fn signed(self, to: TxKind, signer: &PrivateKeySigner) -> TxEnvelope {
        let mut tx = TxLegacy {
            chain_id: Some(self.chain_id),
            nonce: self.nonce,
            gas_price: self.gas_price,
            gas_limit: self.gas_limit,
            to,
            value: U256::from(self.value),
            input: self.input,
        };
        let sig = signer
            .sign_transaction_sync(&mut tx)
            .expect("a PrivateKeySigner signs a well-formed TxLegacy");
        let alloy_env: alloy_consensus::TxEnvelope = tx.into_signed(sig).into();
        let raw_tx = Bytes::from(alloy_env.encoded_2718());
        let tx_hash = keccak256(&raw_tx);
        TxEnvelope {
            correlation_id: self.correlation_id,
            raw_tx,
            sender: signer.address(),
            tx_hash,
        }
    }
}

/// A signer from a small integer seed. The seed fills the last eight
/// bytes of the private key, big-endian.
///
/// # Panics
///
/// Panics if `seed` is 0: a private key of 0 is not a valid secp256k1
/// scalar.
#[must_use]
pub fn seeded_signer(seed: u64) -> PrivateKeySigner {
    let mut k = [0u8; 32];
    k[24..].copy_from_slice(&seed.to_be_bytes());
    PrivateKeySigner::from_bytes(&k.into()).expect("a non-zero seed is a valid private key")
}

/// A signer whose private key is `byte` repeated 32 times.
///
/// # Panics
///
/// Panics if `byte` is 0: a private key of 0 is not a valid secp256k1
/// scalar.
#[must_use]
pub fn byte_signer(byte: u8) -> PrivateKeySigner {
    PrivateKeySigner::from_bytes(&B256::repeat_byte(byte))
        .expect("a non-zero repeated byte is a valid private key")
}

/// The Anvil dev key #0. The key is public and for development only.
///
/// # Panics
///
/// Does not panic: the key literal is a valid private key.
#[must_use]
pub fn anvil_signer_0() -> PrivateKeySigner {
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        .parse()
        .expect("the Anvil dev key #0 literal parses")
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::Transaction;
    use alloy_consensus::transaction::SignerRecoverable;
    use alloy_eips::eip2718::Decodable2718;

    #[test]
    fn default_values_match_the_doc() {
        let tx = LegacyTx::default();
        assert_eq!(tx.chain_id, 1);
        assert_eq!(tx.to, Address::ZERO);
        assert_eq!(tx.nonce, 0);
        assert_eq!(tx.value, 0);
        assert_eq!(tx.gas_limit, 21_000);
        assert_eq!(tx.gas_price, 0);
        assert!(tx.input.is_empty());
        assert_eq!(tx.correlation_id, 0);
    }

    #[test]
    fn sender_recovers_from_raw_tx() {
        let signer = seeded_signer(7);
        let env = LegacyTx {
            nonce: 3,
            value: 5,
            correlation_id: 42,
            ..Default::default()
        }
        .sign(&signer);
        let decoded = alloy_consensus::TxEnvelope::decode_2718(&mut env.raw_tx.as_ref())
            .expect("raw_tx decodes as a 2718 envelope");
        assert_eq!(decoded.recover_signer().unwrap(), signer.address());
        assert_eq!(env.sender, signer.address());
        assert_eq!(env.correlation_id, 42);
    }

    #[test]
    fn sign_create_has_no_recipient() {
        let signer = byte_signer(0x11);
        let env = LegacyTx {
            input: AlloyBytes::from_static(&[0x60, 0x00]),
            ..Default::default()
        }
        .sign_create(&signer);
        let decoded = alloy_consensus::TxEnvelope::decode_2718(&mut env.raw_tx.as_ref())
            .expect("raw_tx decodes as a 2718 envelope");
        assert_eq!(decoded.to(), None);
        assert_eq!(decoded.recover_signer().unwrap(), signer.address());
    }

    #[test]
    fn tx_hash_is_the_keccak_of_raw_tx() {
        let env = LegacyTx::default().sign(&anvil_signer_0());
        assert_eq!(env.tx_hash, keccak256(&env.raw_tx));
    }

    #[test]
    fn seeded_signer_sets_the_last_key_byte() {
        let by_byte = PrivateKeySigner::from_bytes(&B256::with_last_byte(5)).unwrap();
        assert_eq!(seeded_signer(5).address(), by_byte.address());
    }
}
