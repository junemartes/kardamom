//! Pre-signed-transaction queue built from mnemonic-derived signers.

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, Bytes, TxKind, U256};
use alloy_signer_local::PrivateKeySigner;

/// The signer count a `--senders` count of derived signers plus one
/// extra deployer signer needs, as `u32` (what
/// `mnemonic::derive_signers` and `SignerSet::derive` take). `stm-p0`
/// and `stm-p2` both derive one extra signer beyond `--senders`, for
/// the workload's deployer.
///
/// # Errors
///
/// Returns an error if `senders.get() + 1` overflows `u32`.
pub fn signer_count(senders: std::num::NonZeroUsize) -> anyhow::Result<u32> {
    u32::try_from(senders.get())
        .ok()
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| anyhow::anyhow!("--senders + 1 must fit in u32"))
}

/// One signer derived from a BIP-39 mnemonic along m/44'/60'/0'/0/N.
#[derive(Debug, Clone)]
pub struct DerivedSigner {
    /// Concrete secp256k1 signer ready to sign transactions.
    pub signer: PrivateKeySigner,
    /// The signer's Ethereum address. Cached for convenience.
    pub address: Address,
}

/// One transaction's EIP-2718-encoded raw bytes and its hash, from
/// [`DerivedSigner::sign_raw`].
pub struct SignedRaw {
    pub raw: Bytes,
    pub hash: alloy_primitives::B256,
}

impl DerivedSigner {
    /// Sign `tx` and wrap the result as a [`kardamom_types::TxEnvelope`],
    /// ready to submit.
    ///
    /// # Errors
    ///
    /// Returns an error if signing fails with a k256 signer error.
    pub fn sign_envelope(&self, tx: TxLegacy) -> anyhow::Result<kardamom_types::TxEnvelope> {
        let signed = self.sign_raw(tx)?;
        Ok(kardamom_types::TxEnvelope {
            correlation_id: 0,
            raw_tx: signed.raw.into(),
            sender: self.address,
            tx_hash: signed.hash,
        })
    }

    /// Sign `tx` and return its EIP-2718-encoded raw bytes and its hash,
    /// without an envelope wrapper. Used where only the wire bytes are
    /// needed, such as a pre-signed submit queue.
    ///
    /// # Errors
    ///
    /// Returns an error if signing fails with a k256 signer error.
    pub fn sign_raw(&self, mut tx: TxLegacy) -> anyhow::Result<SignedRaw> {
        let sig = self
            .signer
            .sign_transaction_sync(&mut tx)
            .map_err(|e| anyhow::anyhow!("signing tx: {e}"))?;
        let signed = tx.into_signed(sig);
        let hash = *signed.hash();
        let envelope: TxEnvelope = signed.into();
        let mut bytes = Vec::with_capacity(110);
        envelope.encode_2718(&mut bytes);
        Ok(SignedRaw {
            raw: Bytes::from(bytes),
            hash,
        })
    }
}

/// A non-empty set of derived signers. [`SignerSet::new`] and
/// [`SignerSet::derive`] are the only ways to build one, so every
/// function that takes `&SignerSet` can trust the set is non-empty and
/// never re-checks.
#[derive(Debug, Clone)]
pub struct SignerSet(Vec<DerivedSigner>);

impl SignerSet {
    /// Wrap already-derived signers.
    ///
    /// # Errors
    ///
    /// Returns an error if `signers` is empty.
    pub fn new(signers: Vec<DerivedSigner>) -> anyhow::Result<Self> {
        if signers.is_empty() {
            anyhow::bail!("at least one signer is required");
        }
        Ok(Self(signers))
    }

    /// Derive `count` signers from `phrase` and wrap them.
    ///
    /// # Errors
    ///
    /// Returns an error if `count` is 0, or if BIP-32 derivation fails.
    pub fn derive(phrase: &str, count: u32) -> anyhow::Result<Self> {
        Self::new(crate::mnemonic::derive_signers(phrase, count)?)
    }

    /// The first signer. Infallible: a `SignerSet` is never empty. Used
    /// as the deployer for a workload's setup transactions.
    #[must_use]
    pub fn deployer(&self) -> &DerivedSigner {
        &self.0[0]
    }
}

impl std::ops::Deref for SignerSet {
    type Target = [DerivedSigner];

    fn deref(&self) -> &[DerivedSigner] {
        &self.0
    }
}

impl<'a> IntoIterator for &'a SignerSet {
    type Item = &'a DerivedSigner;
    type IntoIter = std::slice::Iter<'a, DerivedSigner>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Pre-sign `count` EIP-2718-encoded legacy value transfers.
/// This function rotates the transfers across `signers`.
///
/// Each signer's nonces start at `nonce_base` and increase in order.
/// The function returns the raw bytes for each transaction, in
/// round-robin dispatch order.
///
/// # Errors
///
/// Returns an error if signing a transaction fails with a k256 signer
/// error.
pub fn presign_transfers(
    signers: &SignerSet,
    chain_id: u64,
    to: Address,
    value: U256,
    count: usize,
    nonce_base: u64,
) -> anyhow::Result<Vec<Bytes>> {
    let txs_per_signer = count.div_ceil(signers.len());
    // Round-robin dispatch order: all signers at nonce_offset 0, then all
    // signers at nonce_offset 1, and so on, truncated to `count`.
    (0..txs_per_signer)
        .flat_map(|nonce_offset| signers.iter().map(move |signer| (nonce_offset, signer)))
        .take(count)
        .map(|(nonce_offset, signer)| {
            let nonce = nonce_base
                .checked_add(nonce_offset as u64)
                .ok_or_else(|| anyhow::anyhow!("nonce_base + {nonce_offset} overflows u64"))?;
            let tx = TxLegacy {
                chain_id: Some(chain_id),
                nonce,
                gas_price: 1_000_000_000,
                gas_limit: 21_000,
                to: TxKind::Call(to),
                value,
                input: Bytes::new(),
            };
            signer.sign_raw(tx).map(|signed| signed.raw)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ANVIL_MNEMONIC as ANVIL_PHRASE;

    #[test]
    fn presign_round_robins_across_signers() {
        let signers = SignerSet::derive(ANVIL_PHRASE, 3).unwrap();
        let to = Address::from([0x11u8; 20]);
        let bytes = presign_transfers(&signers, 1, to, U256::from(1u64), 7, 0).unwrap();
        assert_eq!(bytes.len(), 7);
    }

    #[test]
    fn signer_set_derive_rejects_zero_count() {
        assert!(SignerSet::derive(ANVIL_PHRASE, 0).is_err());
    }
}
