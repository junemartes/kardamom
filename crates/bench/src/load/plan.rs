//! Pre-generation of signed transactions for the load harness.
//!
//! Unlike [`crate::signers::presign_transfers`] (which returns a flat,
//! round-robined byte vec), this builds **per-sender** queues of
//! [`PlannedTx`] — each carrying the locally-computed transaction hash and
//! its nonce — so the engine can (a) submit each sender's txs in strict
//! nonce order and (b) track every tx by hash to a receipt independent of
//! the submit response.

use alloy_consensus::{SignableTransaction, TxEnvelope, TxLegacy};
use alloy_eips::eip2718::Encodable2718;
use alloy_network::TxSignerSync;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};

use crate::signers::DerivedSigner;

/// One pre-signed transaction plus the metadata the engine/tracker need.
#[derive(Debug, Clone)]
pub struct PlannedTx {
    /// EIP-2718-encoded raw transaction bytes (what `eth_sendRawTransaction`
    /// takes).
    pub raw: Bytes,
    /// The transaction hash, computed locally at sign time so the tracker can
    /// key on it without trusting the submit response (a submit that errors
    /// may still have landed).
    pub hash: B256,
    /// Index into the signer set (0-based, after any offset slice).
    pub sender: usize,
    /// This tx's nonce.
    pub nonce: u64,
}

/// Pre-sign `per_sender` legacy value transfers for each signer, returning one
/// queue per signer with strictly increasing nonces from `nonce_start`.
///
/// # Errors
///
/// Errors if `signers` is empty or if signing any transaction fails.
pub fn pregenerate(
    signers: &[DerivedSigner],
    chain_id: u64,
    to: Address,
    value: U256,
    per_sender: usize,
    nonce_start: u64,
    gas_price: u128,
) -> anyhow::Result<Vec<Vec<PlannedTx>>> {
    if signers.is_empty() {
        anyhow::bail!("at least one signer is required");
    }
    let mut out: Vec<Vec<PlannedTx>> = Vec::with_capacity(signers.len());
    for (sender, s) in signers.iter().enumerate() {
        let mut queue = Vec::with_capacity(per_sender);
        for i in 0..per_sender {
            let nonce = nonce_start + i as u64;
            let mut tx = TxLegacy {
                chain_id: Some(chain_id),
                nonce,
                gas_price,
                gas_limit: 21_000,
                to: TxKind::Call(to),
                value,
                input: Bytes::new(),
            };
            let sig = s
                .signer
                .sign_transaction_sync(&mut tx)
                .map_err(|e| anyhow::anyhow!("signing tx (sender {sender} nonce {nonce}): {e}"))?;
            let signed = tx.into_signed(sig);
            let hash = *signed.hash();
            let envelope: TxEnvelope = signed.into();
            let mut bytes = Vec::with_capacity(110);
            envelope.encode_2718(&mut bytes);
            queue.push(PlannedTx {
                raw: Bytes::from(bytes),
                hash,
                sender,
                nonce,
            });
        }
        out.push(queue);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mnemonic;

    const ANVIL_PHRASE: &str = "test test test test test test test test test test test junk";

    #[test]
    fn pregenerate_per_sender_monotonic_nonces() {
        let signers = mnemonic::derive_signers(ANVIL_PHRASE, 3).unwrap();
        let to = Address::from([0x11u8; 20]);
        let q = pregenerate(&signers, 412_346, to, U256::from(1u64), 4, 7, 1_000_000_000).unwrap();
        assert_eq!(q.len(), 3, "one queue per signer");
        for (sender, queue) in q.iter().enumerate() {
            assert_eq!(queue.len(), 4);
            for (i, tx) in queue.iter().enumerate() {
                assert_eq!(
                    tx.nonce,
                    7 + i as u64,
                    "nonces start at nonce_start and increase"
                );
                assert_eq!(tx.sender, sender);
            }
        }
    }

    #[test]
    fn pregenerate_hashes_are_unique_and_nonzero() {
        let signers = mnemonic::derive_signers(ANVIL_PHRASE, 2).unwrap();
        let to = Address::from([0x22u8; 20]);
        let q = pregenerate(&signers, 1, to, U256::from(1u64), 3, 0, 1_000_000_000).unwrap();
        let mut seen = std::collections::HashSet::new();
        for queue in &q {
            for tx in queue {
                assert_ne!(tx.hash, B256::ZERO);
                assert!(seen.insert(tx.hash), "tx hashes must be unique");
            }
        }
    }

    #[test]
    fn pregenerate_empty_signers_errors() {
        let to = Address::from([0u8; 20]);
        assert!(pregenerate(&[], 1, to, U256::from(1u64), 1, 0, 1).is_err());
    }
}
