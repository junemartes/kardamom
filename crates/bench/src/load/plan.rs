//! This module pre-generates signed transactions for the load harness.
//!
//! Unlike [`crate::signers::presign_transfers`], which returns one flat,
//! rotated byte vector, this module builds a per-sender queue of
//! [`PlannedTx`] values. Each value carries the locally computed
//! transaction hash and its nonce. This lets the engine:
//! - pop each sender's transactions in per-sender FIFO nonce order.
//!   Submits run as concurrent tasks, so wire order, and so arrival
//!   order at ingress, is not strict.
//! - track every transaction by hash to a receipt, independent of the
//!   submit response.

use alloy_consensus::TxLegacy;
use alloy_primitives::{Address, B256, Bytes, TxKind, U256};
use kardamom_types::TxEnvelope;

use crate::signers::SignerSet;

/// The transaction fields every pre-generation call shares: the chain
/// ID, the first nonce to sign at, and the gas price. `to` and `value`
/// stay separate, since `deployment_txs` and `pregenerate_defi` compute
/// their own `to` per call and never send value.
#[derive(Debug, Clone, Copy)]
pub struct TxPlanParams {
    pub chain_id: u64,
    pub nonce_start: u64,
    pub gas_price: u128,
}

/// One pre-signed transaction, with the metadata the engine and
/// tracker need.
#[derive(Debug, Clone)]
pub struct PlannedTx {
    /// The EIP-2718-encoded raw transaction bytes. This is the value
    /// `eth_sendRawTransaction` takes.
    pub raw: Bytes,
    /// The transaction hash, computed locally at sign time. This lets
    /// the tracker key on the hash without trusting the submit response,
    /// since a submit that errors can still have landed.
    pub hash: B256,
    /// The index into the signer set, 0-based, after any offset slice.
    pub sender: usize,
    /// This transaction's nonce.
    pub nonce: u64,
}

impl PlannedTx {
    /// Wrap this pre-signed transaction as a [`TxEnvelope`], attaching
    /// the resolved sender address and a caller-chosen correlation ID.
    ///
    /// Callers that do not track a correlation ID pass `0`.
    #[must_use]
    pub fn to_envelope(&self, sender: Address, correlation_id: u64) -> TxEnvelope {
        TxEnvelope {
            correlation_id,
            raw_tx: self.raw.clone().into(),
            sender,
            tx_hash: self.hash,
        }
    }
}

/// Pre-sign `per_sender` legacy value transfers for each signer.
/// Returns one queue per signer, with nonces that strictly increase
/// from `nonce_start`.
///
/// # Errors
///
/// Returns an error if signing a transaction fails.
pub fn pregenerate(
    signers: &SignerSet,
    to: Address,
    value: U256,
    per_sender: usize,
    params: TxPlanParams,
) -> anyhow::Result<Vec<Vec<PlannedTx>>> {
    signers
        .iter()
        .enumerate()
        .map(|(sender, s)| {
            (0..per_sender)
                .map(|i| {
                    let nonce = params
                        .nonce_start
                        .checked_add(i as u64)
                        .ok_or_else(|| anyhow::anyhow!("nonce_start + {i} overflows u64"))?;
                    let tx = TxLegacy {
                        chain_id: Some(params.chain_id),
                        nonce,
                        gas_price: params.gas_price,
                        gas_limit: 21_000,
                        to: TxKind::Call(to),
                        value,
                        input: Bytes::new(),
                    };
                    let signed = s.sign_raw(tx).map_err(|e| {
                        anyhow::anyhow!("signing tx (sender {sender} nonce {nonce}): {e}")
                    })?;
                    Ok(PlannedTx {
                        raw: signed.raw,
                        hash: signed.hash,
                        sender,
                        nonce,
                    })
                })
                .collect::<anyhow::Result<Vec<PlannedTx>>>()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ANVIL_MNEMONIC as ANVIL_PHRASE;

    #[test]
    fn pregenerate_per_sender_monotonic_nonces() {
        let signers = SignerSet::derive(ANVIL_PHRASE, 3).unwrap();
        let to = Address::from([0x11u8; 20]);
        let params = TxPlanParams {
            chain_id: 412_346,
            nonce_start: 7,
            gas_price: 1_000_000_000,
        };
        let q = pregenerate(&signers, to, U256::from(1u64), 4, params).unwrap();
        assert_eq!(q.len(), 3, "one queue per signer");
        q.iter().enumerate().for_each(|(sender, queue)| {
            assert_eq!(queue.len(), 4);
            queue.iter().enumerate().for_each(|(i, tx)| {
                assert_eq!(
                    tx.nonce,
                    7 + i as u64,
                    "nonces start at nonce_start and increase"
                );
                assert_eq!(tx.sender, sender);
            });
        });
    }

    #[test]
    fn pregenerate_hashes_are_unique_and_nonzero() {
        let signers = SignerSet::derive(ANVIL_PHRASE, 2).unwrap();
        let to = Address::from([0x22u8; 20]);
        let params = TxPlanParams {
            chain_id: 1,
            nonce_start: 0,
            gas_price: 1_000_000_000,
        };
        let q = pregenerate(&signers, to, U256::from(1u64), 3, params).unwrap();
        let mut seen = std::collections::HashSet::new();
        q.iter().flatten().for_each(|tx| {
            assert_ne!(tx.hash, B256::ZERO);
            assert!(seen.insert(tx.hash), "tx hashes must be unique");
        });
    }

    #[test]
    fn signer_set_new_rejects_empty_signers() {
        assert!(SignerSet::new(Vec::new()).is_err());
    }
}
