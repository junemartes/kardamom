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

use crate::load::config::CalldataMix;
use crate::signers::{DerivedSigner, SignerSet};

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
            // The load submits through the proxy, which stamps the
            // real deadline. This envelope never reaches the wire.
            max_inclusion_block: u64::MAX,
        }
    }
}

/// The intrinsic gas of a transfer with no calldata.
const TRANSFER_GAS: u64 = 21_000;

/// The gas of one non-zero calldata byte under the calldata floor
/// (EIP-7623): 4 tokens at 10 gas. The floor is above the standard cost
/// of 16 gas, so it sets the intrinsic gas of a calldata-only transfer.
const CALLDATA_FLOOR_GAS_PER_BYTE: u64 = 40;

/// The fields of each transfer a run signs: the sink, the value, the
/// calldata, and a gas limit that covers the calldata.
#[derive(Debug, Clone)]
pub struct TransferShape {
    to: Address,
    value: U256,
    input: Bytes,
    gas_limit: u64,
}

impl TransferShape {
    /// A plain value transfer with no calldata.
    #[must_use]
    pub fn plain(to: Address, value: U256) -> Self {
        Self {
            to,
            value,
            input: Bytes::new(),
            gas_limit: TRANSFER_GAS,
        }
    }

    /// A value transfer with `bytes` of calldata. Every byte is non-zero,
    /// so the gas limit is the calldata floor of non-zero bytes. A sink
    /// with no code ignores the calldata, so the size is the only value
    /// that differs from [`Self::plain`].
    ///
    /// # Errors
    ///
    /// Returns an error if the gas limit overflows `u64`.
    pub fn with_calldata(to: Address, value: U256, bytes: usize) -> anyhow::Result<Self> {
        let gas_limit = u64::try_from(bytes)
            .ok()
            .and_then(|n| n.checked_mul(CALLDATA_FLOOR_GAS_PER_BYTE))
            .and_then(|g| g.checked_add(TRANSFER_GAS))
            .ok_or_else(|| anyhow::anyhow!("the gas of {bytes} calldata bytes overflows u64"))?;
        // The byte value cycles through 1..=255, so no byte is zero.
        let input = (0..bytes)
            .map(|i| u8::try_from(i % 255).map_or(1, |b| b.wrapping_add(1)))
            .collect();
        Ok(Self {
            to,
            value,
            input,
            gas_limit,
        })
    }

    /// Sign this transfer for one sender at one nonce.
    fn sign(&self, slot: Slot<'_>, params: TxPlanParams) -> anyhow::Result<PlannedTx> {
        let tx = TxLegacy {
            chain_id: Some(params.chain_id),
            nonce: slot.nonce,
            gas_price: params.gas_price,
            gas_limit: self.gas_limit,
            to: TxKind::Call(self.to),
            value: self.value,
            input: self.input.clone(),
        };
        let signed = slot
            .signer
            .sign_raw(tx)
            .map_err(|e| sign_error(slot.sender, slot.nonce, &e))?;
        Ok(PlannedTx {
            raw: signed.raw,
            hash: signed.hash,
            sender: slot.sender,
            nonce: slot.nonce,
        })
    }
}

/// The transfers a run signs, in a fixed cycle. A shape with weight `w`
/// takes `w` places of the cycle.
#[derive(Debug, Clone)]
pub struct TransferMix {
    cycle: Vec<TransferShape>,
}

impl TransferMix {
    /// A mix of one shape.
    #[must_use]
    pub fn single(shape: TransferShape) -> Self {
        Self { cycle: vec![shape] }
    }

    /// A mix of value transfers to `to`, with the calldata sizes and the
    /// weights of `mix`.
    ///
    /// # Errors
    ///
    /// Returns an error if the gas of a size overflows `u64`.
    pub fn calldata(to: Address, value: U256, mix: &CalldataMix) -> anyhow::Result<Self> {
        let cycle = mix
            .parts()
            .iter()
            .map(|part| {
                let shape = TransferShape::with_calldata(to, value, part.bytes)?;
                Ok(std::iter::repeat_n(shape, part.weight.get() as usize))
            })
            .collect::<anyhow::Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(Self { cycle })
    }

    /// Pre-sign `per_sender` transfers for each signer. Returns one queue
    /// per signer. The nonces of signer `s` increase strictly from
    /// `starts[s]`; `params.nonce_start` is not read. Transfer `i` of
    /// signer `s` takes the shape at place `s + i` of the cycle, so the
    /// senders do not send the same size at the same time.
    ///
    /// # Errors
    ///
    /// Returns an error if a nonce overflows or signing fails.
    pub fn pregenerate(
        &self,
        signers: &SignerSet,
        per_sender: usize,
        params: TxPlanParams,
        starts: &[u64],
    ) -> anyhow::Result<Vec<Vec<PlannedTx>>> {
        signers
            .iter()
            .zip(starts)
            .enumerate()
            .map(|(sender, (signer, &start))| {
                (0..per_sender)
                    .map(|i| {
                        let nonce = start
                            .checked_add(i as u64)
                            .ok_or_else(|| anyhow::anyhow!("nonce {start} + {i} overflows u64"))?;
                        self.shape(sender, i).sign(
                            Slot {
                                signer,
                                sender,
                                nonce,
                            },
                            params,
                        )
                    })
                    .collect::<anyhow::Result<Vec<PlannedTx>>>()
            })
            .collect()
    }

    /// The shape of transfer `i` of `sender`.
    fn shape(&self, sender: usize, i: usize) -> &TransferShape {
        &self.cycle[sender.wrapping_add(i) % self.cycle.len()]
    }
}

/// One signature to make: the signer, its index in the signer set, and
/// the nonce.
#[derive(Clone, Copy)]
struct Slot<'a> {
    signer: &'a DerivedSigner,
    sender: usize,
    nonce: u64,
}

/// The error for a failed presign at `(sender, nonce)`.
fn sign_error(sender: usize, nonce: u64, e: &anyhow::Error) -> anyhow::Error {
    anyhow::anyhow!("signing tx (sender {sender} nonce {nonce}): {e}")
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
        let q = TransferMix::single(TransferShape::plain(to, U256::from(1u64)))
            .pregenerate(&signers, 4, params, &[7; 3])
            .unwrap();
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
        let q = TransferMix::single(TransferShape::plain(to, U256::from(1u64)))
            .pregenerate(&signers, 3, params, &[0; 2])
            .unwrap();
        let mut seen = std::collections::HashSet::new();
        q.iter().flatten().for_each(|tx| {
            assert_ne!(tx.hash, B256::ZERO);
            assert!(seen.insert(tx.hash), "tx hashes must be unique");
        });
    }

    #[test]
    fn pregenerate_starts_each_sender_at_its_own_nonce() {
        let signers = SignerSet::derive(ANVIL_PHRASE, 2).unwrap();
        let params = TxPlanParams {
            chain_id: 1,
            nonce_start: 0,
            gas_price: 1,
        };
        let q = TransferMix::single(TransferShape::plain(Address::ZERO, U256::ZERO))
            .pregenerate(&signers, 2, params, &[3, 9])
            .unwrap();
        let nonces: Vec<Vec<u64>> = q
            .iter()
            .map(|queue| queue.iter().map(|tx| tx.nonce).collect())
            .collect();
        assert_eq!(nonces, vec![vec![3, 4], vec![9, 10]]);
    }

    #[test]
    fn calldata_shape_carries_non_zero_bytes_and_their_gas() {
        use alloy_consensus::Transaction;
        use alloy_eips::eip2718::Decodable2718;

        let signers = SignerSet::derive(ANVIL_PHRASE, 1).unwrap();
        let params = TxPlanParams {
            chain_id: 412_346,
            nonce_start: 0,
            gas_price: 1_000_000_000,
        };
        let mix = TransferMix::single(
            TransferShape::with_calldata(Address::ZERO, U256::from(1u64), 2_000).unwrap(),
        );
        let q = mix.pregenerate(&signers, 1, params, &[0]).unwrap();
        let tx = alloy_consensus::TxEnvelope::decode_2718(&mut q[0][0].raw.as_ref()).unwrap();
        assert_eq!(tx.input().len(), 2_000);
        assert!(tx.input().iter().all(|b| *b != 0), "every byte is non-zero");
        assert_eq!(tx.gas_limit(), 21_000 + 40 * 2_000);
    }

    #[test]
    fn calldata_gas_overflow_is_an_error() {
        assert!(TransferShape::with_calldata(Address::ZERO, U256::ZERO, usize::MAX).is_err());
    }

    #[test]
    fn signer_set_new_rejects_empty_signers() {
        assert!(SignerSet::new(Vec::new()).is_err());
    }
}
