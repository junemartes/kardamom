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

use std::num::NonZeroU64;

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
        #[allow(
            clippy::cast_possible_truncation,
            reason = "i % 255 + 1 is at most 255"
        )]
        let input = (0..bytes).map(|i| (i % 255 + 1) as u8).collect();
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
/// takes `w` places of the cycle. The mix keeps each shape once, with the
/// end of its places, so a large weight costs no memory.
#[derive(Debug, Clone)]
pub struct TransferMix {
    shapes: Vec<TransferShape>,
    /// The end of the places of each shape: the sum of the weights up to
    /// and including it. It increases strictly.
    ends: Vec<u64>,
    /// The length of the cycle: the last value of `ends`.
    total: NonZeroU64,
}

impl TransferMix {
    /// A mix of one shape.
    #[must_use]
    pub fn single(shape: TransferShape) -> Self {
        Self {
            shapes: vec![shape],
            ends: vec![1],
            total: NonZeroU64::MIN,
        }
    }

    /// A mix of value transfers to `to`, with the calldata sizes and the
    /// weights of `mix`.
    ///
    /// # Errors
    ///
    /// Returns an error if the gas of a size overflows `u64`, or if the
    /// sum of the weights overflows `u64`.
    pub fn calldata(to: Address, value: U256, mix: &CalldataMix) -> anyhow::Result<Self> {
        let shapes = mix
            .parts()
            .iter()
            .map(|part| TransferShape::with_calldata(to, value, part.bytes))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let ends = mix
            .parts()
            .iter()
            .try_fold(Vec::new(), |mut ends: Vec<u64>, part| {
                let end = ends
                    .last()
                    .copied()
                    .unwrap_or(0)
                    .checked_add(u64::from(part.weight.get()))?;
                ends.push(end);
                Some(ends)
            })
            .ok_or_else(|| anyhow::anyhow!("the sum of the calldata weights overflows u64"))?;
        // A `CalldataMix` has one part or more, and every weight is
        // non-zero, so the last end is non-zero.
        let total = ends
            .last()
            .copied()
            .and_then(NonZeroU64::new)
            .ok_or_else(|| anyhow::anyhow!("the calldata mix is empty"))?;
        Ok(Self {
            shapes,
            ends,
            total,
        })
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
        // The place wraps on purpose: the cycle repeats.
        let place = (sender as u64).wrapping_add(i as u64) % self.total;
        // `place < total`, the last end, so the index is in range.
        &self.shapes[self.ends.partition_point(|&end| end <= place)]
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
    fn a_mix_cycles_its_weighted_sizes() {
        use alloy_consensus::Transaction;
        use alloy_eips::eip2718::Decodable2718;

        let signers = SignerSet::derive(ANVIL_PHRASE, 2).unwrap();
        let params = TxPlanParams {
            chain_id: 1,
            nonce_start: 0,
            gas_price: 1,
        };
        let mix =
            TransferMix::calldata(Address::ZERO, U256::ZERO, &"0:2,64:1".parse().unwrap()).unwrap();
        let q = mix.pregenerate(&signers, 3, params, &[0, 0]).unwrap();
        let sizes: Vec<Vec<usize>> = q
            .iter()
            .map(|queue| {
                queue
                    .iter()
                    .map(|tx| {
                        alloy_consensus::TxEnvelope::decode_2718(&mut tx.raw.as_ref())
                            .unwrap()
                            .input()
                            .len()
                    })
                    .collect()
            })
            .collect();
        assert_eq!(sizes, vec![vec![0, 0, 64], vec![0, 64, 0]]);
    }

    #[test]
    fn a_huge_weight_keeps_one_shape() {
        let mix = TransferMix::calldata(
            Address::ZERO,
            U256::ZERO,
            &"0:4000000000,8:1".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(mix.shapes.len(), 2);
        assert_eq!(mix.shape(0, 0).input.len(), 0);
        assert_eq!(mix.shape(3_999_999_999, 0).input.len(), 0);
        assert_eq!(mix.shape(4_000_000_000, 0).input.len(), 8);
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
