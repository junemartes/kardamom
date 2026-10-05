//! Zero-alloc field extraction from RLP-encoded transaction envelopes:
//! the nonce, the fee fields, and the value.

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::U256;
use kardamom_types::TxFees;

use crate::error::SequencerError;
use crate::fees::FeeFields;

/// The fields the sequencer reads from one transaction: the nonce for the
/// state machine, the fee fields and the value for the admission gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TxFields {
    pub(crate) nonce: u64,
    pub(crate) fees: FeeFields,
}

/// Decode the sequencer's fields from an RLP-encoded alloy `TxEnvelope`.
///
/// The proxy already checked that the envelope is well-formed. This
/// function re-decodes it so the sequencer does not need the fields
/// passed in as a side channel.
///
/// This extracts the fields without building the full envelope. The full
/// `ConsensusEnvelope` decode allocates a calldata copy per transaction
/// (128 bytes/tx in the service allocation profile) just to read a few
/// integers. This function walks the RLP headers directly:
/// - Legacy: `rlp([nonce, gas_price, gas_limit, to, value, ...])`.
/// - 0x01 (2930): type byte, then
///   `rlp([chain_id, nonce, gas_price, gas_limit, to, value, ...])`.
/// - 0x02 (1559), 0x03 (4844): type byte, then `rlp([chain_id, nonce,
///   max_priority_fee_per_gas, max_fee_per_gas, gas_limit, to, value, ...])`.
///
/// This path allocates nothing.
///
/// It falls back to the full decode for any format it does not recognize.
/// So acceptance matches the full decode exactly. An equivalence test
/// runs both paths over every transaction type.
pub(crate) fn decode_fields(raw_tx: &bytes::Bytes) -> Result<TxFields, SequencerError> {
    if let Some(fields) = peek_fields(raw_tx.as_ref()) {
        return Ok(fields);
    }
    let mut slice: &[u8] = raw_tx.as_ref();
    let env = ConsensusEnvelope::decode_2718(&mut slice)
        .map_err(|e| SequencerError::MalformedFrame(format!("decode envelope: {e}")))?;
    let legacy = !env.is_dynamic_fee();
    Ok(TxFields {
        nonce: env.nonce(),
        fees: FeeFields {
            fees: TxFees {
                gas_limit: env.gas_limit(),
                max_fee_per_gas: env.max_fee_per_gas(),
                max_priority_fee_per_gas: env.priority_fee_or_price(),
                legacy,
            },
            value: env.value(),
        },
    })
}

/// How a transaction type lays out the fields before the value.
#[derive(Clone, Copy)]
enum Layout {
    /// `[nonce, gas_price, gas_limit, to, value]`.
    Legacy,
    /// `[chain_id, nonce, gas_price, gas_limit, to, value]`.
    AccessList,
    /// `[chain_id, nonce, max_priority, max_fee, gas_limit, to, value]`.
    DynamicFee,
}

/// Walk the RLP structure for the fields. Returns `None` if the caller
/// should fall back to the full decode.
fn peek_fields(b: &[u8]) -> Option<TxFields> {
    let (start, layout) = match b.first()? {
        0x01 => (1usize, Layout::AccessList),
        0x02 | 0x03 => (1usize, Layout::DynamicFee),
        f if *f >= 0xc0 => (0usize, Layout::Legacy),
        _ => return None,
    };
    let mut cur = Cursor { b, i: start };
    cur.enter_list()?;
    if !matches!(layout, Layout::Legacy) {
        cur.skip_item()?;
    }
    let nonce = cur.uint(8)?.to::<u64>();
    let (max_priority_fee_per_gas, max_fee_per_gas) = match layout {
        Layout::Legacy | Layout::AccessList => {
            let price = cur.uint(16)?.to::<u128>();
            (price, price)
        }
        Layout::DynamicFee => {
            let priority = cur.uint(16)?.to::<u128>();
            (priority, cur.uint(16)?.to::<u128>())
        }
    };
    let gas_limit = cur.uint(8)?.to::<u64>();
    cur.skip_item()?;
    let value = cur.uint(32)?;
    Some(TxFields {
        nonce,
        fees: FeeFields {
            fees: TxFees {
                gas_limit,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                legacy: !matches!(layout, Layout::DynamicFee),
            },
            value,
        },
    })
}

/// A position in an RLP byte string. Every method reads one item at the
/// position and moves past it, or returns `None` when the bytes are not
/// the item the layout expects.
struct Cursor<'a> {
    b: &'a [u8],
    i: usize,
}

impl Cursor<'_> {
    /// Move inside the list that starts at the position.
    fn enter_list(&mut self) -> Option<()> {
        let first = *self.b.get(self.i)?;
        self.i += 1;
        if first >= 0xf8 {
            self.i += (first - 0xf7) as usize;
        } else if first < 0xc0 {
            return None; // not a list
        }
        Some(())
    }

    /// Decode an RLP integer of at most `max_len` bytes as a `U256`.
    /// Non-canonical encodings (a leading zero, or a single byte that
    /// should have been inline) go to the full decode, which rejects
    /// them.
    fn uint(&mut self, max_len: usize) -> Option<U256> {
        let p = *self.b.get(self.i)?;
        if p < 0x80 {
            self.i += 1;
            return Some(U256::from(p));
        }
        let l = usize::from(p.checked_sub(0x80)?);
        if l > max_len.min(55) {
            return None;
        }
        let bytes = self.b.get(self.i + 1..self.i + 1 + l)?;
        if bytes.first() == Some(&0) {
            return None;
        }
        self.i += 1 + l;
        Some(U256::from_be_slice(bytes))
    }

    /// Advance past one RLP item. Returns `None` if the data is truncated,
    /// or if the item's declared length would overflow `usize` (an
    /// attacker-controlled length from the wire bytes cannot be trusted
    /// to stay in range).
    ///
    /// `i + 1`, `i + 1 + (p - 0x80)`, and `i + 1 + (p - 0xc0)` stay plain
    /// arithmetic: `i <= b.len()` (the `get(i)` above proved it) and the
    /// prefix-derived offset is bounded at 55, both far under
    /// `usize::MAX`. Only `i + 1 + ll + l` needs a checked add: `l` comes
    /// from [`be_len`] on wire bytes and is not bounded at all.
    fn skip_item(&mut self) -> Option<()> {
        let (b, i) = (self.b, self.i);
        let p = *b.get(i)?;
        self.i = match p {
            0x00..=0x7f => i + 1,
            0x80..=0xb7 => i + 1 + (p - 0x80) as usize,
            0xb8..=0xbf => {
                let ll = (p - 0xb7) as usize;
                let l = be_len(b.get(i + 1..i + 1 + ll)?);
                (i + 1 + ll).checked_add(l)?
            }
            0xc0..=0xf7 => i + 1 + (p - 0xc0) as usize,
            0xf8..=0xff => {
                let ll = (p - 0xf7) as usize;
                let l = be_len(b.get(i + 1..i + 1 + ll)?);
                (i + 1 + ll).checked_add(l)?
            }
        };
        Some(())
    }
}

/// Read a big-endian length from an RLP length-of-length field.
fn be_len(bytes: &[u8]) -> usize {
    bytes.iter().fold(0usize, |l, &x| (l << 8) | x as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{SignableTransaction, TxEip1559, TxEip2930, TxLegacy};
    use alloy_eips::eip2718::Encodable2718;
    use alloy_network::TxSignerSync;
    use alloy_primitives::{Address, TxKind};
    use alloy_signer_local::PrivateKeySigner;

    /// The three wire encodings of one (nonce, fee, value) triple: a
    /// legacy, a 2930, and a 1559 transaction.
    fn encodings(nonce: u64, price: u128, value: U256) -> [(bytes::Bytes, bool); 3] {
        let signer = PrivateKeySigner::random();
        let access_list = alloy_eips::eip2930::AccessList::default();
        let mut legacy = TxLegacy {
            chain_id: Some(412_346),
            nonce,
            gas_price: price,
            gas_limit: 100_000,
            to: TxKind::Call(Address::repeat_byte(9)),
            value,
            input: vec![0xAA; 68].into(),
        };
        let mut eip2930 = TxEip2930 {
            chain_id: 412_346,
            nonce,
            gas_price: price,
            gas_limit: 100_000,
            to: TxKind::Call(Address::repeat_byte(9)),
            value,
            access_list: access_list.clone(),
            input: alloy_primitives::Bytes::default(),
        };
        let mut eip1559 = TxEip1559 {
            chain_id: 412_346,
            nonce,
            gas_limit: 100_000,
            max_fee_per_gas: price,
            max_priority_fee_per_gas: price / 2,
            to: TxKind::Call(Address::repeat_byte(9)),
            value,
            access_list,
            input: vec![0xBB; 260].into(),
        };
        let sig = signer.sign_transaction_sync(&mut legacy).unwrap();
        let legacy: ConsensusEnvelope = legacy.into_signed(sig).into();
        let sig = signer.sign_transaction_sync(&mut eip2930).unwrap();
        let eip2930: ConsensusEnvelope = eip2930.into_signed(sig).into();
        let sig = signer.sign_transaction_sync(&mut eip1559).unwrap();
        let eip1559: ConsensusEnvelope = eip1559.into_signed(sig).into();
        [
            (bytes::Bytes::from(legacy.encoded_2718()), true),
            (bytes::Bytes::from(eip2930.encoded_2718()), true),
            (bytes::Bytes::from(eip1559.encoded_2718()), false),
        ]
    }

    /// The field peek must agree with the full decode, for every
    /// transaction type and integer width. Acceptance is identical by
    /// construction: peek falls back on anything it does not recognize.
    #[test]
    fn peek_fields_matches_full_decode() {
        let nonces = [
            0u64,
            1,
            127,
            128,
            255,
            256,
            65_535,
            1 << 20,
            u64::from(u32::MAX),
            u64::MAX,
        ];
        let prices = [0u128, 1, 127, 128, 1_000_000_000, u128::from(u64::MAX) + 1];
        let values = [U256::ZERO, U256::from(1u8), U256::from(1u128 << 100)];
        let cases = nonces
            .iter()
            .zip(prices.iter().cycle())
            .zip(values.iter().cycle())
            .flat_map(|((&nonce, &price), &value)| {
                encodings(nonce, price, value)
                    .into_iter()
                    .map(move |(raw, legacy)| (nonce, price, value, raw, legacy))
            });
        for (nonce, price, value, raw, legacy) in cases {
            let peeked = peek_fields(&raw).expect("peek handles every supported type");
            assert_eq!(decode_fields(&raw).unwrap(), peeked);
            assert_eq!(peeked.nonce, nonce, "nonce {nonce} legacy={legacy}");
            assert_eq!(peeked.fees.fees.max_fee_per_gas, price);
            assert_eq!(peeked.fees.fees.gas_limit, 100_000);
            assert_eq!(peeked.fees.fees.legacy, legacy);
            assert_eq!(peeked.fees.value, value);
            let expected_tip = if legacy { price } else { price / 2 };
            assert_eq!(peeked.fees.fees.max_priority_fee_per_gas, expected_tip);
        }
        // Garbage input must return an error through the fallback, not panic.
        assert!(decode_fields(&bytes::Bytes::from_static(&[0xde, 0xad])).is_err());
        assert!(decode_fields(&bytes::Bytes::new()).is_err());
    }
}
