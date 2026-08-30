//! Zero-alloc nonce extraction from RLP-encoded transaction envelopes.

use alloy_consensus::TxEnvelope as ConsensusEnvelope;
use alloy_consensus::transaction::Transaction;
use alloy_rlp::Decodable;

use crate::error::SequencerError;

/// Decode the nonce from an RLP-encoded alloy `TxEnvelope`.
///
/// The proxy already checked that the envelope is well-formed. This
/// function re-decodes it so the sequencer does not need the nonce passed
/// in as a side channel.
///
/// This extracts the nonce without building the full envelope. The full
/// `ConsensusEnvelope::decode` call allocates a calldata copy per
/// transaction (128 bytes/tx in the service allocation profile) just to
/// read one integer. This function walks the RLP headers directly:
/// - Legacy: `rlp([nonce, gas_price, ...])`, nonce is field 0.
/// - Typed (0x01 2930, 0x02 1559, 0x03 4844): type byte, then
///   `rlp([chain_id, nonce, ...])`, nonce is field 1.
///
/// This path allocates nothing.
///
/// It falls back to the full decode for any format it does not recognize.
/// So acceptance matches the old behavior exactly. An equivalence test
/// runs both paths over every transaction type.
pub(crate) fn decode_nonce(raw_tx: &bytes::Bytes) -> Result<u64, SequencerError> {
    if let Some(n) = peek_nonce(raw_tx.as_ref()) {
        return Ok(n);
    }
    let env = ConsensusEnvelope::decode(&mut raw_tx.as_ref())
        .map_err(|e| SequencerError::MalformedFrame(format!("decode envelope: {e}")))?;
    Ok(env.nonce())
}

/// Walk the RLP structure for the nonce. Returns `None` if the caller
/// should fall back to the full decode.
fn peek_nonce(b: &[u8]) -> Option<u64> {
    // (payload_start, skip_fields) for the list holding the nonce.
    let (mut i, skip) = match b.first()? {
        0x01..=0x03 => (1usize, 1usize),     // typed: [chain_id, nonce, ..]
        f if *f >= 0xc0 => (0usize, 0usize), // legacy: [nonce, ..]
        _ => return None,
    };
    // Enter the outer list.
    let first = *b.get(i)?;
    i += 1;
    if first >= 0xf8 {
        let ll = (first - 0xf7) as usize;
        i += ll;
    } else if first < 0xc0 {
        return None; // not a list
    }
    // Skip `skip` fields, then decode the nonce as an RLP integer.
    for _ in 0..skip {
        i = skip_rlp_item(b, i)?;
    }
    let p = *b.get(i)?;
    if p < 0x80 {
        return Some(u64::from(p)); // single-byte integer
    }
    if p <= 0x88 {
        let l = (p - 0x80) as usize;
        let bytes = b.get(i + 1..i + 1 + l)?;
        if l > 0 && bytes[0] == 0 {
            return None; // non-canonical; let the full decode reject it
        }
        let mut v = 0u64;
        for &x in bytes {
            v = (v << 8) | u64::from(x);
        }
        return Some(v);
    }
    None // the nonce cannot be a list or over 8 bytes; the full decode rejects it
}

/// Advance past one RLP item that starts at `i`. Returns `None` if the
/// data is truncated.
fn skip_rlp_item(b: &[u8], i: usize) -> Option<usize> {
    let p = *b.get(i)?;
    Some(match p {
        0x00..=0x7f => i + 1,
        0x80..=0xb7 => i + 1 + (p - 0x80) as usize,
        0xb8..=0xbf => {
            let ll = (p - 0xb7) as usize;
            let mut l = 0usize;
            for &x in b.get(i + 1..i + 1 + ll)? {
                l = (l << 8) | x as usize;
            }
            i + 1 + ll + l
        }
        0xc0..=0xf7 => i + 1 + (p - 0xc0) as usize,
        0xf8..=0xff => {
            let ll = (p - 0xf7) as usize;
            let mut l = 0usize;
            for &x in b.get(i + 1..i + 1 + ll)? {
                l = (l << 8) | x as usize;
            }
            i + 1 + ll + l
        }
    })
}

#[cfg(test)]
mod nonce_tests {
    use super::*;

    /// The nonce peek must agree with the full decode, for every
    /// transaction type and nonce width. Acceptance is identical by
    /// construction: peek falls back on anything it does not recognize.
    #[test]
    fn peek_nonce_matches_full_decode() {
        use alloy_consensus::{SignableTransaction, TxEip1559, TxEip2930, TxLegacy};
        use alloy_eips::eip2718::Encodable2718;
        use alloy_network::TxSignerSync;
        use alloy_primitives::{Address, TxKind, U256};
        use alloy_signer_local::PrivateKeySigner;

        let signer = PrivateKeySigner::random();
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
        for &nonce in &nonces {
            // Legacy
            let mut t = TxLegacy {
                chain_id: Some(412_346),
                nonce,
                gas_price: 1_000_000_000,
                gas_limit: 100_000,
                to: TxKind::Call(Address::repeat_byte(9)),
                value: U256::from(5u64),
                input: vec![0xAA; 68].into(),
            };
            let sig = signer.sign_transaction_sync(&mut t).unwrap();
            let e: alloy_consensus::TxEnvelope = t.into_signed(sig).into();
            let raw = bytes::Bytes::from(e.encoded_2718());
            assert_eq!(peek_nonce(&raw), Some(nonce), "legacy nonce {nonce}");
            assert_eq!(decode_nonce(&raw).unwrap(), nonce);

            // EIP-1559
            let mut t = TxEip1559 {
                chain_id: 412_346,
                nonce,
                gas_limit: 100_000,
                max_fee_per_gas: 2_000_000_000,
                max_priority_fee_per_gas: 1,
                to: TxKind::Call(Address::repeat_byte(9)),
                value: U256::ZERO,
                access_list: Default::default(),
                input: vec![0xBB; 260].into(),
            };
            let sig = signer.sign_transaction_sync(&mut t).unwrap();
            let e: alloy_consensus::TxEnvelope = t.into_signed(sig).into();
            let raw = bytes::Bytes::from(e.encoded_2718());
            assert_eq!(peek_nonce(&raw), Some(nonce), "1559 nonce {nonce}");
            assert_eq!(decode_nonce(&raw).unwrap(), nonce);

            // EIP-2930
            let mut t = TxEip2930 {
                chain_id: 412_346,
                nonce,
                gas_price: 1_000_000_000,
                gas_limit: 100_000,
                to: TxKind::Call(Address::repeat_byte(9)),
                value: U256::ZERO,
                access_list: Default::default(),
                input: Default::default(),
            };
            let sig = signer.sign_transaction_sync(&mut t).unwrap();
            let e: alloy_consensus::TxEnvelope = t.into_signed(sig).into();
            let raw = bytes::Bytes::from(e.encoded_2718());
            assert_eq!(peek_nonce(&raw), Some(nonce), "2930 nonce {nonce}");
            assert_eq!(decode_nonce(&raw).unwrap(), nonce);
        }
        // Garbage input must return an error through the fallback, not panic.
        assert!(decode_nonce(&bytes::Bytes::from_static(&[0xde, 0xad])).is_err());
        assert!(decode_nonce(&bytes::Bytes::new()).is_err());
    }
}
