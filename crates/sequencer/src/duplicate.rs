//! Receipt-cache duplicate notification (Past-nonce signal).
//!
//! The calls for the sequencer to publish a "duplicate notification"
//! onto the receipt-cache channel when an incoming tx has `nonce < expected`.
//! No existing wire type in `kardamom-types` covers this; we ship a small
//! local rkyv-archived type here so the proxy / receipt-cache consumers can
//! discriminate it from a `CachedReceipt`. May be promoted into
//! `kardamom-types` in a future cross-cutting change.

use alloy_primitives::Address;
use rkyv::{Archive, Deserialize, Serialize};

use types::wire;

/// Emitted by the sequencer to the receipt-cache channel when an inbound
/// `TxEnvelope` has a nonce strictly less than the sender's expected next
/// nonce (i.e. the tx is a duplicate of one already canonicalised on B).
/// Consumers (proxy) use it to release any client waiting on the duplicate
/// `correlation_id` with the cached receipt for the original `(sender, nonce)`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct DuplicateNotification {
    pub correlation_id: u64,
    #[rkyv(with = wire::AddressBytes)]
    pub sender: Address,
    pub nonce: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkyv::rancor;

    #[test]
    fn rkyv_round_trip() {
        let d = DuplicateNotification {
            correlation_id: 0xDEAD_BEEF,
            sender: Address::repeat_byte(0xAB),
            nonce: 7,
        };
        let bytes = rkyv::to_bytes::<rancor::Error>(&d).unwrap();
        let back: DuplicateNotification =
            rkyv::from_bytes::<DuplicateNotification, rancor::Error>(&bytes).unwrap();
        assert_eq!(d, back);
    }
}
