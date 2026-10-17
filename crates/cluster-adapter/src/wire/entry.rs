//! The transaction entries that carry their bytes, and the egress frames
//! that go with them.
//!
//! - The full entry: an ingress kind-0 frame of record type [`RT_TX_OFFER`]
//!   `[kind=0][guard header][canonical_id:32][record_type=5][lane:u8][envelope]`.
//! - The canonical entry: an egress kind-1 frame whose payload is
//!   `[canonical_id:32][record_type=6][lane:u8][envelope]`. The sealer builds
//!   it from the ordered full entry: it keeps the id, the lane and the
//!   envelope, and drops the guard header.
//! - The relayed reference ([`EGRESS_KIND_RELAYED_REF`]): the index, the id
//!   and the record type of a canonical entry, with no bytes.
//! - The feed gap ([`EGRESS_KIND_FEED_GAP`]).
//!
//! The envelope is the archived `TxEnvelope` in its frozen rkyv layout. The
//! sealer does not parse it.

use alloy_primitives::B256;
use kardamom_types::{BPosition, TxEnvelope, TxRef};

use super::egress::decode_rkyv_body;
use super::{
    CANONICAL_ID_LEN, KIND_REPLAY_REQUEST, WireError, encode_kind_2u64_id, rd_slice, rd_u8, rd_u64,
};
#[cfg(any(test, feature = "testing"))]
use super::{
    EGRESS_KIND_FEED_GAP, EGRESS_KIND_RELAYED_REF, GuardHeader, KIND_INGRESS_RECORD,
    RELAYED_REF_LEN, RT_TX, RT_TX_OFFER,
};

/// A full entry: one transaction and its archived envelope, offered on
/// ingress as record type [`RT_TX_OFFER`].
#[cfg(any(test, feature = "testing"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxOffer<'a> {
    /// The fields that the sealer checks and orders by, and never relays.
    pub guard: GuardHeader,
    /// The transaction hash.
    pub canonical_id: B256,
    /// The lane of the sender.
    pub lane: u8,
    /// The archived `TxEnvelope`.
    pub envelope: &'a [u8],
}

#[cfg(any(test, feature = "testing"))]
impl TxOffer<'_> {
    /// Encode this entry as an ingress kind-0 frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(
            super::INGRESS_CANONICAL_ID_OFFSET + CANONICAL_ID_LEN + 2 + self.envelope.len(),
        );
        b.push(KIND_INGRESS_RECORD);
        self.guard.write(&mut b);
        b.extend_from_slice(self.canonical_id.as_slice());
        b.push(RT_TX_OFFER);
        b.push(self.lane);
        b.extend_from_slice(self.envelope);
        b
    }
}

/// A canonical entry: an ordered transaction with its envelope, relayed as
/// record type [`RT_TX`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalTx {
    /// The transaction hash.
    pub canonical_id: B256,
    /// The lane of the sender.
    pub lane: u8,
    /// The transaction. Its `tx_hash` equals `canonical_id`.
    pub envelope: TxEnvelope,
}

impl CanonicalTx {
    /// Parse the fields after the record type: `[lane:u8][envelope]`. The
    /// envelope is copied to an aligned buffer before rkyv reads it, because
    /// it sits at an odd offset in the frame. An id that is not the hash in
    /// the envelope is refused: the frame was mis-encoded.
    pub(super) fn parse(canonical_id: B256, fields: &[u8]) -> Result<Self, WireError> {
        let lane = rd_u8(fields, 0)?;
        let envelope: TxEnvelope =
            decode_rkyv_body(&fields[1..]).map_err(|e| WireError::BadTx(e.to_string()))?;
        if envelope.tx_hash != canonical_id {
            return Err(WireError::BadTx(format!(
                "canonical id {canonical_id} is not the envelope hash {}",
                envelope.tx_hash
            )));
        }
        Ok(Self {
            canonical_id,
            lane,
            envelope,
        })
    }

    /// The reference that a receipt of this entry keeps. No `tx_data`
    /// archive holds the entry, so the archive fields are zero. The lane is
    /// the shard.
    #[must_use]
    pub fn tx_ref(&self) -> TxRef {
        TxRef::new(self.canonical_id, self.lane, BPosition::from_index(0), 0)
    }

    /// The relayed payload of this entry, as the sealer builds it:
    /// `[canonical_id:32][record_type=6][lane:u8][envelope]`.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::BadTx`] if the envelope does not serialize.
    #[cfg(any(test, feature = "testing"))]
    pub fn payload(&self) -> Result<Vec<u8>, WireError> {
        let envelope = rkyv::to_bytes::<rkyv::rancor::Error>(&self.envelope)
            .map_err(|e| WireError::BadTx(e.to_string()))?;
        Ok([
            self.canonical_id.as_slice(),
            &[RT_TX, self.lane],
            envelope.as_slice(),
        ]
        .concat())
    }
}

/// A relayed reference: the index, the id and the record type of a
/// canonical entry, without its bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayedRef {
    /// The canonical index of the entry.
    pub index: u64,
    /// The transaction hash.
    pub canonical_id: B256,
    /// The record type of the entry.
    pub record_type: u8,
}

impl RelayedRef {
    /// Parse `[kind=15][index:u64][canonical_id:32][record_type:u8]`. A
    /// longer frame reads the same: a later field goes at the tail.
    pub(super) fn parse(buf: &[u8]) -> Result<Self, WireError> {
        Ok(Self {
            index: rd_u64(buf, 1)?,
            canonical_id: B256::from_slice(rd_slice(buf, 9, CANONICAL_ID_LEN)?),
            record_type: rd_u8(buf, 9 + CANONICAL_ID_LEN)?,
        })
    }

    /// Frame this reference exactly as the Java service does.
    #[cfg(any(test, feature = "testing"))]
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(RELAYED_REF_LEN);
        b.push(EGRESS_KIND_RELAYED_REF);
        b.extend_from_slice(&self.index.to_le_bytes());
        b.extend_from_slice(self.canonical_id.as_slice());
        b.push(self.record_type);
        b
    }
}

/// Frame an answer of the member feed exactly as the Java service does:
/// `[kind:u8][a:u64][b:u64][request_id:u64]`. The kinds are 3, 4, 11 and 16.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_feed_answer(kind: u8, a: u64, b: u64, request_id: u64) -> Vec<u8> {
    encode_kind_2u64_id(kind, a, b, request_id)
}

/// Frame a feed request on the control stream:
/// `[kind=1][from_index:u64][from_block:u64][request_id:u64]`, the layout of
/// ingress kind 1 with the request id as a tail. The member answers the
/// newest request, and each frame of its answer carries `request_id`.
#[must_use]
pub fn encode_feed_request(from_index: u64, from_block: u64, request_id: u64) -> Vec<u8> {
    encode_kind_2u64_id(KIND_REPLAY_REQUEST, from_index, from_block, request_id)
}

/// Frame a feed gap exactly as the Java service does.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_feed_gap(next_index: u64, next_block: u64, request_id: u64) -> Vec<u8> {
    encode_feed_answer(EGRESS_KIND_FEED_GAP, next_index, next_block, request_id)
}
