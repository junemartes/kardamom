//! The transaction entries that carry their bytes, and the egress frames
//! that go with them.
//!
//! - The full entry: an ingress kind-0 frame of record type [`RT_TX_OFFER`]
//!   `[kind=0][guard header][canonical_id:32][record_type=5][lane:u8][envelope]`.
//! - The full reference: an ingress kind-0 frame of record type
//!   [`RT_TXREF_OFFER`]
//!   `[kind=0][guard header][canonical_id:32][record_type=7][lane:u8][term_id:i32][term_offset:i32][session_id:i32][envelope]`.
//!   The sealer relays it as the same canonical entry as a full entry: it
//!   drops the guard header and the position in the image.
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
    CANONICAL_ID_LEN, ENVELOPE_ROOT_LEN, GuardHeader, INGRESS_CANONICAL_ID_OFFSET,
    KIND_INGRESS_RECORD, KIND_REPLAY_REQUEST, POSITION_MASK, POSITION_TAG, RT_TX, RT_TXREF_OFFER,
    TXREF_RECORD_LEN, WireError, encode_kind_2u64_id, rd_i32, rd_slice, rd_u8, rd_u64,
};
#[cfg(any(test, feature = "testing"))]
use super::{EGRESS_KIND_FEED_GAP, EGRESS_KIND_RELAYED_REF, RELAYED_REF_LEN, RT_TX_OFFER};

/// The offset of the record type in a kind-0 frame.
const RECORD_TYPE_OFFSET: usize = INGRESS_CANONICAL_ID_OFFSET + CANONICAL_ID_LEN;
/// The offset of the lane in a kind-0 frame of a transaction reference.
const REF_LANE_OFFSET: usize = RECORD_TYPE_OFFSET + 1;
/// The offset of the term id in a kind-0 frame of a transaction reference.
const REF_TERM_ID_OFFSET: usize = REF_LANE_OFFSET + 1;
/// The offset of the term offset in a kind-0 frame of a transaction reference.
const REF_TERM_OFFSET_OFFSET: usize = REF_TERM_ID_OFFSET + 4;
/// The offset of the session id in a kind-0 frame of a transaction reference.
const REF_SESSION_OFFSET: usize = REF_TERM_OFFSET_OFFSET + 4;

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
    /// Parse the fields after the record type: `[lane:u8][envelope]`.
    pub(super) fn parse(canonical_id: B256, fields: &[u8]) -> Result<Self, WireError> {
        Self::from_parts(canonical_id, rd_u8(fields, 0)?, &fields[1..])
    }

    /// The entry of `lane` with the archived `envelope`. The envelope is
    /// copied to an aligned buffer before rkyv reads it, because it sits at
    /// an odd offset in the frame. An id that is not the hash in the
    /// envelope is refused: the frame was mis-encoded.
    fn from_parts(canonical_id: B256, lane: u8, envelope: &[u8]) -> Result<Self, WireError> {
        let envelope: TxEnvelope =
            decode_rkyv_body(envelope).map_err(|e| WireError::BadTx(e.to_string()))?;
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

/// A full reference: a positioned transaction reference and the archived
/// envelope of its transaction, offered on ingress as record type
/// [`RT_TXREF_OFFER`]. The first [`TXREF_RECORD_LEN`] bytes of its frame are
/// the frame of the reference with record type 7, and the envelope follows.
/// The deadline field of the guard carries the position of the reference in
/// its image, tagged with [`POSITION_TAG`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxRefOffer<'a> {
    /// The fields that the sealer checks and orders by, and never relays.
    pub guard: GuardHeader,
    /// The transaction hash, the lane, and the place of the transaction in
    /// its `tx_data` image.
    pub reference: TxRef,
    /// The archived `TxEnvelope`.
    pub envelope: &'a [u8],
}

impl<'a> TxRefOffer<'a> {
    /// Encode this reference as an ingress kind-0 frame.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(TXREF_RECORD_LEN + self.envelope.len());
        self.guard
            .write_reference(&mut b, &self.reference, RT_TXREF_OFFER);
        b.extend_from_slice(self.envelope);
        b
    }

    /// Read a kind-0 frame of record type [`RT_TXREF_OFFER`]. It refuses
    /// another kind, another record type, and an envelope shorter than
    /// [`ENVELOPE_ROOT_LEN`]. It does not decode the envelope: see
    /// [`Self::canonical`].
    ///
    /// # Errors
    ///
    /// Returns [`WireError::BadIngressKind`], [`WireError::BadRecordType`],
    /// or [`WireError::TooShort`].
    pub fn parse(buf: &'a [u8]) -> Result<Self, WireError> {
        let kind = rd_u8(buf, 0)?;
        if kind != KIND_INGRESS_RECORD {
            return Err(WireError::BadIngressKind(kind));
        }
        let record_type = rd_u8(buf, RECORD_TYPE_OFFSET)?;
        if record_type != RT_TXREF_OFFER {
            return Err(WireError::BadRecordType(record_type));
        }
        rd_slice(buf, TXREF_RECORD_LEN, ENVELOPE_ROOT_LEN)?;
        let position = BPosition {
            term_id: rd_i32(buf, REF_TERM_ID_OFFSET)?,
            term_offset: rd_i32(buf, REF_TERM_OFFSET_OFFSET)?,
        };
        Ok(Self {
            guard: GuardHeader::read(buf)?,
            reference: TxRef::new(
                B256::from_slice(rd_slice(
                    buf,
                    INGRESS_CANONICAL_ID_OFFSET,
                    CANONICAL_ID_LEN,
                )?),
                rd_u8(buf, REF_LANE_OFFSET)?,
                position,
                rd_i32(buf, REF_SESSION_OFFSET)?,
            ),
            envelope: &buf[TXREF_RECORD_LEN..],
        })
    }

    /// The stream position that the deadline field carries, or `None` when
    /// the field holds no [`POSITION_TAG`].
    #[must_use]
    pub fn position(&self) -> Option<u64> {
        let deadline = self.guard.max_inclusion_block;
        (deadline & !POSITION_MASK == POSITION_TAG).then_some(deadline & POSITION_MASK)
    }

    /// The relayed payload that the sealer builds from this reference:
    /// `[canonical_id:32][record_type=6][lane:u8][envelope]`.
    #[must_use]
    pub fn canonical_payload(&self) -> Vec<u8> {
        [
            self.reference.tx_hash.as_slice(),
            &[RT_TX, self.reference.shard_id],
            self.envelope,
        ]
        .concat()
    }

    /// The canonical entry of this reference. It decodes the envelope and
    /// checks that its hash is the canonical id.
    ///
    /// # Errors
    ///
    /// Returns [`WireError::BadTx`] if the envelope does not decode, or if
    /// its hash is not the canonical id.
    pub fn canonical(&self) -> Result<CanonicalTx, WireError> {
        CanonicalTx::from_parts(
            self.reference.tx_hash,
            self.reference.shard_id,
            self.envelope,
        )
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
