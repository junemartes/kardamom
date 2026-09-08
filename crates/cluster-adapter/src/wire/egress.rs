//! Egress-direction codec (cluster to Rust executor). It has the decoded
//! [`EgressItem`] stream, plus encoders that mirror the Java service's
//! framing byte-for-byte (used by tests and the in-Rust service mock).
//! Frame layouts are documented on the `EGRESS_KIND_*` constants in the
//! parent module.

use alloy_primitives::{Address, B256};
use kardamom_types::epoch::EpochRecord;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, BlockBoundaryStart, DepositRef, TxOrderingMessage, TxRef};
use rkyv::Archive;
use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::rancor;

use super::{
    CANONICAL_ID_LEN, EGRESS_KIND_BOUNDARY, EGRESS_KIND_CONTIGUITY_REJECT, EGRESS_KIND_RELAYED,
    EGRESS_KIND_REMOTE_ORIGIN_REJECT, EGRESS_KIND_REPLAY_DONE, EGRESS_KIND_REPLAY_UNAVAILABLE,
    RT_DEPOSITREF, RT_EPOCH, RT_REMOTE_EPOCH, RT_TXREF, SENDER_LEN, WireError, encode_kind_2u64,
    rd_i32, rd_len, rd_slice, rd_u64, too_short,
};

// ── decode (egress: cluster to Rust) ────────────────────────────────────────

/// A decoded egress item from the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EgressItem {
    /// A relayed canonical record with its assigned 0-based index.
    Record { index: u64, msg: TxOrderingMessage },
    /// A generated block boundary.
    Boundary(BlockBoundaryStart),
    /// Replay refused: the requested range predates the service's retention.
    ReplayUnavailable {
        oldest_index: u64,
        oldest_block: u64,
    },
    /// Replay complete up to (exclusive) the given live cursor.
    ReplayDone { up_to_index: u64, up_to_block: u64 },
    /// Contiguity reject. The service refused to seal `sender`'s ref at
    /// `nonce`, because it expected `expected`. Republish from `expected`:
    /// the unconfirmed ledger holds the missing refs.
    ContiguityReject {
        sender: Address,
        nonce: u64,
        expected: u64,
    },
    /// Remote-origin reject. The sealer refused a kind-5 record from
    /// `origin_chain_id` at `first_seq`. `expected_next_seq` is the
    /// sealer's lane cursor for that origin (0 when the reason is not a
    /// seq mismatch). `reason` is a `REMOTE_ORIGIN_REJECT_*` code; see
    /// [`super::remote_origin_reject_reason`].
    RemoteOriginReject {
        origin_chain_id: u64,
        first_seq: u64,
        expected_next_seq: u64,
        reason: u8,
    },
}

impl EgressItem {
    /// # Errors
    ///
    /// Returns an error if `buf` is too short for its kind byte, or its
    /// kind byte is not one of the `EGRESS_KIND_*` constants.
    pub fn decode(buf: &[u8]) -> Result<Self, WireError> {
        let kind = *buf.first().ok_or_else(|| too_short(buf, 0, 1))?;
        match kind {
            EGRESS_KIND_RELAYED => Self::decode_relayed(buf),
            EGRESS_KIND_BOUNDARY => Self::decode_boundary(buf),
            EGRESS_KIND_REPLAY_UNAVAILABLE => Ok(Self::ReplayUnavailable {
                oldest_index: rd_u64(buf, 1)?,
                oldest_block: rd_u64(buf, 9)?,
            }),
            EGRESS_KIND_REPLAY_DONE => Ok(Self::ReplayDone {
                up_to_index: rd_u64(buf, 1)?,
                up_to_block: rd_u64(buf, 9)?,
            }),
            EGRESS_KIND_CONTIGUITY_REJECT => Self::decode_contiguity_reject(buf),
            EGRESS_KIND_REMOTE_ORIGIN_REJECT => Self::decode_remote_origin_reject(buf),
            other => Err(WireError::BadEgressKind(other)),
        }
    }

    fn decode_relayed(buf: &[u8]) -> Result<Self, WireError> {
        let index = rd_u64(buf, 1)?;
        let payload_len = rd_len(buf, 9)?;
        let start = 13;
        let payload = rd_slice(buf, start, payload_len).map_err(|_| WireError::BadPayloadLen {
            declared: payload_len,
            remaining: buf.len().saturating_sub(start),
        })?;
        Ok(Self::Record {
            index,
            msg: RelayedPayload::parse(payload)?.decode()?,
        })
    }

    fn decode_boundary(buf: &[u8]) -> Result<Self, WireError> {
        let block_number = rd_u64(buf, 1)?;
        let end_tx_idx = rd_u64(buf, 9)?;
        let l2_timestamp = rd_u64(buf, 17)?;
        let l1_origin = rd_u64(buf, 25)?;
        Ok(Self::Boundary(BlockBoundaryStart {
            block_number,
            end_tx_idx: BPosition::from_index(end_tx_idx),
            l2_timestamp,
            l1_origin,
        }))
    }

    fn decode_contiguity_reject(buf: &[u8]) -> Result<Self, WireError> {
        let sender = rd_slice(buf, 1, SENDER_LEN)?;
        Ok(Self::ContiguityReject {
            sender: Address::from_slice(sender),
            nonce: rd_u64(buf, 1 + SENDER_LEN)?,
            expected: rd_u64(buf, 1 + SENDER_LEN + 8)?,
        })
    }

    fn decode_remote_origin_reject(buf: &[u8]) -> Result<Self, WireError> {
        Ok(Self::RemoteOriginReject {
            origin_chain_id: rd_u64(buf, 1)?,
            first_seq: rd_u64(buf, 9)?,
            expected_next_seq: rd_u64(buf, 17)?,
            reason: *buf.get(25).ok_or_else(|| too_short(buf, 25, 1))?,
        })
    }
}

/// A relayed payload slice, `[canonical_id:32][record_type:u8][fields…]`,
/// parsed once in [`RelayedPayload::parse`] so every record-type decoder
/// below is a method reading `self.id`/`self.fields`, rather than a free
/// function each re-deriving them from a raw `&[u8]`.
struct RelayedPayload<'a> {
    id: B256,
    record_type: u8,
    fields: &'a [u8],
}

impl<'a> RelayedPayload<'a> {
    fn parse(p: &'a [u8]) -> Result<Self, WireError> {
        let cid = rd_slice(p, 0, CANONICAL_ID_LEN)?;
        let record_type = *p
            .get(CANONICAL_ID_LEN)
            .ok_or_else(|| too_short(p, CANONICAL_ID_LEN, 1))?;
        Ok(Self {
            id: B256::from_slice(cid),
            record_type,
            fields: &p[CANONICAL_ID_LEN + 1..],
        })
    }

    /// Decode `self.fields` into a [`TxOrderingMessage`]. This recovers
    /// the original `tx_data` or `deposit` position. The caller assigns
    /// the canonical L2 position from the relayed record's `index`.
    fn decode(&self) -> Result<TxOrderingMessage, WireError> {
        match self.record_type {
            RT_TXREF => self.decode_txref(),
            RT_DEPOSITREF => self.decode_depositref(),
            RT_EPOCH => self.decode_epoch(),
            RT_REMOTE_EPOCH => self.decode_remote_epoch(),
            other => Err(WireError::BadRecordType(other)),
        }
    }

    fn decode_txref(&self) -> Result<TxOrderingMessage, WireError> {
        let shard_id = *self
            .fields
            .first()
            .ok_or_else(|| too_short(self.fields, 0, 1))?;
        let term_id = rd_i32(self.fields, 1)?;
        let term_offset = rd_i32(self.fields, 5)?;
        let tx_data_session_id = rd_i32(self.fields, 9)?;
        Ok(TxOrderingMessage::TxRef(TxRef::new(
            self.id,
            shard_id,
            BPosition {
                term_id,
                term_offset,
            },
            tx_data_session_id,
        )))
    }

    fn decode_depositref(&self) -> Result<TxOrderingMessage, WireError> {
        let term_id = rd_i32(self.fields, 0)?;
        let term_offset = rd_i32(self.fields, 4)?;
        Ok(TxOrderingMessage::DepositRef(DepositRef::new(
            self.id,
            BPosition {
                term_id,
                term_offset,
            },
        )))
    }

    fn decode_epoch(&self) -> Result<TxOrderingMessage, WireError> {
        let epoch: EpochRecord =
            decode_rkyv_body(self.fields).map_err(|e| WireError::BadEpoch(e.to_string()))?;
        // The canonical id comes from the epoch itself. So if a relayed
        // record's id does not match its payload, the record was
        // tampered with or mis-encoded. Reject it instead of trusting
        // the header.
        if epoch.canonical_id() != self.id {
            return Err(WireError::BadEpoch(format!(
                "canonical id {} does not match epoch for L1 block {}",
                self.id, epoch.l1_number
            )));
        }
        Ok(TxOrderingMessage::Epoch(epoch))
    }

    fn decode_remote_epoch(&self) -> Result<TxOrderingMessage, WireError> {
        let rec: RemoteEpochRecord =
            decode_rkyv_body(self.fields).map_err(|e| WireError::BadRemoteEpoch(e.to_string()))?;
        // The id commits to the pair's (origin, anchor number, anchor
        // hash, seq range), so a mismatch means the header and the batch
        // disagree about WHICH slice of the pair's sequence this is —
        // the one thing dedup cannot be allowed to get wrong. The sealer
        // reads the anchor and the seq range from the header only, so
        // this check is what binds those header fields to the body.
        if rec.canonical_id() != self.id {
            return Err(WireError::BadRemoteEpoch(format!(
                "canonical id {} does not match remote epoch from chain {} seqs {}..={}",
                self.id,
                rec.origin_chain_id,
                rec.first_seq,
                rec.last_seq()
            )));
        }
        Ok(TxOrderingMessage::RemoteEpoch(rec))
    }
}

/// Decode an rkyv body that sits at a byte offset that is never aligned in
/// place, so rkyv refuses to read it without a copy into an aligned buffer
/// first. The buffer alignment is 16: the archived `Deposit` and
/// `XChainMessage` carry a `u128`, so their archived forms need 16, and
/// rkyv's default is 16. An 8-aligned buffer only worked when the
/// allocator happened to hand out a 16-aligned block. Shared by the epoch
/// and remote-epoch relayed record types; every other record type decodes
/// field-by-field and never hits this. These records happen at most once
/// per L1 (or origin) block, so the copy cost is small. The alternative,
/// padding the frame to realign it, would have to survive the Java relay
/// byte-for-byte.
fn decode_rkyv_body<T>(fields: &[u8]) -> Result<T, rancor::Error>
where
    T: Archive,
    T::Archived: rkyv::Deserialize<T, HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<HighValidator<'a, rancor::Error>>,
{
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(fields.len());
    aligned.extend_from_slice(fields);
    rkyv::from_bytes::<T, rancor::Error>(&aligned)
}

// ── encode (egress: mirrors Java framing, for tests and a Rust service mock) ─

/// Frame a relayed record exactly as the Java service does. `payload` is the
/// relayed payload (`[canonical_id:32][record_type][fields…]`).
///
/// # Errors
///
/// Returns [`WireError::EntryTooLarge`] if `payload.len()` does not fit
/// in a `u32`. Every relayed payload rides one Aeron frame (MTU about
/// 1408 bytes), far under that bound.
pub fn encode_egress_record(index: u64, payload: &[u8]) -> Result<Vec<u8>, WireError> {
    let len = u32::try_from(payload.len())
        .map_err(|_| WireError::EntryTooLarge { len: payload.len() })?;
    let mut b = Vec::with_capacity(1 + 8 + 4 + payload.len());
    b.push(EGRESS_KIND_RELAYED);
    b.extend_from_slice(&index.to_le_bytes());
    b.extend_from_slice(&len.to_le_bytes());
    b.extend_from_slice(payload);
    Ok(b)
}

/// Frame a block boundary exactly as the Java service does.
#[must_use]
pub fn encode_egress_boundary(
    block_number: u64,
    end_tx_idx: u64,
    l2_timestamp: u64,
    l1_origin: u64,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 8 + 8 + 8);
    b.push(EGRESS_KIND_BOUNDARY);
    b.extend_from_slice(&block_number.to_le_bytes());
    b.extend_from_slice(&end_tx_idx.to_le_bytes());
    b.extend_from_slice(&l2_timestamp.to_le_bytes());
    b.extend_from_slice(&l1_origin.to_le_bytes());
    b
}

/// Frame a replay-unavailable notice exactly as the Java service does.
#[must_use]
pub fn encode_replay_unavailable(oldest_index: u64, oldest_block: u64) -> Vec<u8> {
    encode_kind_2u64(EGRESS_KIND_REPLAY_UNAVAILABLE, oldest_index, oldest_block)
}

/// Frame a replay-done marker exactly as the Java service does.
#[must_use]
pub fn encode_replay_done(up_to_index: u64, up_to_block: u64) -> Vec<u8> {
    encode_kind_2u64(EGRESS_KIND_REPLAY_DONE, up_to_index, up_to_block)
}

/// Frame a contiguity reject exactly as the Java service does. The real
/// encoder is the Java service; this is a test and mock-server helper.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_contiguity_reject(sender: Address, nonce: u64, expected: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + SENDER_LEN + 8 + 8);
    b.push(EGRESS_KIND_CONTIGUITY_REJECT);
    b.extend_from_slice(sender.as_slice());
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(&expected.to_le_bytes());
    b
}

/// Frame a remote-origin reject exactly as the Java service does.
pub fn encode_remote_origin_reject(
    origin_chain_id: u64,
    first_seq: u64,
    expected_next_seq: u64,
    reason: u8,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 8 + 8 + 1);
    b.push(EGRESS_KIND_REMOTE_ORIGIN_REJECT);
    b.extend_from_slice(&origin_chain_id.to_le_bytes());
    b.extend_from_slice(&first_seq.to_le_bytes());
    b.extend_from_slice(&expected_next_seq.to_le_bytes());
    b.push(reason);
    b
}
