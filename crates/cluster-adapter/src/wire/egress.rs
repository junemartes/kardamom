//! Egress-direction codec (cluster to Rust executor). It has the decoded
//! [`EgressItem`] stream, plus encoders that mirror the Java service's
//! framing byte-for-byte (used by tests and the in-Rust service mock).
//! Frame layouts are documented on the `EGRESS_KIND_*` constants in the
//! parent module.

use alloy_primitives::{Address, B256};
use kardamom_types::epoch::EpochRecord;
use kardamom_types::xchain::RemoteEpochRecord;
use kardamom_types::{BPosition, BlockBoundaryStart, DepositRef, TxOrderingMessage, TxRef};

use super::{
    CANONICAL_ID_LEN, EGRESS_KIND_BOUNDARY, EGRESS_KIND_CONTIGUITY_REJECT, EGRESS_KIND_RELAYED,
    EGRESS_KIND_REPLAY_DONE, EGRESS_KIND_REPLAY_UNAVAILABLE, RT_DEPOSITREF, RT_EPOCH,
    RT_REMOTE_EPOCH, RT_TXREF, SENDER_LEN, WireError, encode_kind_2u64, rd_i32, rd_u32, rd_u64,
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
}

pub fn decode_egress(buf: &[u8]) -> Result<EgressItem, WireError> {
    let kind = *buf.first().ok_or(WireError::TooShort {
        at: 0,
        need: 1,
        have: 0,
    })?;
    match kind {
        EGRESS_KIND_RELAYED => {
            let index = rd_u64(buf, 1)?;
            let payload_len = rd_u32(buf, 9)? as usize;
            let start = 13;
            let payload = buf
                .get(start..start + payload_len)
                .ok_or(WireError::BadPayloadLen {
                    declared: payload_len,
                    remaining: buf.len().saturating_sub(start),
                })?;
            Ok(EgressItem::Record {
                index,
                msg: decode_relayed_payload(payload)?,
            })
        }
        EGRESS_KIND_BOUNDARY => {
            let block_number = rd_u64(buf, 1)?;
            let end_tx_idx = rd_u64(buf, 9)?;
            let l2_timestamp = rd_u64(buf, 17)?;
            let l1_origin = rd_u64(buf, 25)?;
            Ok(EgressItem::Boundary(BlockBoundaryStart {
                block_number,
                end_tx_idx: BPosition::from_index(end_tx_idx),
                l2_timestamp,
                l1_origin,
            }))
        }
        EGRESS_KIND_REPLAY_UNAVAILABLE => Ok(EgressItem::ReplayUnavailable {
            oldest_index: rd_u64(buf, 1)?,
            oldest_block: rd_u64(buf, 9)?,
        }),
        EGRESS_KIND_REPLAY_DONE => Ok(EgressItem::ReplayDone {
            up_to_index: rd_u64(buf, 1)?,
            up_to_block: rd_u64(buf, 9)?,
        }),
        EGRESS_KIND_CONTIGUITY_REJECT => {
            let sender = buf.get(1..1 + SENDER_LEN).ok_or(WireError::TooShort {
                at: 1,
                need: SENDER_LEN,
                have: buf.len().saturating_sub(1),
            })?;
            Ok(EgressItem::ContiguityReject {
                sender: Address::from_slice(sender),
                nonce: rd_u64(buf, 1 + SENDER_LEN)?,
                expected: rd_u64(buf, 1 + SENDER_LEN + 8)?,
            })
        }
        other => Err(WireError::BadEgressKind(other)),
    }
}

/// Decode a relayed payload `[canonical_id:32][record_type:u8][fields…]`
/// into a `TxOrderingMessage`. This recovers the original `tx_data` or
/// `deposit` position. The caller assigns the canonical L2 position from
/// `index`.
fn decode_relayed_payload(p: &[u8]) -> Result<TxOrderingMessage, WireError> {
    let cid = p.get(0..CANONICAL_ID_LEN).ok_or(WireError::TooShort {
        at: 0,
        need: CANONICAL_ID_LEN,
        have: p.len(),
    })?;
    let id = B256::from_slice(cid);
    let rt = *p.get(CANONICAL_ID_LEN).ok_or(WireError::TooShort {
        at: CANONICAL_ID_LEN,
        need: 1,
        have: p.len().saturating_sub(CANONICAL_ID_LEN),
    })?;
    let fields = &p[CANONICAL_ID_LEN + 1..];
    match rt {
        RT_TXREF => {
            let shard_id = *fields.first().ok_or(WireError::TooShort {
                at: 0,
                need: 1,
                have: 0,
            })?;
            let term_id = rd_i32(fields, 1)?;
            let term_offset = rd_i32(fields, 5)?;
            let tx_data_session_id = rd_i32(fields, 9)?;
            Ok(TxOrderingMessage::TxRef(TxRef::new(
                id,
                shard_id,
                BPosition {
                    term_id,
                    term_offset,
                },
                tx_data_session_id,
            )))
        }
        RT_DEPOSITREF => {
            let term_id = rd_i32(fields, 0)?;
            let term_offset = rd_i32(fields, 4)?;
            Ok(TxOrderingMessage::DepositRef(DepositRef::new(
                id,
                BPosition {
                    term_id,
                    term_offset,
                },
            )))
        }
        RT_EPOCH => {
            // The rkyv body sits at offset 33 of the relayed payload (after
            // the canonical id and the record type). This offset is never
            // aligned in place, so rkyv refuses to read it without a copy
            // into an aligned buffer. The buffer alignment is 16: the
            // archived `Deposit` and `XChainMessage` carry a `u128`, so
            // their archived forms need 16, and rkyv's default is 16. An
            // 8-aligned buffer only worked when the allocator happened to
            // hand out a 16-aligned block. Every other record type decodes
            // field-by-field, so it never hits this. Epochs happen about
            // once per L1 block, so the copy cost is small. The
            // alternative, padding the frame to realign it, would have to
            // survive the Java relay byte-for-byte.
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(fields.len());
            aligned.extend_from_slice(fields);
            let epoch: EpochRecord = rkyv::from_bytes::<EpochRecord, rkyv::rancor::Error>(&aligned)
                .map_err(|e| WireError::BadEpoch(e.to_string()))?;
            // The canonical id comes from the epoch itself. So if a relayed
            // record's id does not match its payload, the record was
            // tampered with or mis-encoded. Reject it instead of trusting
            // the header.
            if epoch.canonical_id() != id {
                return Err(WireError::BadEpoch(format!(
                    "canonical id {id} does not match epoch for L1 block {}",
                    epoch.l1_number
                )));
            }
            Ok(TxOrderingMessage::Epoch(epoch))
        }
        RT_REMOTE_EPOCH => {
            // Same unaligned-body copy as RT_EPOCH, for the same reason.
            let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(fields.len());
            aligned.extend_from_slice(fields);
            let rec: RemoteEpochRecord =
                rkyv::from_bytes::<RemoteEpochRecord, rkyv::rancor::Error>(&aligned)
                    .map_err(|e| WireError::BadRemoteEpoch(e.to_string()))?;
            // The id commits to the pair's (origin, anchor, seq range), so a
            // mismatch means the header and the batch disagree about WHICH
            // slice of the pair's sequence this is — the one thing dedup
            // cannot be allowed to get wrong.
            if rec.canonical_id() != id {
                return Err(WireError::BadRemoteEpoch(format!(
                    "canonical id {id} does not match remote epoch from chain {} seqs {}..={}",
                    rec.origin_chain_id,
                    rec.first_seq,
                    rec.last_seq()
                )));
            }
            Ok(TxOrderingMessage::RemoteEpoch(rec))
        }
        other => Err(WireError::BadRecordType(other)),
    }
}

// ── encode (egress: mirrors Java framing, for tests and a Rust service mock) ─

/// Frame a relayed record exactly as the Java service does. `payload` is the
/// relayed payload (`[canonical_id:32][record_type][fields…]`).
pub fn encode_egress_record(index: u64, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 4 + payload.len());
    b.push(EGRESS_KIND_RELAYED);
    b.extend_from_slice(&index.to_le_bytes());
    b.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    b.extend_from_slice(payload);
    b
}

/// Frame a block boundary exactly as the Java service does.
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
pub fn encode_replay_unavailable(oldest_index: u64, oldest_block: u64) -> Vec<u8> {
    encode_kind_2u64(EGRESS_KIND_REPLAY_UNAVAILABLE, oldest_index, oldest_block)
}

/// Frame a replay-done marker exactly as the Java service does.
pub fn encode_replay_done(up_to_index: u64, up_to_block: u64) -> Vec<u8> {
    encode_kind_2u64(EGRESS_KIND_REPLAY_DONE, up_to_index, up_to_block)
}

/// Frame a contiguity reject exactly as the Java service does.
pub fn encode_contiguity_reject(sender: Address, nonce: u64, expected: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + SENDER_LEN + 8 + 8);
    b.push(EGRESS_KIND_CONTIGUITY_REJECT);
    b.extend_from_slice(sender.as_slice());
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(&expected.to_le_bytes());
    b
}
