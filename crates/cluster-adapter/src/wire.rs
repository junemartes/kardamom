//! Application envelope shared with the Java clustered service.
//!
//! Two directions, all little-endian (matching the cluster SBE schema and the
//! Java framing in `SealerClusteredService`):
//!
//! **Ingress** (Rust sequencer → cluster), one app message per record:
//! ```text
//!   [kind:u8 = 0][sender:20][nonce:u64 LE][canonical_id:32][record_type:u8][fields…]
//!     TxRef       fields = [shard_id:u8][tx_data_position.term_id:i32][.term_offset:i32][tx_data_session_id:i32]
//!     DepositRef  fields = [deposit_position.term_id:i32][.term_offset:i32]
//! ```
//! The Java service parses `sender`/`nonce` for the per-sender contiguity
//! guard (#85 fix B: a known sender's ref whose nonce is not the expected
//! next one is REJECTED with [`EGRESS_KIND_CONTIGUITY_REJECT`] instead of
//! silently sealing a canonical nonce gap) and `canonical_id` (at its fixed
//! offset) for dedup, then relays everything from `canonical_id` onward
//! verbatim — it never inspects `record_type`/`fields`. The guard header
//! sits BEFORE the canonical id precisely so the **relayed payload** stays
//! `[canonical_id:32][record_type:u8][fields…]` — executors are untouched.
//! An all-zero sender is guard-exempt (deposits carry no sender nonce).
//!
//! **Egress** (cluster → Rust executor):
//! ```text
//!   relayed record:  [kind:u8 = 1][index:u64][payload_len:u32][relayed payload…]
//!   block boundary:  [kind:u8 = 2][block_number:u64][end_tx_idx:u64][l2_timestamp:u64]
//! ```
//! `index` is the 0-based canonical record index assigned by the leader's
//! replicated state machine; the executor maps it to `BPosition::from_index`.

use alloy_primitives::{Address, B256};
use kardamom_types::{BPosition, BlockBoundaryStart, DepositRef, TxOrderingMessage, TxRef};
use thiserror::Error;

/// Ingress app-message kind (the leading tag byte).
pub const KIND_INGRESS_RECORD: u8 = 0;
/// Ingress kind: egress-subscribe announcement `[kind:u8 = 2]` — the sending
/// session wants the canonical egress broadcast (relayed records +
/// boundaries). Matches Java `KIND_SUBSCRIBE`.
pub const KIND_SUBSCRIBE: u8 = 2;
/// Ingress kind: a batch of ingress records
/// `[kind:u8 = 3][count:u16 LE][per entry: len:u32 LE + entry bytes]`, where
/// each entry is a complete single-record ingress frame
/// (`[kind:u8 = 0][sender:20][nonce:u64][canonical_id:32][payload…]`). One
/// cluster offer carries the whole batch; the service unpacks and processes
/// entries exactly like individually-offered records, so consensus
/// determinism, dedup, the contiguity guard and the egress format are all
/// unchanged — batching is purely an ingress-transport amortization
/// (~75-byte refs each previously paid a full offer round trip). Matches
/// Java `KIND_BATCH`.
pub const KIND_BATCH: u8 = 3;
/// Ingress kind: a replay request `[kind:u8 = 1][from_index:u64][from_block:u64]`.
/// The service re-offers retained egress frames with `record.index >= from_index`
/// or `boundary.block_number >= from_block` to the REQUESTING session only (not
/// deduped, not relayed, no canonical id). Matches Java `KIND_REPLAY_REQUEST`.
pub const KIND_REPLAY_REQUEST: u8 = 1;
/// Egress kind: a relayed canonical record. Matches Java `EGRESS_KIND_RELAYED`.
pub const EGRESS_KIND_RELAYED: u8 = 1;
/// Egress kind: a generated block boundary. Matches Java `EGRESS_KIND_BOUNDARY`.
pub const EGRESS_KIND_BOUNDARY: u8 = 2;
/// Egress kind: replay refused — the requested range predates the service's
/// bounded in-memory retention: `[kind:u8 = 3][oldest_index:u64][oldest_block:u64]`.
/// The consumer cannot recover the gap and must fail-stop (full resync).
/// Matches Java `EGRESS_KIND_REPLAY_UNAVAILABLE`.
pub const EGRESS_KIND_REPLAY_UNAVAILABLE: u8 = 3;
/// Egress kind: replay complete — all retained frames at/after the requested
/// cursor have been re-offered: `[kind:u8 = 4][up_to_index:u64][up_to_block:u64]`
/// (exclusive: the NEXT live record index / boundary block at completion time).
/// The consumer exits catch-up ordering mode. Matches Java `EGRESS_KIND_REPLAY_DONE`.
pub const EGRESS_KIND_REPLAY_DONE: u8 = 4;
/// Egress kind: contiguity reject (#85 fix B) — a known sender's ingress
/// record carried a nonce other than the expected next one, so sealing it
/// would commit a canonical nonce gap:
/// `[kind:u8 = 5][sender:20][nonce:u64][expected:u64]`. Sent to the OFFERING
/// session only; the sequencer rewinds its unconfirmed ledger to `expected`
/// and republishes (#114's machinery), converting a silent gap into a
/// recoverable signal. Matches Java `EGRESS_KIND_CONTIGUITY_REJECT`.
pub const EGRESS_KIND_CONTIGUITY_REJECT: u8 = 5;

/// Record discriminant inside the relayed payload.
pub const RT_TXREF: u8 = 0;
pub const RT_DEPOSITREF: u8 = 1;

/// Canonical id length (a 32-byte hash). Matches Java `CANONICAL_ID_LEN`.
pub const CANONICAL_ID_LEN: usize = 32;

/// Sender address length in the ingress guard header. Matches Java `SENDER_LEN`.
pub const SENDER_LEN: usize = 20;
/// Ingress record layout offsets. Matches Java `SENDER_OFFSET` /
/// `NONCE_OFFSET` / `CANONICAL_ID_OFFSET` in `SealerClusteredService`.
pub const INGRESS_SENDER_OFFSET: usize = 1;
pub const INGRESS_NONCE_OFFSET: usize = INGRESS_SENDER_OFFSET + SENDER_LEN;
pub const INGRESS_CANONICAL_ID_OFFSET: usize = INGRESS_NONCE_OFFSET + 8;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum WireError {
    #[error("buffer too short: need {need} at offset {at}, have {have}")]
    TooShort { at: usize, need: usize, have: usize },
    #[error("unknown egress kind {0}")]
    BadEgressKind(u8),
    #[error("unknown record type {0}")]
    BadRecordType(u8),
    #[error("declared payload_len {declared} exceeds remaining {remaining}")]
    BadPayloadLen { declared: usize, remaining: usize },
}

// ── encode (ingress: Rust → cluster) ────────────────────────────────────────

/// Encode a `TxRef` as an ingress app message. `sender`/`nonce` feed the
/// service's per-sender contiguity guard (#85 fix B) and are NOT part of the
/// relayed payload — executors never see them.
pub fn encode_ingress_txref(r: &TxRef, sender: Address, nonce: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(INGRESS_CANONICAL_ID_OFFSET + CANONICAL_ID_LEN + 1 + 1 + 8 + 4);
    b.push(KIND_INGRESS_RECORD);
    b.extend_from_slice(sender.as_slice()); // sender (20)
    b.extend_from_slice(&nonce.to_le_bytes());
    b.extend_from_slice(r.tx_hash.as_slice()); // canonical_id (32)
    b.push(RT_TXREF);
    b.push(r.shard_id);
    b.extend_from_slice(&r.tx_data_position.term_id.to_le_bytes());
    b.extend_from_slice(&r.tx_data_position.term_offset.to_le_bytes());
    // tx_data publisher session (active/active ingress join discriminator).
    b.extend_from_slice(&r.tx_data_session_id.to_le_bytes());
    b
}

/// Encode a `DepositRef` as an ingress app message. Deposits carry no sender
/// nonce; the all-zero sender marks the record guard-exempt.
pub fn encode_ingress_depositref(r: &DepositRef) -> Vec<u8> {
    let mut b = Vec::with_capacity(INGRESS_CANONICAL_ID_OFFSET + CANONICAL_ID_LEN + 1 + 8);
    b.push(KIND_INGRESS_RECORD);
    b.extend_from_slice(Address::ZERO.as_slice()); // guard-exempt sender
    b.extend_from_slice(&0u64.to_le_bytes());
    b.extend_from_slice(r.source_hash.as_slice()); // canonical_id (32)
    b.push(RT_DEPOSITREF);
    b.extend_from_slice(&r.deposit_position.term_id.to_le_bytes());
    b.extend_from_slice(&r.deposit_position.term_offset.to_le_bytes());
    b
}

// ── decode (egress: cluster → Rust) ─────────────────────────────────────────

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
    /// Contiguity reject (#85 fix B): the service refused to seal `sender`'s
    /// ref at `nonce` because it expected `expected` — republish from
    /// `expected` (the unconfirmed ledger holds the missing refs).
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
            Ok(EgressItem::Boundary(BlockBoundaryStart {
                block_number,
                end_tx_idx: BPosition::from_index(end_tx_idx),
                l2_timestamp,
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

/// Encode an egress-subscribe announcement (ingress): `[kind:u8 = 2]`. A
/// session that sends this is a canonical-stream consumer — the service
/// includes it in the per-record/per-boundary egress fan-out. Publisher-only
/// sessions (sequencers) never send it and stop receiving the canonical
/// broadcast they were dropping client-side anyway.
pub fn encode_subscribe() -> Vec<u8> {
    vec![KIND_SUBSCRIBE]
}

/// Encode a batch of already-encoded single-record ingress frames.
pub fn encode_ingress_batch(entries: &[Vec<u8>]) -> Vec<u8> {
    let payload: usize = entries.iter().map(|e| 4 + e.len()).sum();
    let mut b = Vec::with_capacity(1 + 2 + payload);
    b.push(KIND_BATCH);
    b.extend_from_slice(&(entries.len() as u16).to_le_bytes());
    for e in entries {
        b.extend_from_slice(&(e.len() as u32).to_le_bytes());
        b.extend_from_slice(e);
    }
    b
}

/// Decode a batch frame — used by tests / a Rust service mock (the real
/// service-side decode lives in the Java `SealerClusteredService`).
pub fn decode_ingress_batch(buf: &[u8]) -> Result<Vec<&[u8]>, WireError> {
    if buf.first() != Some(&KIND_BATCH) {
        return Err(WireError::BadEgressKind(*buf.first().unwrap_or(&255)));
    }
    let hdr = buf.get(1..3).ok_or(WireError::TooShort {
        at: 1,
        need: 2,
        have: buf.len().saturating_sub(1),
    })?;
    let count = u16::from_le_bytes([hdr[0], hdr[1]]) as usize;
    let mut out = Vec::with_capacity(count);
    let mut pos = 3usize;
    for _ in 0..count {
        let len_bytes = buf.get(pos..pos + 4).ok_or(WireError::TooShort {
            at: pos,
            need: 4,
            have: buf.len().saturating_sub(pos),
        })?;
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        pos += 4;
        out.push(buf.get(pos..pos + len).ok_or(WireError::TooShort {
            at: pos,
            need: len,
            have: buf.len().saturating_sub(pos),
        })?);
        pos += len;
    }
    Ok(out)
}

/// Encode a replay request (ingress). The service re-offers retained frames
/// from `(from_index, from_block)` to the requesting session.
pub fn encode_replay_request(from_index: u64, from_block: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 8);
    b.push(KIND_REPLAY_REQUEST);
    b.extend_from_slice(&from_index.to_le_bytes());
    b.extend_from_slice(&from_block.to_le_bytes());
    b
}

/// Decode a replay request — used by tests / a Rust service mock (the real
/// service-side decode lives in the Java `SealerClusteredService`).
pub fn decode_replay_request(buf: &[u8]) -> Result<(u64, u64), WireError> {
    if buf.first() != Some(&KIND_REPLAY_REQUEST) {
        return Err(WireError::BadEgressKind(*buf.first().unwrap_or(&255)));
    }
    Ok((rd_u64(buf, 1)?, rd_u64(buf, 9)?))
}

/// Frame a replay-unavailable notice exactly as the Java service does.
pub fn encode_replay_unavailable(oldest_index: u64, oldest_block: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 8);
    b.push(EGRESS_KIND_REPLAY_UNAVAILABLE);
    b.extend_from_slice(&oldest_index.to_le_bytes());
    b.extend_from_slice(&oldest_block.to_le_bytes());
    b
}

/// Frame a replay-done marker exactly as the Java service does.
pub fn encode_replay_done(up_to_index: u64, up_to_block: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 8);
    b.push(EGRESS_KIND_REPLAY_DONE);
    b.extend_from_slice(&up_to_index.to_le_bytes());
    b.extend_from_slice(&up_to_block.to_le_bytes());
    b
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

/// Decode a relayed payload `[canonical_id:32][record_type:u8][fields…]` into a
/// `TxOrderingMessage` (the original `tx_data`/`deposit` position is recovered;
/// the canonical L2 position is assigned by the caller from `index`).
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
        other => Err(WireError::BadRecordType(other)),
    }
}

// ── encode (egress: mirrors the Java framing — for tests / a Rust service mock) ─

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
pub fn encode_egress_boundary(block_number: u64, end_tx_idx: u64, l2_timestamp: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8 + 8 + 8);
    b.push(EGRESS_KIND_BOUNDARY);
    b.extend_from_slice(&block_number.to_le_bytes());
    b.extend_from_slice(&end_tx_idx.to_le_bytes());
    b.extend_from_slice(&l2_timestamp.to_le_bytes());
    b
}

/// The `(canonical_id, relayed_payload)` a cluster service extracts from an
/// ingress message — `relayed_payload` is everything from `canonical_id`
/// onward (what the Java service forwards to egress); the guard header
/// (`sender`/`nonce`) before it is consumed by the service and never
/// relayed. Used by the in-Rust service mock in tests.
pub fn split_ingress(buf: &[u8]) -> Result<([u8; 32], &[u8]), WireError> {
    let payload = buf
        .get(INGRESS_CANONICAL_ID_OFFSET..)
        .ok_or(WireError::TooShort {
            at: INGRESS_CANONICAL_ID_OFFSET,
            need: 0,
            have: buf.len(),
        })?;
    let cid: [u8; 32] = payload
        .get(0..CANONICAL_ID_LEN)
        .ok_or(WireError::TooShort {
            at: 0,
            need: CANONICAL_ID_LEN,
            have: payload.len(),
        })?
        .try_into()
        .unwrap();
    Ok((cid, payload))
}

/// The `(sender, nonce)` guard header of an ingress record frame — what the
/// Java service feeds the per-sender contiguity guard. Used by tests and the
/// in-Rust service mock.
pub fn ingress_sender_nonce(buf: &[u8]) -> Result<(Address, u64), WireError> {
    let sender = buf
        .get(INGRESS_SENDER_OFFSET..INGRESS_SENDER_OFFSET + SENDER_LEN)
        .ok_or(WireError::TooShort {
            at: INGRESS_SENDER_OFFSET,
            need: SENDER_LEN,
            have: buf.len().saturating_sub(INGRESS_SENDER_OFFSET),
        })?;
    Ok((
        Address::from_slice(sender),
        rd_u64(buf, INGRESS_NONCE_OFFSET)?,
    ))
}

// ── byte helpers ────────────────────────────────────────────────────────────

fn rd_u32(b: &[u8], at: usize) -> Result<u32, WireError> {
    Ok(u32::from_le_bytes(slice4(b, at)?))
}
fn rd_i32(b: &[u8], at: usize) -> Result<i32, WireError> {
    Ok(i32::from_le_bytes(slice4(b, at)?))
}
fn rd_u64(b: &[u8], at: usize) -> Result<u64, WireError> {
    let s = b.get(at..at + 8).ok_or(WireError::TooShort {
        at,
        need: 8,
        have: b.len().saturating_sub(at),
    })?;
    Ok(u64::from_le_bytes(s.try_into().unwrap()))
}
fn slice4(b: &[u8], at: usize) -> Result<[u8; 4], WireError> {
    Ok(b.get(at..at + 4)
        .ok_or(WireError::TooShort {
            at,
            need: 4,
            have: b.len().saturating_sub(at),
        })?
        .try_into()
        .unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txref() -> TxRef {
        TxRef::new(
            B256::repeat_byte(0xAB),
            3,
            BPosition {
                term_id: 7,
                term_offset: 9_999,
            },
            5, // tx_data_session_id (exercises the active/active session field roundtrip)
        )
    }
    fn depositref() -> DepositRef {
        DepositRef::new(
            B256::repeat_byte(0xCD),
            BPosition {
                term_id: 1,
                term_offset: 42,
            },
        )
    }

    /// Ingress → (service relays from the canonical id) → egress → decode
    /// reproduces the TxRef; the guard header is consumed, not relayed.
    #[test]
    fn txref_ingress_relay_egress_roundtrip() {
        let r = txref();
        let sender = Address::repeat_byte(0x42);
        let ingress = encode_ingress_txref(&r, sender, 7);
        assert_eq!(ingress_sender_nonce(&ingress).unwrap(), (sender, 7));
        // Mirror the Java service: parse the id, relay from the canonical id.
        let (cid, relayed) = split_ingress(&ingress).unwrap();
        assert_eq!(cid, r.tx_hash.0);
        let egress = encode_egress_record(5, relayed);
        match decode_egress(&egress).unwrap() {
            EgressItem::Record { index, msg } => {
                assert_eq!(index, 5);
                assert_eq!(msg, TxOrderingMessage::TxRef(r));
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn depositref_ingress_relay_egress_roundtrip() {
        let r = depositref();
        let ingress = encode_ingress_depositref(&r);
        // Deposits carry the guard-exempt zero sender.
        assert_eq!(ingress_sender_nonce(&ingress).unwrap(), (Address::ZERO, 0));
        let (cid, relayed) = split_ingress(&ingress).unwrap();
        assert_eq!(cid, r.source_hash.0);
        let egress = encode_egress_record(8, relayed);
        match decode_egress(&egress).unwrap() {
            EgressItem::Record { index, msg } => {
                assert_eq!(index, 8);
                assert_eq!(msg, TxOrderingMessage::DepositRef(r));
            }
            other => panic!("expected Record, got {other:?}"),
        }
    }

    #[test]
    fn boundary_roundtrip() {
        let egress = encode_egress_boundary(12, 100, 1_700_000_000_250);
        match decode_egress(&egress).unwrap() {
            EgressItem::Boundary(b) => {
                assert_eq!(b.block_number, 12);
                assert_eq!(b.end_tx_idx.as_index(), 100);
                assert_eq!(b.l2_timestamp, 1_700_000_000_250);
            }
            other => panic!("expected Boundary, got {other:?}"),
        }
    }

    #[test]
    fn ingress_layout_is_kind_sender_nonce_id_then_fields() {
        let r = txref();
        let sender = Address::repeat_byte(0x42);
        let b = encode_ingress_txref(&r, sender, 0x0102_0304_0506_0708);
        assert_eq!(b[0], KIND_INGRESS_RECORD);
        assert_eq!(&b[1..21], sender.as_slice());
        assert_eq!(
            b[21..29],
            0x0102_0304_0506_0708u64.to_le_bytes(),
            "nonce is little-endian at offset 21 (Java NONCE_OFFSET)"
        );
        assert_eq!(&b[29..61], r.tx_hash.as_slice());
        assert_eq!(b[61], RT_TXREF);
        assert_eq!(b[62], 3); // shard_id
        // The relayed payload begins with the canonical id — the guard
        // header never reaches the executors.
        let (_cid, relayed) = split_ingress(&b).unwrap();
        assert_eq!(&relayed[0..32], r.tx_hash.as_slice());
    }

    #[test]
    fn contiguity_reject_roundtrip() {
        let sender = Address::repeat_byte(0x99);
        let b = encode_contiguity_reject(sender, 12, 8);
        match decode_egress(&b).unwrap() {
            EgressItem::ContiguityReject {
                sender: s,
                nonce,
                expected,
            } => {
                assert_eq!(s, sender);
                assert_eq!(nonce, 12);
                assert_eq!(expected, 8);
            }
            other => panic!("expected ContiguityReject, got {other:?}"),
        }
    }

    #[test]
    fn replay_request_roundtrip() {
        let b = encode_replay_request(1234, 56);
        assert_eq!(b[0], KIND_REPLAY_REQUEST);
        assert_eq!(decode_replay_request(&b).unwrap(), (1234, 56));
        // A record ingress message is NOT a replay request.
        assert!(decode_replay_request(&encode_ingress_txref(&txref(), Address::ZERO, 0)).is_err());
    }

    #[test]
    fn replay_unavailable_roundtrip() {
        let b = encode_replay_unavailable(100, 7);
        match decode_egress(&b).unwrap() {
            EgressItem::ReplayUnavailable {
                oldest_index,
                oldest_block,
            } => {
                assert_eq!(oldest_index, 100);
                assert_eq!(oldest_block, 7);
            }
            other => panic!("expected ReplayUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn bad_kind_and_record_type_error() {
        assert_eq!(decode_egress(&[9, 0, 0]), Err(WireError::BadEgressKind(9)));
        // A relayed payload with an unknown record type.
        let mut payload = vec![0u8; 32];
        payload.push(7); // record_type 7
        let egress = encode_egress_record(0, &payload);
        assert_eq!(decode_egress(&egress), Err(WireError::BadRecordType(7)));
    }

    #[test]
    fn truncated_egress_errors_cleanly() {
        assert!(matches!(
            decode_egress(&[EGRESS_KIND_RELAYED, 0, 0]),
            Err(WireError::TooShort { .. })
        ));
        assert!(decode_egress(&[]).is_err());
    }
}
