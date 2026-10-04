//! Ingress-direction codec (Rust sequencer to cluster). It covers guarded
//! record frames, batches, the origin-advancing epoch record, the
//! subscribe and replay-request control messages, and the guard-header
//! splitters used by tests and the in-Rust service mock. Frame layouts
//! are documented on the `KIND_*` constants in the parent module.

use alloy_primitives::Address;
#[cfg(any(test, feature = "testing"))]
use kardamom_types::DepositRef;
use kardamom_types::TxRef;
use kardamom_types::VoidRecord;
use kardamom_types::epoch::EpochRecord;
use kardamom_types::xchain::RemoteEpochRecord;

#[cfg(any(test, feature = "testing"))]
use super::RT_DEPOSITREF;
use super::{
    CANONICAL_ID_LEN, INGRESS_CANONICAL_ID_OFFSET, INGRESS_DEADLINE_OFFSET, INGRESS_NONCE_OFFSET,
    INGRESS_SENDER_OFFSET, INGRESS_TIP_OFFSET, KIND_BATCH, KIND_INGRESS_RECORD, KIND_ORIGIN_RECORD,
    KIND_POSTED_CURSOR,
    KIND_REMOTE_ORIGIN_RECORD, KIND_REPLAY_REQUEST, KIND_SUBSCRIBE, KIND_VOID_REQUEST, RT_EPOCH,
    RT_REMOTE_EPOCH, RT_TXREF, SENDER_LEN, WireError, encode_kind_2u64, epoch_slots, rd_slice,
    rd_u64, rd_u128, remote_epoch_slots, too_short,
};

/// The guard header of a kind-0 ingress frame: the fields the service
/// checks and orders by, and never relays. Executors never see them.
///
/// Every field derives from the transaction bytes or the `tx_data`
/// envelope, so every racing replica of a shard encodes the same bytes
/// for the same record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuardHeader {
    /// Feeds the service's per-sender contiguity guard, with `nonce`.
    pub sender: Address,
    pub nonce: u64,
    /// The last block the sealer may order the record into.
    pub max_inclusion_block: u64,
    /// The amount in wei the sender bids for its place. The service
    /// orders a window of records by it, highest first. Zero when the
    /// sequencer's fee setting is off.
    pub tip: u128,
}

impl GuardHeader {
    /// The guard-exempt header: an all-zero sender, no deadline, no bid.
    /// Deposits and test records that need no guard carry it.
    pub const EXEMPT: Self = Self {
        sender: Address::ZERO,
        nonce: 0,
        max_inclusion_block: u64::MAX,
        tip: 0,
    };

    fn write(&self, b: &mut Vec<u8>) {
        b.extend_from_slice(self.sender.as_slice()); // sender (20)
        b.extend_from_slice(&self.nonce.to_le_bytes());
        b.extend_from_slice(&self.max_inclusion_block.to_le_bytes());
        b.extend_from_slice(&self.tip.to_le_bytes());
    }
}

// ── encode (ingress: Rust to cluster) ───────────────────────────────────────

/// Encode a `TxRef` as an ingress app message: the [`GuardHeader`], then
/// the relayed payload.
#[must_use]
pub fn encode_ingress_txref(r: &TxRef, guard: GuardHeader) -> Vec<u8> {
    let mut b = Vec::with_capacity(INGRESS_CANONICAL_ID_OFFSET + CANONICAL_ID_LEN + 1 + 1 + 8 + 4);
    b.push(KIND_INGRESS_RECORD);
    guard.write(&mut b);
    b.extend_from_slice(r.tx_hash.as_slice()); // canonical_id (32)
    b.push(RT_TXREF);
    b.push(r.shard_id);
    b.extend_from_slice(&r.tx_data_position.term_id.to_le_bytes());
    b.extend_from_slice(&r.tx_data_position.term_offset.to_le_bytes());
    // tx_data publisher session. This is the active/active ingress join
    // discriminator.
    b.extend_from_slice(&r.tx_data_session_id.to_le_bytes());
    b
}

/// Encode a `DepositRef` as an ingress app message. Deposits carry no
/// sender nonce. The all-zero sender marks the record as exempt from the
/// guard check.
///
/// No production caller: the sequencer publishes epochs, not
/// `DepositRef`s. Kept for the wire round-trip test.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn encode_ingress_depositref(r: &DepositRef) -> Vec<u8> {
    let mut b = Vec::with_capacity(INGRESS_CANONICAL_ID_OFFSET + CANONICAL_ID_LEN + 1 + 8);
    b.push(KIND_INGRESS_RECORD);
    // A deposit derives from L1, never expires, and bids nothing.
    GuardHeader::EXEMPT.write(&mut b);
    b.extend_from_slice(r.source_hash.as_slice()); // canonical_id (32)
    b.push(RT_DEPOSITREF);
    b.extend_from_slice(&r.deposit_position.term_id.to_le_bytes());
    b.extend_from_slice(&r.deposit_position.term_offset.to_le_bytes());
    b
}

/// Encode an [`EpochRecord`] as an origin-advancing ingress message:
/// `[kind=4][canonical_id:32][l1_origin:u64][slot_count:u32][RT_EPOCH][rkyv
/// EpochRecord…]`.
///
/// The service closes the current block before relaying this, so the
/// epoch's deposits lead a new block. It also adopts `l1_origin` for
/// later boundaries. `slot_count` is [`epoch_slots`].
///
/// # Errors
///
/// Returns an error if `epoch` fails to rkyv-serialize, or its deposit
/// count overflows `u32`.
pub fn encode_ingress_epoch(epoch: &EpochRecord) -> Result<Vec<u8>, WireError> {
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(epoch)
        .map_err(|e| WireError::BadEpoch(e.to_string()))?;
    let slots = u32::try_from(epoch_slots(epoch)).map_err(|_| {
        WireError::BadEpoch(format!("{} deposits overflows u32", epoch.deposits.len()))
    })?;
    let mut b = Vec::with_capacity(1 + CANONICAL_ID_LEN + 8 + 4 + 1 + body.len());
    b.push(KIND_ORIGIN_RECORD);
    b.extend_from_slice(epoch.canonical_id().as_slice());
    b.extend_from_slice(&epoch.l1_number.to_le_bytes());
    b.extend_from_slice(&slots.to_le_bytes());
    b.push(RT_EPOCH);
    b.extend_from_slice(&body);
    Ok(b)
}

/// Encode a [`RemoteEpochRecord`] as a REMOTE-ORIGIN-ADVANCING ingress
/// message: `[kind=5][canonical_id:32][origin_chain_id:u64][anchor_number:u64]
/// [slot_count:u32][first_seq:u64][last_seq:u64][RT_REMOTE_EPOCH][rkyv
/// RemoteEpochRecord…]`.
///
/// The two u64s are a pair, not a number: `anchor_number` positions the record
/// only within `origin_chain_id`, so the sealer keys its marker on both.
/// `slot_count` is [`remote_epoch_slots`]. `first_seq` and `last_seq` feed
/// the sealer's lane contiguity guard. See [`KIND_REMOTE_ORIGIN_RECORD`]
/// for why this is a distinct kind rather than a record type under
/// [`KIND_ORIGIN_RECORD`].
///
/// # Errors
///
/// Returns an error if `rec` fails to rkyv-serialize, or its message
/// count overflows `u32`.
pub fn encode_ingress_remote_epoch(rec: &RemoteEpochRecord) -> Result<Vec<u8>, WireError> {
    // `rec.messages` is a `NonEmptyVec`, so an empty record cannot be
    // built. No empty-batch check is needed at the wire boundary.
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(rec)
        .map_err(|e| WireError::BadRemoteEpoch(e.to_string()))?;
    let slots = u32::try_from(remote_epoch_slots(rec)).map_err(|_| {
        WireError::BadRemoteEpoch(format!("{} messages overflows u32", rec.messages.len()))
    })?;
    let mut b = Vec::with_capacity(1 + CANONICAL_ID_LEN + 8 + 8 + 4 + 8 + 8 + 1 + body.len());
    b.push(KIND_REMOTE_ORIGIN_RECORD);
    b.extend_from_slice(rec.canonical_id().as_slice());
    b.extend_from_slice(&rec.origin_chain_id.to_le_bytes());
    b.extend_from_slice(&rec.anchor_number.to_le_bytes());
    b.extend_from_slice(&slots.to_le_bytes());
    b.extend_from_slice(&rec.first_seq.to_le_bytes());
    b.extend_from_slice(&rec.last_seq().to_le_bytes());
    b.push(RT_REMOTE_EPOCH);
    b.extend_from_slice(&body);
    Ok(b)
}

/// Encode an egress-subscribe announcement (ingress): `[kind:u8 = 2]`. A
/// session that sends this is a canonical-stream consumer. The service
/// includes it in the per-record and per-boundary egress fan-out.
/// Publisher-only sessions (sequencers) never send it. They stop
/// receiving the canonical broadcast, which they were already dropping
/// on the client side.
#[must_use]
pub fn encode_subscribe() -> Vec<u8> {
    vec![KIND_SUBSCRIBE]
}

/// Encode a batch of already-encoded single-record ingress frames.
///
/// # Errors
///
/// Returns [`WireError::BatchTooLarge`] if `entries.len()` does not fit
/// in a `u16`, or [`WireError::EntryTooLarge`] if any entry's length
/// does not fit in a `u32`. The caller chunks batches far below either
/// bound (one Aeron MTU holds a few dozen refs at most).
pub fn encode_ingress_batch(entries: &[Vec<u8>]) -> Result<Vec<u8>, WireError> {
    let count = u16::try_from(entries.len()).map_err(|_| WireError::BatchTooLarge {
        entries: entries.len(),
    })?;
    let payload: usize = entries.iter().map(|e| 4 + e.len()).sum();
    let mut b = Vec::with_capacity(1 + 2 + payload);
    b.push(KIND_BATCH);
    b.extend_from_slice(&count.to_le_bytes());
    for e in entries {
        let len = u32::try_from(e.len()).map_err(|_| WireError::EntryTooLarge { len: e.len() })?;
        b.extend_from_slice(&len.to_le_bytes());
        b.extend_from_slice(e);
    }
    Ok(b)
}

/// Encode a replay request (ingress). The service re-offers retained frames
/// from `(from_index, from_block)` to the requesting session.
#[must_use]
pub fn encode_replay_request(from_index: u64, from_block: u64) -> Vec<u8> {
    encode_kind_2u64(KIND_REPLAY_REQUEST, from_index, from_block)
}

/// Encode a void request (ingress): this voter cannot get the envelope of the
/// entry at `void.index` from the live stream or from any archive. See
/// [`KIND_VOID_REQUEST`] for the vote rule.
#[must_use]
pub fn encode_void_request(voter_id: u8, void: &VoidRecord) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 1 + 8 + CANONICAL_ID_LEN);
    b.push(KIND_VOID_REQUEST);
    b.push(voter_id);
    b.extend_from_slice(&void.index.to_le_bytes());
    b.extend_from_slice(void.tx_hash.as_slice());
    b
}

/// Encode the batcher's posted cursor (ingress): `posted_head` is the last
/// L2 block confirmed on L1. See [`KIND_POSTED_CURSOR`].
#[must_use]
pub fn encode_ingress_posted_cursor(posted_head: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(1 + 8);
    b.push(KIND_POSTED_CURSOR);
    b.extend_from_slice(&posted_head.to_le_bytes());
    b
}

/// Decode a replay request. Used by tests and a Rust service mock. The
/// real service-side decode lives in the Java `SealerClusteredService`.
#[cfg(test)]
pub(crate) fn decode_replay_request(buf: &[u8]) -> Result<(u64, u64), WireError> {
    if buf.first() != Some(&KIND_REPLAY_REQUEST) {
        return Err(WireError::BadEgressKind(*buf.first().unwrap_or(&255)));
    }
    Ok((rd_u64(buf, 1)?, rd_u64(buf, 9)?))
}

// ── ingress-frame splitters (the service's view of a record) ────────────────

/// The `(canonical_id, relayed_payload)` pair that a cluster service
/// extracts from an ingress message. `relayed_payload` is everything from
/// `canonical_id` onward: what the Java service forwards to egress. The
/// guard header (`sender` and `nonce`) before it is consumed by the
/// service and never relayed. Used by the in-Rust service mock in tests.
///
/// # Errors
///
/// Returns an error if `buf` is too short to hold the canonical id.
///
/// # Panics
///
/// Never: the slice handed to `try_into` is exactly `CANONICAL_ID_LEN`
/// bytes, checked just above.
pub fn split_ingress(buf: &[u8]) -> Result<([u8; 32], &[u8]), WireError> {
    let payload = buf
        .get(INGRESS_CANONICAL_ID_OFFSET..)
        .ok_or_else(|| too_short(buf, INGRESS_CANONICAL_ID_OFFSET, 0))?;
    let cid: [u8; 32] = rd_slice(payload, 0, CANONICAL_ID_LEN)?
        .try_into()
        .expect("rd_slice(.., CANONICAL_ID_LEN) returns a CANONICAL_ID_LEN-byte slice on Ok");
    Ok((cid, payload))
}

/// The `(sender, nonce)` guard header of an ingress record frame. The
/// Java service feeds this to the per-sender contiguity guard. Used by
/// tests and the in-Rust service mock.
///
/// # Errors
///
/// Returns an error if `buf` is too short to hold the guard header.
pub fn ingress_sender_nonce(buf: &[u8]) -> Result<(Address, u64), WireError> {
    let sender = rd_slice(buf, INGRESS_SENDER_OFFSET, SENDER_LEN)?;
    Ok((
        Address::from_slice(sender),
        rd_u64(buf, INGRESS_NONCE_OFFSET)?,
    ))
}

/// The inclusion deadline in the guard header of a kind-0 ingress frame.
///
/// # Errors
///
/// Returns [`WireError::TooShort`] if `buf` ends before the field.
pub fn ingress_deadline(buf: &[u8]) -> Result<u64, WireError> {
    rd_u64(buf, INGRESS_DEADLINE_OFFSET)
}

/// The tip in the guard header of a kind-0 ingress frame.
///
/// # Errors
///
/// Returns [`WireError::TooShort`] if `buf` ends before the field.
pub fn ingress_tip(buf: &[u8]) -> Result<u128, WireError> {
    rd_u128(buf, INGRESS_TIP_OFFSET)
}
