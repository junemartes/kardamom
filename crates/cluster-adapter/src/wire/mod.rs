//! Application envelope shared with the Java clustered service.
//!
//! Two directions, both little-endian. This matches the cluster SBE schema
//! and the Java framing in `SealerClusteredService`:
//!
//! Ingress (Rust sequencer to cluster): one app message per record:
//! ```text
//!   [kind:u8 = 0][sender:20][nonce:u64 LE][canonical_id:32][record_type:u8][fields…]
//!     TxRef       fields = [shard_id:u8][tx_data_position.term_id:i32][.term_offset:i32][tx_data_session_id:i32]
//!     DepositRef  fields = [deposit_position.term_id:i32][.term_offset:i32]
//! ```
//! The Java service parses `sender` and `nonce` for the per-sender
//! contiguity guard: a known sender's ref with a nonce other than the
//! expected next one is rejected with [`EGRESS_KIND_CONTIGUITY_REJECT`],
//! instead of silently sealing a canonical nonce gap. It also parses
//! `canonical_id` (at its fixed offset) for dedup, then relays everything
//! from `canonical_id` onward verbatim. It never inspects `record_type` or
//! `fields`. The guard header sits before the canonical id, so the relayed
//! payload stays `[canonical_id:32][record_type:u8][fields…]` and
//! executors never see it. An all-zero sender is exempt from the guard
//! check (deposits carry no sender nonce).
//!
//! Egress (cluster to Rust executor):
//! ```text
//!   relayed record:  [kind:u8 = 1][index:u64][payload_len:u32][relayed payload…]
//!   block boundary:  [kind:u8 = 2][block_number:u64][end_tx_idx:u64][l2_timestamp:u64][l1_origin:u64]
//!   remote reject:   [kind:u8 = 6][origin_chain_id:u64][first_seq:u64][expected_next_seq:u64][reason:u8]
//! ```
//! `index` is the 0-based canonical record index that the leader's
//! replicated state machine assigns. The executor maps it to
//! `BPosition::from_index`.
//!
//! Codecs are split by direction. The ingress encode and decode live in
//! `wire/ingress.rs`, and the egress side lives in `wire/egress.rs`. This
//! module holds the constants, offsets, and [`WireError`] that both
//! directions share, and re-exports everything, so `wire::X` paths stay
//! unchanged.

use kardamom_cluster_client::bytes;
use kardamom_types::epoch::EpochRecord;
use thiserror::Error;

mod egress;
mod ingress;
#[cfg(test)]
mod tests;

#[cfg(any(test, feature = "testing"))]
pub use egress::encode_contiguity_reject;
pub use egress::{
    EgressItem, encode_egress_boundary, encode_egress_record, encode_remote_origin_reject,
    encode_replay_done, encode_replay_unavailable,
};
#[cfg(any(test, feature = "testing"))]
pub use ingress::encode_ingress_depositref;
pub use ingress::{
    encode_ingress_batch, encode_ingress_epoch, encode_ingress_remote_epoch, encode_ingress_txref,
    encode_replay_request, encode_subscribe, ingress_sender_nonce, split_ingress,
};

/// A `TxRef` fixture for wire and publish tests: distinct-enough bytes to
/// prove `tag` round-trips, not a value with consensus meaning. Shared by
/// every test that needs *a* `TxRef` rather than a specific one.
#[cfg(any(test, feature = "testing"))]
#[must_use]
pub fn txref(tag: u8) -> kardamom_types::TxRef {
    kardamom_types::TxRef::new(
        alloy_primitives::B256::repeat_byte(tag),
        1,
        kardamom_types::BPosition {
            term_id: 0,
            term_offset: i32::from(tag),
        },
        0,
    )
}

/// Ingress app-message kind (the leading tag byte).
pub const KIND_INGRESS_RECORD: u8 = 0;
/// Ingress kind: egress-subscribe announcement `[kind:u8 = 2]`. The
/// sending session wants the canonical egress broadcast (relayed records
/// and boundaries). Matches Java `KIND_SUBSCRIBE`.
pub const KIND_SUBSCRIBE: u8 = 2;
/// Ingress kind: a batch of ingress records
/// `[kind:u8 = 3][count:u16 LE][per entry: len:u32 LE + entry bytes]`. Each
/// entry is a complete single-record ingress frame
/// (`[kind:u8 = 0][sender:20][nonce:u64][canonical_id:32][payload…]`). One
/// cluster offer carries the whole batch. The service unpacks it and
/// processes each entry exactly like an individually offered record, so
/// consensus determinism, dedup, the contiguity guard, and the egress
/// format all stay unchanged. Batching amortizes the per-offer round
/// trip: each entry is about 75 bytes. Matches Java `KIND_BATCH`.
pub const KIND_BATCH: u8 = 3;
/// Ingress kind: an origin-advancing record
/// `[kind:u8 = 4][canonical_id:32][l1_origin:u64][slot_count:u32][record_type:u8][fields…]`.
///
/// The service dedupes it by `canonical_id`, like a normal record. But it
/// closes the current block first, so the record's contents lead a new
/// block, and it adopts `l1_origin` for later boundaries. This is
/// deliberately a separate kind, not a `record_type` the service would have
/// to parse. That way the sealer stays schema-agnostic (it never learns
/// what an epoch or a deposit is), the hot-path `TxRef` framing stays
/// untouched, and the origin reaches the Raft state machine as ordered
/// data. If the sealer read L1 directly instead, replicas would become
/// non-deterministic. This record carries no guard header, because
/// deposits are not nonce-gated, so there is nothing to check for
/// contiguity. Kind 4, because [`KIND_BATCH`] holds 3. Matches Java
/// `KIND_ORIGIN_RECORD`.
pub const KIND_ORIGIN_RECORD: u8 = 4;
/// Ingress kind: a REMOTE-ORIGIN-ADVANCING record
/// `[kind:u8 = 5][canonical_id:32][origin_chain_id:u64][anchor_number:u64][slot_count:u32][first_seq:u64][last_seq:u64][record_type:u8][fields…]`.
///
/// [`KIND_ORIGIN_RECORD`] for a peer Kardamom chain instead of L1. Same
/// posture — deduped on `canonical_id`, block closed before the record is
/// relayed, no guard header (cross-chain messages are not nonce-gated) — with
/// two differences the sealer must see WITHOUT parsing the payload:
///
/// * the position is a PAIR of u64s, not one. A remote origin is only
///   meaningful relative to the chain it came from, so the sealer tracks a
///   marker per `origin_chain_id`; a single number would silently merge two
///   peers' progress.
/// * the adopted position is NOT stamped into block boundaries. `l1_origin` is
///   part of the L2 block's identity; a peer chain's anchor is not, so a
///   remote origin advances per-pair bookkeeping only.
///
/// `first_seq` and `last_seq` are the record's seq range. The sealer keeps
/// a `next_seq` per origin and accepts a record only if `first_seq` equals
/// it (the lane contiguity guard, audit H2/H9). It also checks
/// `slot_count == 2 + last_seq - first_seq`, so a frame cannot claim more
/// slots than its body fills (audit H3). Every header field is bound by
/// `canonical_id`, which commits to the origin, the anchor, and the seq
/// range. A rejected frame answers the offering session with
/// [`EGRESS_KIND_REMOTE_ORIGIN_REJECT`].
///
/// A distinct KIND rather than a `record_type` under [`KIND_ORIGIN_RECORD`]
/// for the same reason kind 4 exists at all: the sealer branches on the frame
/// tag and never opens the payload, so it needs no notion of what a remote
/// epoch is. Kind 5 because 0–4 are taken. Matches Java
/// `KIND_REMOTE_ORIGIN_RECORD`.
pub const KIND_REMOTE_ORIGIN_RECORD: u8 = 5;
/// Ingress kind: a replay request `[kind:u8 = 1][from_index:u64][from_block:u64]`.
/// The service re-offers retained egress frames with `record.index >=
/// from_index` or `boundary.block_number >= from_block`, to the
/// requesting session only. It does not dedupe or relay these frames, and
/// they carry no canonical id. Matches Java `KIND_REPLAY_REQUEST`.
pub const KIND_REPLAY_REQUEST: u8 = 1;
/// Egress kind: a relayed canonical record. Matches Java `EGRESS_KIND_RELAYED`.
pub const EGRESS_KIND_RELAYED: u8 = 1;
/// Egress kind: a generated block boundary. Matches Java `EGRESS_KIND_BOUNDARY`.
pub const EGRESS_KIND_BOUNDARY: u8 = 2;
/// Egress kind: replay refused. The requested range predates the
/// service's bounded in-memory retention:
/// `[kind:u8 = 3][oldest_index:u64][oldest_block:u64]`. The consumer
/// cannot recover the gap, and must fail-stop for a full resync. Matches
/// Java `EGRESS_KIND_REPLAY_UNAVAILABLE`.
pub const EGRESS_KIND_REPLAY_UNAVAILABLE: u8 = 3;
/// Egress kind: replay complete. The service has re-offered all retained
/// frames at or after the requested cursor:
/// `[kind:u8 = 4][up_to_index:u64][up_to_block:u64]`. The range is
/// exclusive: it points to the next live record index or boundary block
/// at completion time. The consumer exits catch-up ordering mode. Matches
/// Java `EGRESS_KIND_REPLAY_DONE`.
pub const EGRESS_KIND_REPLAY_DONE: u8 = 4;
/// Egress kind: contiguity reject. A known sender's ingress record
/// carried a nonce other than the expected next one, so sealing it would
/// commit a canonical nonce gap:
/// `[kind:u8 = 5][sender:20][nonce:u64][expected:u64]`. The service sends
/// this only to the offering session. The sequencer rewinds its
/// unconfirmed ledger to `expected` and republishes, turning a silent gap
/// into a recoverable signal. Matches Java `EGRESS_KIND_CONTIGUITY_REJECT`.
pub const EGRESS_KIND_CONTIGUITY_REJECT: u8 = 5;
/// Egress kind: remote-origin reject. The sealer refused a
/// [`KIND_REMOTE_ORIGIN_RECORD`] frame:
/// `[kind:u8 = 6][origin_chain_id:u64][first_seq:u64][expected_next_seq:u64][reason:u8]`.
/// The service sends this only to the offering session, like the
/// contiguity reject. The sequencer logs it and counts it. It does not
/// republish: the record is the watcher's, and the watcher reconciles its
/// cursor with the destination's `Inbox.nextSeq` at startup. `reason` is
/// one of the `REMOTE_ORIGIN_REJECT_*` codes. Matches Java
/// `EGRESS_KIND_REMOTE_ORIGIN_REJECT`.
pub const EGRESS_KIND_REMOTE_ORIGIN_REJECT: u8 = 6;

/// `first_seq` is not the sealer's `next_seq` for the origin.
pub const REMOTE_ORIGIN_REJECT_SEQ_MISMATCH: u8 = 1;
/// `anchor_number` does not advance the origin's adopted anchor.
pub const REMOTE_ORIGIN_REJECT_ANCHOR_REGRESSED: u8 = 2;
/// `slot_count != 2 + last_seq - first_seq`.
pub const REMOTE_ORIGIN_REJECT_SLOT_COUNT_MISMATCH: u8 = 3;
/// `origin_chain_id` is not in the sealer's remote-origin allowlist.
pub const REMOTE_ORIGIN_REJECT_UNKNOWN_ORIGIN: u8 = 4;
/// `last_seq < first_seq`, or the range overflows.
pub const REMOTE_ORIGIN_REJECT_BAD_RANGE: u8 = 5;

/// Human-readable label for a remote-origin reject reason. Used as a
/// metric label and in log lines. Unknown codes map to `"unknown"`.
pub fn remote_origin_reject_reason(code: u8) -> &'static str {
    match code {
        REMOTE_ORIGIN_REJECT_SEQ_MISMATCH => "seq_mismatch",
        REMOTE_ORIGIN_REJECT_ANCHOR_REGRESSED => "anchor_regressed",
        REMOTE_ORIGIN_REJECT_SLOT_COUNT_MISMATCH => "slot_count_mismatch",
        REMOTE_ORIGIN_REJECT_UNKNOWN_ORIGIN => "unknown_origin",
        REMOTE_ORIGIN_REJECT_BAD_RANGE => "bad_range",
        _ => "unknown",
    }
}

/// Record discriminant inside the relayed payload.
pub const RT_TXREF: u8 = 0;
pub const RT_DEPOSITREF: u8 = 1;
/// An rkyv-encoded [`EpochRecord`]: the L1 origin's deposits, in log order.
pub const RT_EPOCH: u8 = 2;
/// An rkyv-encoded [`RemoteEpochRecord`]: one peer Kardamom chain's outbox
/// batch, in seq order. Origin-advancing like `RT_EPOCH`, but tracked per
/// peer in the sealer and never stamped into boundaries.
pub const RT_REMOTE_EPOCH: u8 = 3;

/// How many canonical slots an epoch occupies. This is one slot for the
/// epoch marker itself, plus one slot per deposit.
///
/// Every other record maps 1:1 onto a slot. Three separate mechanisms rely
/// on that: the sealer's cumulative `canonicalCount` (the block-boundary
/// alignment key in `BlockBoundaryStart::end_tx_idx`), the egress reader's
/// dense `next_index` cursor (a hole there reads as a gap and triggers
/// replay catch-up), and the executor's per-tx `tx_idx` (which keys
/// receipts and the BAL, so two txs must never share one).
///
/// An epoch is the exception: one record carries N deposits, so it claims
/// a contiguous range instead. Slot 0 is the marker (it advances the
/// origin and applies no tx). Slots `1..=N` are the deposits. The marker
/// counts even when `N == 0`. This keeps the range non-empty, so an empty
/// epoch still owns a distinct index instead of colliding with the record
/// after it.
///
/// The count travels on the frame because the Java sealer never parses
/// the payload. Every consumer that does parse it re-derives this value
/// and fail-stops on a mismatch.
#[must_use]
pub fn epoch_slots(epoch: &EpochRecord) -> u64 {
    1 + epoch.deposits.len() as u64
}

/// How many canonical slots a remote epoch occupies: **one for the marker,
/// plus one per message** — the [`epoch_slots`] rule applied to the
/// cross-chain batch. `derive_remote_epoch` rejects empty batches, so the
/// range is always ≥ 2; the `1 +` stays for shape-uniformity with epochs and
/// so a decoding bug surfaces as a slot mismatch, not an off-by-one.
#[must_use]
pub fn remote_epoch_slots(rec: &kardamom_types::xchain::RemoteEpochRecord) -> u64 {
    1 + rec.messages.len() as u64
}

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
    #[error("bad epoch record: {0}")]
    BadEpoch(String),
    #[error("bad remote epoch record: {0}")]
    BadRemoteEpoch(String),
    #[error("batch entry count {entries} overflows the u16 wire count")]
    BatchTooLarge { entries: usize },
    #[error("length {len} overflows the u32 wire length prefix")]
    EntryTooLarge { len: usize },
    #[error("declared length {declared} does not fit in this platform's usize")]
    LenOverflow { declared: u32 },
}

// ── shared helpers (both directions) ────────────────────────────────────────

/// Encode the shared `[kind:u8][a:u64 LE][b:u64 LE]` control frame. The
/// replay request ([`KIND_REPLAY_REQUEST`]), replay-unavailable
/// ([`EGRESS_KIND_REPLAY_UNAVAILABLE`]), and replay-done
/// ([`EGRESS_KIND_REPLAY_DONE`]) messages are byte-identical, apart from
/// the kind byte.
fn encode_kind_2u64(kind: u8, a: u64, b: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 8);
    buf.push(kind);
    buf.extend_from_slice(&a.to_le_bytes());
    buf.extend_from_slice(&b.to_le_bytes());
    buf
}

// Exact-width LE reads come from `kardamom_cluster_client::bytes`
// (shared with the SBE session codec). A `None` maps to this codec's own
// [`WireError::TooShort`], with this codec's offsets.

fn rd_u32(b: &[u8], at: usize) -> Result<u32, WireError> {
    bytes::u32_le(b, at).ok_or_else(|| too_short(b, at, 4))
}
fn rd_i32(b: &[u8], at: usize) -> Result<i32, WireError> {
    bytes::i32_le(b, at).ok_or_else(|| too_short(b, at, 4))
}
fn rd_u64(b: &[u8], at: usize) -> Result<u64, WireError> {
    bytes::u64_le(b, at).ok_or_else(|| too_short(b, at, 8))
}

/// Read a `u32` length prefix at `at`, converted to `usize`. A real error
/// instead of a `usize::MAX` sentinel: this can only fail on a target
/// where `usize` is narrower than `u32`.
pub(super) fn rd_len(b: &[u8], at: usize) -> Result<usize, WireError> {
    let declared = rd_u32(b, at)?;
    usize::try_from(declared).map_err(|_| WireError::LenOverflow { declared })
}

pub(super) fn too_short(b: &[u8], at: usize, need: usize) -> WireError {
    WireError::TooShort {
        at,
        need,
        have: b.len().saturating_sub(at),
    }
}

/// Read a `len`-byte slice at `at`, or [`WireError::TooShort`] with this
/// codec's offsets. Shared by every fixed-width field read in the ingress
/// and egress decoders (sender addresses, canonical ids, relayed
/// payloads), so each site is one call instead of a hand-rolled
/// `TooShort { .. }` literal.
pub(super) fn rd_slice(b: &[u8], at: usize, len: usize) -> Result<&[u8], WireError> {
    at.checked_add(len)
        .and_then(|end| b.get(at..end))
        .ok_or_else(|| too_short(b, at, len))
}
