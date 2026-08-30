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
//!   block boundary:  [kind:u8 = 2][block_number:u64][end_tx_idx:u64][l2_timestamp:u64]
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

pub use egress::{
    EgressItem, decode_egress, encode_contiguity_reject, encode_egress_boundary,
    encode_egress_record, encode_replay_done, encode_replay_unavailable,
};
pub use ingress::{
    decode_ingress_batch, decode_replay_request, encode_ingress_batch, encode_ingress_depositref,
    encode_ingress_epoch, encode_ingress_txref, encode_replay_request, encode_subscribe,
    ingress_sender_nonce, split_ingress,
};

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
/// format all stay unchanged. Batching only amortizes ingress transport
/// cost: each ~75-byte ref used to pay for a full offer round trip.
/// Matches Java `KIND_BATCH`.
pub const KIND_BATCH: u8 = 3;
/// Ingress kind: an origin-advancing record
/// `[kind:u8 = 4][canonical_id:32][l1_origin:u64][slot_count:u32][record_type:u8][fields…]`.
///
/// The service dedupes it by `canonical_id`, like a normal record. But it
/// closes the current block first, so the record's contents lead a new
/// block, and it adopts `l1_origin` for later boundaries. This is
/// deliberately a separate kind, not a record_type the service would have
/// to parse. That way the sealer stays schema-agnostic (it never learns
/// what an epoch or a deposit is), the hot-path TxRef framing stays
/// untouched, and the origin reaches the Raft state machine as ordered
/// data. If the sealer read L1 directly instead, replicas would become
/// non-deterministic. This record carries no guard header, because
/// deposits are not nonce-gated, so there is nothing to check for
/// contiguity. Kind 4, because [`KIND_BATCH`] holds 3. Matches Java
/// `KIND_ORIGIN_RECORD`. See
/// `docs/agents/l1-origin-deposit-derivation-spec.md`.
pub const KIND_ORIGIN_RECORD: u8 = 4;
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

/// Record discriminant inside the relayed payload.
pub const RT_TXREF: u8 = 0;
pub const RT_DEPOSITREF: u8 = 1;
/// An rkyv-encoded [`EpochRecord`]: the L1 origin's deposits, in log order.
pub const RT_EPOCH: u8 = 2;

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
pub fn epoch_slots(epoch: &EpochRecord) -> u64 {
    1 + epoch.deposits.len() as u64
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
fn too_short(b: &[u8], at: usize, need: usize) -> WireError {
    WireError::TooShort {
        at,
        need,
        have: b.len().saturating_sub(at),
    }
}
