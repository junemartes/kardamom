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
//!
//! Codecs are split by direction — the ingress encode/decode lives in
//! `wire/ingress.rs`, the egress side in `wire/egress.rs` — while this module
//! holds the constants, offsets and [`WireError`] both directions share, and
//! re-exports everything so `wire::X` paths are unchanged.

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
/// Ingress kind: an ORIGIN-ADVANCING record
/// `[kind:u8 = 4][canonical_id:32][l1_origin:u64][slot_count:u32][record_type:u8][fields…]`.
///
/// Deduped by `canonical_id` like a normal record, but the service closes the
/// current block FIRST (so the record's contents lead a new block) and adopts
/// `l1_origin` for subsequent boundaries. Deliberately a separate KIND rather
/// than a record_type the service would have to parse: the sealer stays
/// schema-agnostic (it never learns what an epoch or a deposit is), the
/// hot-path TxRef framing is untouched, and the origin reaches the Raft state
/// machine as ORDERED DATA rather than by the sealer reading L1, which would
/// make replicas non-deterministic. Carries NO guard header — deposits are not
/// nonce-gated, so there is nothing to contiguity-check. Kind 4 because
/// [`KIND_BATCH`] holds 3. Matches Java `KIND_ORIGIN_RECORD`. See
/// `docs/agents/l1-origin-deposit-derivation-spec.md`.
pub const KIND_ORIGIN_RECORD: u8 = 4;
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
/// An rkyv-encoded [`EpochRecord`]: the L1 origin's deposits, in log order.
pub const RT_EPOCH: u8 = 2;

/// How many canonical slots an epoch occupies: **one for the epoch marker
/// itself, plus one per deposit**.
///
/// Every other record maps 1:1 onto a slot, and three separate mechanisms lean
/// on that: the sealer's cumulative `canonicalCount` (the block-boundary
/// alignment key in `BlockBoundaryStart::end_tx_idx`), the egress reader's
/// dense `next_index` cursor (a hole there is read as a gap and triggers replay
/// catch-up), and the executor's per-tx `tx_idx` (which keys receipts and the
/// BAL, so two txs must never share one).
///
/// An epoch is the exception — one record carrying N deposits — so it claims a
/// CONTIGUOUS RANGE instead: slot 0 is the marker (it advances the origin and
/// applies no tx), slots `1..=N` are the deposits. Counting the marker even
/// when `N == 0` is what keeps the range non-empty, so an empty epoch still
/// owns a distinct index rather than colliding with the record after it.
///
/// The count travels on the frame because the Java sealer never parses the
/// payload; every consumer that DOES parse it re-derives this value and
/// fail-stops on a mismatch.
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

/// Encode the shared `[kind:u8][a:u64 LE][b:u64 LE]` control frame — the
/// replay request ([`KIND_REPLAY_REQUEST`]), replay-unavailable
/// ([`EGRESS_KIND_REPLAY_UNAVAILABLE`]) and replay-done
/// ([`EGRESS_KIND_REPLAY_DONE`]) messages are byte-identical apart from the
/// kind byte.
fn encode_kind_2u64(kind: u8, a: u64, b: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + 8 + 8);
    buf.push(kind);
    buf.extend_from_slice(&a.to_le_bytes());
    buf.extend_from_slice(&b.to_le_bytes());
    buf
}

// Exact-width LE reads come from `kardamom_cluster_client::bytes` (shared with
// the SBE session codec); a `None` maps to this codec's own
// [`WireError::TooShort`] with this codec's offsets.

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
