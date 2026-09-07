//! Cross-chain (Kardamom ↔ Kardamom) **remote epoch** derivation — the single
//! definition of "which outbox messages does origin chain O contribute next,
//! and in what order".
//!
//! Like [`crate::epoch`], this lives in `kardamom-types` on purpose: under
//! `docs/specs/interop-outbox-messaging-spec.md` the same rule is run by the
//! **producer** (the interop watcher, deriving from a peer validator's feed or
//! a sovereign local validator) and the **verifier** (the destination
//! validator, re-deriving and rejecting a chain that disagrees). One copy, or
//! the verification verifies nothing.
//!
//! Contents:
//!
//!   * [`remote_source_hash`] — the cross-chain tx's canonical id, domain 2 of
//!     the [`crate::epoch::source_hash`] scheme. **Position-based** (origin
//!     chain id + per-pair seq), NOT content-based: dedup must key on the
//!     message's slot so an equivocating origin (two payloads at one seq)
//!     collapses to first-seen and is then caught by content verification as a
//!     fault — content-keyed ids would let both execute.
//!   * [`alias_remote_address`] — hash-based aliasing. The L1 constant-offset
//!     trick ([`crate::epoch::alias_l1_address`]) cannot serve here: two
//!     origins' contracts at the same address would collide on the
//!     destination.
//!   * [`OutboxMessage`] — the decoded shape of one `MessageSent` outbox
//!     event, transport-agnostic (peer feed, sovereign validator, test
//!     fixture), which is what lets producer and verifier share
//!     [`derive_remote_epoch`].
//!   * [`XChainMessage`] / [`Callback`] — the wire shapes on the canonical
//!     stream.
//!   * [`RemoteEpochRecord`] — one origin's contiguous message batch as it
//!     travels on the canonical stream.
//!   * [`msg_leaf`] — the commitment the Outbox predeploy stores; must stay
//!     byte-identical to `Outbox.hashMessage` (see `contracts/src/Outbox.sol`).

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, address, keccak256};
use alloy_rlp::{Encodable, Header};
use bytes::Bytes;
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize};

use crate::wire;

/// Domain of [`remote_source_hash`] in the source-hash scheme shared with
/// deposits (`crate::epoch`: 0 = user deposit, 1 = reserved system tx).
pub const XCHAIN_SOURCE_DOMAIN: u64 = 2;

/// Tag string hashed into [`alias_remote_address`]. Versioned: changing the
/// alias scheme is a chain-splitting change and must bump this.
pub const XCHAIN_ALIAS_TAG: &str = "KARDAMOM_XCHAIN_ALIAS_V0";

/// Canonical predeploy address of the `Outbox` (cross-chain send entry point)
/// on every Kardamom chain. Seeded into genesis like
/// [`crate::withdrawals::MESSAGE_PASSER`].
pub const OUTBOX: Address = address!("0x42000000000000000000000000000000000000E0");

/// Canonical predeploy address of the `Inbox` (cross-chain delivery entry
/// point). The derived 0x7D tx always calls this address, with the aliased
/// origin OUTBOX as the EVM sender — an address no key exists for, which is
/// what makes delivery injection unforgeable by user txs.
pub const INBOX: Address = address!("0x42000000000000000000000000000000000000E1");

/// Largest `gasLimit` the origin `Outbox` accepts for one message. This
/// mirrors `Outbox.MAX_MESSAGE_GAS` (`contracts/src/L2/Outbox.sol`).
/// [`derive_remote_epoch`] rejects a larger value, so an honest origin can
/// never produce a message that the destination cannot budget.
pub const MAX_MESSAGE_GAS: u64 = 10_000_000;

/// Largest `data` length the origin `Outbox` accepts for one message. This
/// mirrors `Outbox.MAX_DATA_BYTES` (`contracts/src/L2/Outbox.sol`).
pub const MAX_DATA_BYTES: usize = 65_536;

/// The EVM sender of a derived cross-chain tx from `origin_chain_id`: the
/// origin's Outbox predeploy, aliased into the destination's address space.
/// `Inbox.deliver` authenticates against exactly this address (the OP
/// aliased-counterpart pattern), so only the derivation pipeline can deliver.
pub fn xchain_tx_sender(origin_chain_id: u64) -> Address {
    alias_remote_address(origin_chain_id, OUTBOX)
}

/// First `abi.encode` word of [`msg_leaf`]. Must equal
/// `Outbox.LEAF_DOMAIN_XCHAIN`. Domain-separates message leaves from every
/// other keccak commitment in the system (withdrawal leaves, tree nodes).
pub fn xchain_leaf_domain() -> B256 {
    keccak256("KARDAMOM_XCHAIN_MESSAGE_V0")
}

/// Canonical id of a cross-chain tx on the destination:
///
/// ```text
///   msg_id_hash        = keccak256(rlp([origin_chain_id, seq]))
///   remote_source_hash = keccak256(rlp([domain = 2u64, msg_id_hash]))
/// ```
///
/// Deliberately position-based — see the module docs. Also deliberately
/// independent of the trust mode's anchor identity: the same message resolves
/// to the same tx hash whether it arrived via an anchored feed or a sovereign
/// validator, so switching a pair's trust mode never re-identifies its txs.
pub fn remote_source_hash(origin_chain_id: u64, seq: u64) -> B256 {
    let inner = encode_list_two(&origin_chain_id, &seq);
    let msg_id_hash = keccak256(&inner);
    let outer = encode_list_two(&XCHAIN_SOURCE_DOMAIN, &msg_id_hash);
    keccak256(&outer)
}

fn encode_list_two<A: Encodable, B: Encodable>(a: &A, b: &B) -> Vec<u8> {
    let mut buf = Vec::new();
    let payload_len = a.length() + b.length();
    Header {
        list: true,
        payload_length: payload_len,
    }
    .encode(&mut buf);
    a.encode(&mut buf);
    b.encode(&mut buf);
    buf
}

/// Alias an origin-chain sender into the destination's address space:
/// `address(keccak256(TAG ‖ be64(origin_chain_id) ‖ sender)[12..])`.
///
/// Hash-based rather than offset-based so that senders from *different*
/// origins can never collide with each other (or with local contracts) —
/// the price is irreversibility, which is why [`XChainMessage`] carries the
/// un-aliased `origin_sender` on the wire and aliases only at the execution
/// edge.
pub fn alias_remote_address(origin_chain_id: u64, sender: Address) -> Address {
    let mut buf = Vec::with_capacity(XCHAIN_ALIAS_TAG.len() + 8 + 20);
    buf.extend_from_slice(XCHAIN_ALIAS_TAG.as_bytes());
    buf.extend_from_slice(&origin_chain_id.to_be_bytes());
    buf.extend_from_slice(sender.as_slice());
    let h = keccak256(&buf);
    Address::from_slice(&h.as_slice()[12..])
}

/// Response requested by a message's sender: enqueued through the
/// destination's own Outbox when delivery completes (success or failure),
/// addressed back to `target` on the origin. Fixed-size by design — the
/// response payload is generated (status + return-data hash + `context`),
/// never sender-supplied.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct Callback {
    /// Contract on the ORIGIN chain that receives the response.
    #[rkyv(with = wire::AddressBytes)]
    pub target: Address,
    /// Gas budget for the response delivery on the origin.
    pub gas_limit: u64,
    /// Opaque correlation value, echoed back verbatim.
    #[rkyv(with = wire::B256Bytes)]
    pub context: B256,
}

impl Callback {
    /// `keccak256(abi.encode(target, gasLimit, context))` — the `cbHash` word
    /// of [`msg_leaf`]. Must equal `Outbox.hashCallback`.
    pub fn commitment(&self) -> B256 {
        let mut buf = [0u8; 96];
        buf[12..32].copy_from_slice(self.target.as_slice());
        buf[56..64].copy_from_slice(&self.gas_limit.to_be_bytes());
        buf[64..96].copy_from_slice(self.context.as_slice());
        keccak256(buf)
    }
}

/// `cbHash` for a message without a callback.
pub fn no_callback_hash() -> B256 {
    B256::ZERO
}

/// One decoded `MessageSent` outbox event, as observed on the ORIGIN chain.
///
/// Pure data: whether it came from a peer validator's WS feed, a
/// B-operated sovereign validator, or a test fixture is not this type's
/// business — that transport-agnosticism is what lets the producer and the
/// verifier share [`derive_remote_epoch`], and what lets the e2e suite
/// simulate an external validator by scripting these.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxMessage {
    /// Origin block the send happened in. Not part of the message identity —
    /// reporting/anchoring metadata, the analogue of
    /// [`crate::epoch::DepositLog::block_number`].
    pub origin_block_number: u64,
    /// Hash of that origin block.
    pub origin_block_hash: B256,
    /// Destination chain the sender addressed. Checked against the deriving
    /// chain's own id — a foreign-destination message in a batch is a
    /// producer bug or a malicious feed, never silently dropped.
    pub dest_chain_id: u64,
    /// Dense per-(origin, dest) sequence number from the Outbox predeploy.
    pub seq: u64,
    /// Origin-chain sender (un-aliased).
    pub sender: Address,
    /// Destination-chain call target.
    pub target: Address,
    /// Value burned on the origin, to be minted on the destination. v1
    /// messaging keeps this 0; the field is carried (and committed in the
    /// leaf) so the wire does not change when value transfer ships.
    pub value: u128,
    /// Gas budget for the inner call on the destination.
    pub gas_limit: u64,
    /// Inner-call calldata.
    pub data: AlloyBytes,
    /// Requested response, if any.
    pub callback: Option<Callback>,
}

/// One cross-chain message as it travels on the canonical stream and into
/// execution.
///
/// Carries the UN-aliased `origin_sender` — unlike [`crate::Deposit`], whose
/// `from` is pre-aliased — because verification must be able to recompute
/// [`msg_leaf`] from the wire record alone, and the leaf commits to the
/// origin-side sender. Execution aliases at the edge via
/// [`alias_remote_address`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct XChainMessage {
    /// [`remote_source_hash`] — canonical id, dedup key, and the receipt's
    /// `tx_hash` on the destination.
    #[rkyv(with = wire::B256Bytes)]
    pub source_hash: B256,
    /// Dense per-(origin, dest) sequence number.
    pub seq: u64,
    /// Origin-chain sender, un-aliased (see type docs).
    #[rkyv(with = wire::AddressBytes)]
    pub origin_sender: Address,
    /// Destination-chain call target.
    #[rkyv(with = wire::AddressBytes)]
    pub target: Address,
    /// Minted on the destination before the inner call (0 in v1).
    pub value: u128,
    /// Gas budget for the inner call.
    pub gas_limit: u64,
    /// Inner-call calldata.
    #[rkyv(with = wire::BytesVec)]
    pub input: Bytes,
    /// Requested response, if any.
    #[rkyv(with = Map<rkyv::with::Identity>)]
    pub callback: Option<Callback>,
}

impl XChainMessage {
    /// The sender the destination EVM observes.
    pub fn aliased_sender(&self, origin_chain_id: u64) -> Address {
        alias_remote_address(origin_chain_id, self.origin_sender)
    }

    /// Recompute this message's Outbox commitment. `origin_chain_id` and
    /// `dest_chain_id` come from the enclosing [`RemoteEpochRecord`] and the
    /// deriving chain respectively — they are not duplicated on the wire.
    pub fn leaf(&self, origin_chain_id: u64, dest_chain_id: u64) -> B256 {
        msg_leaf(
            origin_chain_id,
            dest_chain_id,
            self.seq,
            self.origin_sender,
            self.target,
            self.value,
            self.gas_limit,
            keccak256(&self.input),
            self.callback
                .as_ref()
                .map(Callback::commitment)
                .unwrap_or_else(no_callback_hash),
        )
    }
}

/// The commitment the Outbox predeploy stores per message:
/// `keccak256(abi.encode(LEAF_DOMAIN_XCHAIN, originChainId, destChainId, seq,
/// sender, target, value, gasLimit, keccak256(data), cbHash))` — ten static
/// 32-byte words. Must stay byte-identical to `Outbox.hashMessage`. Origin
/// AND destination chain ids inside the leaf make replay across pairs
/// impossible.
#[allow(clippy::too_many_arguments)]
pub fn msg_leaf(
    origin_chain_id: u64,
    dest_chain_id: u64,
    seq: u64,
    sender: Address,
    target: Address,
    value: u128,
    gas_limit: u64,
    data_hash: B256,
    cb_hash: B256,
) -> B256 {
    let mut buf = [0u8; 320];
    buf[0..32].copy_from_slice(xchain_leaf_domain().as_slice());
    buf[56..64].copy_from_slice(&origin_chain_id.to_be_bytes());
    buf[88..96].copy_from_slice(&dest_chain_id.to_be_bytes());
    buf[120..128].copy_from_slice(&seq.to_be_bytes());
    buf[140..160].copy_from_slice(sender.as_slice());
    buf[172..192].copy_from_slice(target.as_slice());
    buf[208..224].copy_from_slice(&value.to_be_bytes());
    buf[248..256].copy_from_slice(&gas_limit.to_be_bytes());
    buf[256..288].copy_from_slice(data_hash.as_slice());
    buf[288..320].copy_from_slice(cb_hash.as_slice());
    keccak256(buf)
}

/// Canonical signature of `Inbox.deliver` — the call every derived 0x7D tx
/// makes. The callback parameter is the static tuple `(address,uint64,bytes32)`
/// (`XChain.Callback`), which Solidity resolves into the signature exactly as
/// written here.
pub const INBOX_DELIVER_SIGNATURE: &str =
    "deliver(uint64,uint64,address,address,uint256,uint64,bytes,(address,uint64,bytes32))";

/// 4-byte function selector of [`INBOX_DELIVER_SIGNATURE`].
pub fn inbox_deliver_selector() -> [u8; 4] {
    let h = keccak256(INBOX_DELIVER_SIGNATURE.as_bytes());
    [h[0], h[1], h[2], h[3]]
}

/// ABI-encode `Inbox.deliver(originChainId, seq, originSender, target, value,
/// gasLimit, data, cb)` for one message — the calldata of the derived 0x7D tx.
///
/// Hand-rolled: the execution edge is `no_std` and must not grow an ABI
/// codegen dependency for one fixed call; byte-parity with `alloy-sol-types`
/// is pinned in tests. Layout: a 10-word head — the six static params, the
/// offset word for `data` (0x140, the tail begins right after the head), and
/// the static callback tuple inlined as three words — then `data`'s length
/// word and its right-padded bytes. `callback: None` encodes as the zeroed
/// tuple, which is exactly what `XChain.isNone` tests for.
pub fn deliver_calldata(origin_chain_id: u64, msg: &XChainMessage) -> Vec<u8> {
    const HEAD_WORDS: usize = 10;
    let data = msg.input.as_ref();
    let padded_len = data.len().div_ceil(32) * 32;
    let mut out = Vec::with_capacity(4 + (HEAD_WORDS + 1) * 32 + padded_len);
    out.extend_from_slice(&inbox_deliver_selector());
    push_word_u64(&mut out, origin_chain_id);
    push_word_u64(&mut out, msg.seq);
    push_word_address(&mut out, msg.origin_sender);
    push_word_address(&mut out, msg.target);
    push_word_u128(&mut out, msg.value);
    push_word_u64(&mut out, msg.gas_limit);
    push_word_u64(&mut out, (HEAD_WORDS * 32) as u64);
    let cb = msg.callback.unwrap_or_default();
    push_word_address(&mut out, cb.target);
    push_word_u64(&mut out, cb.gas_limit);
    out.extend_from_slice(cb.context.as_slice());
    push_word_u64(&mut out, data.len() as u64);
    out.extend_from_slice(data);
    out.resize(out.len() + (padded_len - data.len()), 0);
    out
}

fn push_word_u64(out: &mut Vec<u8>, v: u64) {
    let mut w = [0u8; 32];
    w[24..].copy_from_slice(&v.to_be_bytes());
    out.extend_from_slice(&w);
}

fn push_word_u128(out: &mut Vec<u8>, v: u128) {
    let mut w = [0u8; 32];
    w[16..].copy_from_slice(&v.to_be_bytes());
    out.extend_from_slice(&w);
}

fn push_word_address(out: &mut Vec<u8>, a: Address) {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(a.as_slice());
    out.extend_from_slice(&w);
}

/// One origin chain's contiguous batch of messages, as it travels on the
/// canonical stream.
///
/// Atomic by construction, like [`crate::epoch::EpochRecord`]: the origin
/// marker and its messages are one record, messages by VALUE. Unlike L1
/// epochs, an EMPTY record is invalid — remote origins advance only when
/// messages exist (the no-skip rule is enforced on the dense per-pair `seq`,
/// not on origin blocks), so an empty batch has nothing to say.
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct RemoteEpochRecord {
    /// The origin chain.
    pub origin_chain_id: u64,
    /// Origin block number the batch's LAST message was observed in — the
    /// position this record advances the pair's origin marker to.
    pub anchor_number: u64,
    /// Trust-mode anchor reference for the batch (anchored mode: the L1
    /// output identity; sovereign mode: the origin block hash at
    /// `anchor_number`). Opaque here; the verifier interprets it per the
    /// pair's configured trust mode.
    #[rkyv(with = wire::B256Bytes)]
    pub anchor_hash: B256,
    /// Sequence number of `messages[0]`.
    pub first_seq: u64,
    /// The batch, in seq order, dense from `first_seq`. Non-empty by
    /// construction ([`derive_remote_epoch`] rejects empty input).
    pub messages: Vec<XChainMessage>,
}

impl RemoteEpochRecord {
    /// Sequence number of the last message in the batch.
    pub fn last_seq(&self) -> u64 {
        // Saturating on purpose. A wire-decoded record reaches this method
        // (through `canonical_id`) before any check runs, so a hostile
        // `first_seq` must not panic a debug build. [`derive_remote_epoch`]
        // and the validator reject a record whose seq range overflows.
        self.first_seq
            .saturating_add(self.messages.len() as u64)
            .saturating_sub(1)
    }

    /// Canonical id for cluster dedup: racing relayers that observe the same
    /// feed prefix derive byte-identical records, and first-seen dedup
    /// collapses them — the `EpochRecord::canonical_id` property, keyed by
    /// pair position rather than L1 hash.
    pub fn canonical_id(&self) -> B256 {
        let mut buf = Vec::with_capacity(8 + 32 + 16);
        buf.extend_from_slice(&self.origin_chain_id.to_be_bytes());
        buf.extend_from_slice(self.anchor_hash.as_slice());
        buf.extend_from_slice(&self.first_seq.to_be_bytes());
        buf.extend_from_slice(&self.last_seq().to_be_bytes());
        keccak256(&buf)
    }
}

/// THE derivation rule: a batch of observed outbox messages from
/// `origin_chain_id` → the record the destination chain must contain next.
///
/// `expected_first_seq` is the deriving side's cursor for the pair (one past
/// the last message already canonicalised). Messages may arrive in any order;
/// callers need not pre-sort. Every violation is an error rather than a
/// repair: for a VERIFIER these are chain faults, for a PRODUCER bugs or a
/// malicious feed — either way, fail stop, never skip or reorder.
pub fn derive_remote_epoch(
    self_chain_id: u64,
    origin_chain_id: u64,
    expected_first_seq: u64,
    msgs: &[OutboxMessage],
) -> Result<RemoteEpochRecord, XChainError> {
    if msgs.is_empty() {
        return Err(XChainError::Empty);
    }
    let mut ordered: Vec<&OutboxMessage> = msgs.iter().collect();
    ordered.sort_by_key(|m| m.seq);

    if let Some(dup) = ordered.windows(2).find(|w| w[0].seq == w[1].seq) {
        return Err(XChainError::DuplicateSeq { seq: dup[0].seq });
    }
    let first = ordered[0].seq;
    if first != expected_first_seq {
        return Err(if first < expected_first_seq {
            XChainError::SeqRegressed {
                expected: expected_first_seq,
                found: first,
            }
        } else {
            XChainError::SeqSkipped {
                expected: expected_first_seq,
                found: first,
            }
        });
    }
    for w in ordered.windows(2) {
        let Some(want) = w[0].seq.checked_add(1) else {
            return Err(XChainError::SeqOverflow { seq: w[0].seq });
        };
        if w[1].seq != want {
            return Err(XChainError::SeqSkipped {
                expected: want,
                found: w[1].seq,
            });
        }
    }
    // The record's seq range must fit in u64. The validator computes
    // `last_seq + 1` as its next cursor, so the last seq must leave room
    // for one more.
    if first.checked_add(ordered.len() as u64).is_none() {
        return Err(XChainError::SeqOverflow { seq: first });
    }
    for m in &ordered {
        if m.dest_chain_id != self_chain_id {
            return Err(XChainError::ForeignDestination {
                expected: self_chain_id,
                found: m.dest_chain_id,
            });
        }
        // Producer-side mirror of the `Outbox.sendMessage` checks. An
        // honest origin can never trip these. A feed that does is
        // malicious or corrupt, and the record must not reach the
        // canonical stream: the destination budgets delivery from these
        // bounds.
        if m.value != 0 {
            return Err(XChainError::ValueNotAllowed {
                seq: m.seq,
                value: m.value,
            });
        }
        if m.gas_limit > MAX_MESSAGE_GAS {
            return Err(XChainError::GasLimitAboveCap {
                seq: m.seq,
                gas_limit: m.gas_limit,
                cap: MAX_MESSAGE_GAS,
            });
        }
        if m.data.len() > MAX_DATA_BYTES {
            return Err(XChainError::DataAboveCap {
                seq: m.seq,
                len: m.data.len(),
                cap: MAX_DATA_BYTES,
            });
        }
    }

    let last = ordered.last().expect("non-empty");
    Ok(RemoteEpochRecord {
        origin_chain_id,
        anchor_number: last.origin_block_number,
        anchor_hash: last.origin_block_hash,
        first_seq: first,
        messages: ordered
            .into_iter()
            .map(|m| XChainMessage {
                source_hash: remote_source_hash(origin_chain_id, m.seq),
                seq: m.seq,
                origin_sender: m.sender,
                target: m.target,
                value: m.value,
                gas_limit: m.gas_limit,
                input: Bytes::copy_from_slice(m.data.as_ref()),
                callback: m.callback,
            })
            .collect(),
    })
}

/// Why a remote epoch could not be derived. All variants are fail-stop for a
/// verifier and bugs (or a malicious feed) for a producer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum XChainError {
    #[error("remote epochs advance only with messages; an empty batch is invalid")]
    Empty,
    #[error("two outbox messages share seq {seq} for one origin")]
    DuplicateSeq { seq: u64 },
    #[error("outbox seq skipped: expected {expected}, found {found}")]
    SeqSkipped { expected: u64, found: u64 },
    #[error("outbox seq regressed: expected {expected}, found {found}")]
    SeqRegressed { expected: u64, found: u64 },
    #[error("message for chain {found} in a batch derived by chain {expected}")]
    ForeignDestination { expected: u64, found: u64 },
    /// A seq at or near `u64::MAX`. The dense-lane arithmetic (`seq + 1`)
    /// would overflow.
    #[error("outbox seq {seq} overflows the dense-lane arithmetic")]
    SeqOverflow { seq: u64 },
    /// v1 messaging carries no value. The `Outbox` rejects a nonzero value,
    /// so this can only come from a malicious or corrupt feed.
    #[error("outbox message seq {seq} carries value {value}; v1 delivery is value-free")]
    ValueNotAllowed { seq: u64, value: u128 },
    /// `gas_limit` above [`MAX_MESSAGE_GAS`], which the `Outbox` rejects.
    #[error("outbox message seq {seq} gas limit {gas_limit} is above the cap {cap}")]
    GasLimitAboveCap { seq: u64, gas_limit: u64, cap: u64 },
    /// `data` longer than [`MAX_DATA_BYTES`], which the `Outbox` rejects.
    #[error("outbox message seq {seq} data length {len} is above the cap {cap}")]
    DataAboveCap { seq: u64, len: usize, cap: usize },
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, b256};

    fn msg(seq: u64, dest: u64) -> OutboxMessage {
        OutboxMessage {
            origin_block_number: seq.saturating_add(100),
            origin_block_hash: B256::repeat_byte(0x0B),
            dest_chain_id: dest,
            seq,
            sender: Address::repeat_byte(0xA1),
            target: Address::repeat_byte(0xB2),
            value: 0,
            gas_limit: 200_000,
            data: AlloyBytes::from(alloc::vec![0xCA, 0xFE]),
            callback: None,
        }
    }

    const SELF: u64 = 412_347;
    const ORIGIN: u64 = 412_346;

    #[test]
    fn source_hash_is_position_based_and_deterministic() {
        assert_eq!(remote_source_hash(ORIGIN, 7), remote_source_hash(ORIGIN, 7));
        assert_ne!(remote_source_hash(ORIGIN, 7), remote_source_hash(ORIGIN, 8));
        assert_ne!(
            remote_source_hash(ORIGIN, 7),
            remote_source_hash(ORIGIN + 1, 7)
        );
    }

    #[test]
    fn source_hash_domain_separates_from_deposits() {
        // A deposit's source_hash over structurally-similar inputs must never
        // collide with a remote message's — the domain word differs.
        let dep = crate::epoch::source_hash(B256::ZERO, 7);
        // remote_source_hash rlp-encodes (chain_id, seq), not (hash, index),
        // so equality could only happen by keccak collision; assert anyway as
        // the cheap canary.
        assert_ne!(dep, remote_source_hash(0, 7));
    }

    #[test]
    fn alias_is_per_origin_and_deterministic() {
        let s = Address::repeat_byte(0x11);
        assert_eq!(alias_remote_address(1, s), alias_remote_address(1, s));
        assert_ne!(alias_remote_address(1, s), alias_remote_address(2, s));
        assert_ne!(
            alias_remote_address(1, s),
            alias_remote_address(1, Address::repeat_byte(0x12))
        );
        // And never the identity — a remote sender must not impersonate the
        // local account at the same address.
        assert_ne!(alias_remote_address(1, s), s);
    }

    #[test]
    fn derivation_orders_by_seq_regardless_of_input_order() {
        let fwd = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(6, SELF)]).unwrap();
        let rev = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(6, SELF), msg(5, SELF)]).unwrap();
        assert_eq!(fwd, rev);
        assert_eq!(fwd.first_seq, 5);
        assert_eq!(fwd.last_seq(), 6);
        assert_eq!(fwd.messages[0].source_hash, remote_source_hash(ORIGIN, 5));
    }

    #[test]
    fn empty_batch_is_rejected_unlike_l1_epochs() {
        assert_eq!(
            derive_remote_epoch(SELF, ORIGIN, 0, &[]).unwrap_err(),
            XChainError::Empty
        );
    }

    #[test]
    fn gap_and_regression_and_duplicate_are_faults() {
        let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(6, SELF)]).unwrap_err();
        assert!(
            matches!(
                e,
                XChainError::SeqSkipped {
                    expected: 5,
                    found: 6
                }
            ),
            "got {e:?}"
        );

        let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(4, SELF)]).unwrap_err();
        assert!(
            matches!(
                e,
                XChainError::SeqRegressed {
                    expected: 5,
                    found: 4
                }
            ),
            "got {e:?}"
        );

        let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(7, SELF)]).unwrap_err();
        assert!(
            matches!(
                e,
                XChainError::SeqSkipped {
                    expected: 6,
                    found: 7
                }
            ),
            "got {e:?}"
        );

        let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(5, SELF)]).unwrap_err();
        assert!(
            matches!(e, XChainError::DuplicateSeq { seq: 5 }),
            "got {e:?}"
        );
    }

    #[test]
    fn seq_overflow_is_a_fault_not_a_panic() {
        // Two messages at the top of the u64 range: the gap check would
        // compute `u64::MAX + 1`.
        let e = derive_remote_epoch(
            SELF,
            ORIGIN,
            u64::MAX - 1,
            &[msg(u64::MAX - 1, SELF), msg(u64::MAX, SELF)],
        )
        .unwrap_err();
        assert!(matches!(e, XChainError::SeqOverflow { .. }), "got {e:?}");
        // One message at u64::MAX: the next cursor would overflow.
        let e = derive_remote_epoch(SELF, ORIGIN, u64::MAX, &[msg(u64::MAX, SELF)]).unwrap_err();
        assert!(
            matches!(e, XChainError::SeqOverflow { seq: u64::MAX }),
            "got {e:?}"
        );
        // A hostile record does not panic `last_seq`.
        let r = RemoteEpochRecord {
            first_seq: u64::MAX,
            messages: alloc::vec![XChainMessage::default(), XChainMessage::default()],
            ..Default::default()
        };
        // The value is meaningless for an invalid record. Only the
        // absence of a panic matters.
        let _ = r.last_seq();
        let r = RemoteEpochRecord::default();
        assert_eq!(r.last_seq(), 0);
    }

    #[test]
    fn outbox_bounds_are_mirrored_on_the_producer() {
        // Value.
        let mut m = msg(0, SELF);
        m.value = 1;
        let e = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap_err();
        assert!(
            matches!(e, XChainError::ValueNotAllowed { seq: 0, value: 1 }),
            "got {e:?}"
        );
        // Gas limit.
        let mut m = msg(0, SELF);
        m.gas_limit = MAX_MESSAGE_GAS + 1;
        let e = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap_err();
        assert!(
            matches!(
                e,
                XChainError::GasLimitAboveCap {
                    seq: 0,
                    gas_limit: 10_000_001,
                    cap: MAX_MESSAGE_GAS
                }
            ),
            "got {e:?}"
        );
        // Data length.
        let mut m = msg(0, SELF);
        m.data = AlloyBytes::from(alloc::vec![0xFFu8; MAX_DATA_BYTES + 1]);
        let e = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap_err();
        assert!(
            matches!(
                e,
                XChainError::DataAboveCap {
                    seq: 0,
                    len: 65_537,
                    cap: MAX_DATA_BYTES
                }
            ),
            "got {e:?}"
        );
    }

    #[test]
    fn honest_maxima_pass_the_producer_checks() {
        let mut m = msg(0, SELF);
        m.value = 0;
        m.gas_limit = MAX_MESSAGE_GAS;
        m.data = AlloyBytes::from(alloc::vec![0xFFu8; MAX_DATA_BYTES]);
        let r = derive_remote_epoch(SELF, ORIGIN, 0, &[m]).unwrap();
        assert_eq!(r.messages[0].gas_limit, MAX_MESSAGE_GAS);
        assert_eq!(r.messages[0].input.len(), MAX_DATA_BYTES);
    }

    #[test]
    fn foreign_destination_is_rejected_not_dropped() {
        let e =
            derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF), msg(1, SELF + 9)]).unwrap_err();
        assert!(
            matches!(e, XChainError::ForeignDestination { .. }),
            "got {e:?}"
        );
    }

    #[test]
    fn derivation_is_deterministic_across_callers() {
        let batch = [msg(2, SELF), msg(0, SELF), msg(1, SELF)];
        assert_eq!(
            derive_remote_epoch(SELF, ORIGIN, 0, &batch).unwrap(),
            derive_remote_epoch(SELF, ORIGIN, 0, &batch).unwrap()
        );
    }

    #[test]
    fn canonical_id_keys_on_pair_position() {
        let a = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
        let b = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
        let c = derive_remote_epoch(SELF, ORIGIN, 1, &[msg(1, SELF)]).unwrap();
        assert_eq!(a.canonical_id(), b.canonical_id());
        assert_ne!(a.canonical_id(), c.canonical_id());
    }

    #[test]
    fn leaf_recomputable_from_wire_record() {
        // The property that justifies carrying the un-aliased sender: a
        // verifier holding only the RemoteEpochRecord and the pair identity
        // can recompute every Outbox commitment.
        let rec = derive_remote_epoch(SELF, ORIGIN, 0, &[msg(0, SELF)]).unwrap();
        let m = &rec.messages[0];
        let expect = msg_leaf(
            ORIGIN,
            SELF,
            0,
            Address::repeat_byte(0xA1),
            Address::repeat_byte(0xB2),
            0,
            200_000,
            keccak256([0xCA, 0xFE]),
            no_callback_hash(),
        );
        assert_eq!(m.leaf(ORIGIN, SELF), expect);
    }

    #[test]
    fn callback_commitment_binds_all_fields() {
        let base = Callback {
            target: Address::repeat_byte(0x01),
            gas_limit: 100_000,
            context: B256::repeat_byte(0x02),
        };
        let mut c2 = base;
        c2.gas_limit += 1;
        assert_ne!(base.commitment(), c2.commitment());
        assert_ne!(base.commitment(), no_callback_hash());
    }

    #[test]
    fn leaf_known_vector_is_pinned() {
        // Anchored output for fixed inputs — must match Outbox.hashMessage in
        // contracts/test/Outbox.t.sol (the cross-language tie). Changing the
        // leaf layout flips this and forces a review conversation.
        let leaf = msg_leaf(
            1,
            2,
            3,
            Address::repeat_byte(0x04),
            Address::repeat_byte(0x05),
            6,
            7,
            keccak256([0x08]),
            B256::ZERO,
        );
        // Same inputs, same value, asserted in contracts/test/Outbox.t.sol —
        // the cross-language tie.
        assert_eq!(
            leaf,
            b256!("0df14340efd8c8b32f4c333c3dca8470b0bae319a3dfe32adb213df2b8834d3c")
        );
    }
}

#[cfg(test)]
mod deliver_abi_tests {
    use super::*;
    use alloy_primitives::U256;
    use alloy_sol_types::{SolCall, sol};

    sol! {
        struct SolCb {
            address target;
            uint64 gasLimit;
            bytes32 context;
        }

        function deliver(
            uint64 originChainId,
            uint64 seq,
            address originSender,
            address target,
            uint256 value,
            uint64 gasLimit,
            bytes data,
            SolCb cb
        );
    }

    const ORIGIN: u64 = 412_346;

    fn wire_msg(data: &[u8], callback: Option<Callback>) -> XChainMessage {
        XChainMessage {
            source_hash: remote_source_hash(ORIGIN, 3),
            seq: 3,
            origin_sender: Address::repeat_byte(0xA7),
            target: Address::repeat_byte(0xB9),
            value: 0,
            gas_limit: 250_000,
            input: Bytes::copy_from_slice(data),
            callback,
        }
    }

    /// What `SolCall::abi_encode` produces for the same message — the
    /// compiler-grade oracle the hand-encoding must match byte for byte.
    fn reference(origin_chain_id: u64, m: &XChainMessage) -> alloc::vec::Vec<u8> {
        let cb = m.callback.unwrap_or_default();
        deliverCall {
            originChainId: origin_chain_id,
            seq: m.seq,
            originSender: m.origin_sender,
            target: m.target,
            value: U256::from(m.value),
            gasLimit: m.gas_limit,
            data: AlloyBytes::copy_from_slice(m.input.as_ref()),
            cb: SolCb {
                target: cb.target,
                gasLimit: cb.gas_limit,
                context: cb.context,
            },
        }
        .abi_encode()
    }

    #[test]
    fn selector_matches_sol_resolution() {
        // `sol!` resolves the user-defined struct to its tuple type when
        // computing the selector — the same resolution Solidity applies to
        // `XChain.Callback calldata cb` — so this pins the signature string
        // against the actual Inbox dispatch.
        assert_eq!(inbox_deliver_selector(), deliverCall::SELECTOR);
    }

    #[test]
    fn hand_encoding_is_byte_identical_to_sol_types() {
        let cb = Callback {
            target: Address::repeat_byte(0x0C),
            gas_limit: 90_000,
            context: B256::repeat_byte(0x1D),
        };
        // Empty, sub-word, and word+1 payloads exercise the zero-length
        // tail, right-padding, and a two-word tail respectively.
        for data in [&b""[..], &[0x01][..], &[0xEE; 33][..]] {
            for callback in [None, Some(cb)] {
                let m = wire_msg(data, callback);
                assert_eq!(
                    deliver_calldata(ORIGIN, &m),
                    reference(ORIGIN, &m),
                    "data len {}, callback {}",
                    data.len(),
                    callback.is_some()
                );
            }
        }
    }

    #[test]
    fn value_rides_the_uint256_word() {
        // v1 execution rejects nonzero value, but the encoding must already
        // carry it faithfully — the wire does not change when value ships.
        let mut m = wire_msg(&[0x02], None);
        m.value = u128::MAX;
        assert_eq!(deliver_calldata(ORIGIN, &m), reference(ORIGIN, &m));
    }
}
