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
        self.first_seq + self.messages.len() as u64 - 1
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
    if let Some(gap) = ordered.windows(2).find(|w| w[1].seq != w[0].seq + 1) {
        return Err(XChainError::SeqSkipped {
            expected: gap[0].seq + 1,
            found: gap[1].seq,
        });
    }
    for m in &ordered {
        if m.dest_chain_id != self_chain_id {
            return Err(XChainError::ForeignDestination {
                expected: self_chain_id,
                found: m.dest_chain_id,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, b256};

    fn msg(seq: u64, dest: u64) -> OutboxMessage {
        OutboxMessage {
            origin_block_number: 100 + seq,
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
        assert_eq!(
            remote_source_hash(ORIGIN, 7),
            remote_source_hash(ORIGIN, 7)
        );
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
        assert!(matches!(e, XChainError::SeqSkipped { expected: 5, found: 6 }), "got {e:?}");

        let e = derive_remote_epoch(SELF, ORIGIN, 5, &[msg(4, SELF)]).unwrap_err();
        assert!(matches!(e, XChainError::SeqRegressed { expected: 5, found: 4 }), "got {e:?}");

        let e =
            derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(7, SELF)]).unwrap_err();
        assert!(matches!(e, XChainError::SeqSkipped { expected: 6, found: 7 }), "got {e:?}");

        let e =
            derive_remote_epoch(SELF, ORIGIN, 5, &[msg(5, SELF), msg(5, SELF)]).unwrap_err();
        assert!(matches!(e, XChainError::DuplicateSeq { seq: 5 }), "got {e:?}");
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
