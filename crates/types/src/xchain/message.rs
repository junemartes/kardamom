//! The wire and record types: one message, one callback, and one origin's
//! batch of messages.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, keccak256};
use bytes::Bytes;
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize};

use super::leaf::{MsgLeaf, no_callback_hash};
use crate::wire;

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
    /// of [`super::msg_leaf`]. Must equal `Outbox.hashCallback`.
    #[must_use]
    pub fn commitment(&self) -> B256 {
        let mut buf = [0u8; 96];
        buf[0..32].copy_from_slice(&crate::abi::word_address(self.target));
        buf[32..64].copy_from_slice(&crate::abi::word_u64(self.gas_limit));
        buf[64..96].copy_from_slice(self.context.as_slice());
        keccak256(buf)
    }
}

/// One decoded `MessageSent` outbox event, as observed on the ORIGIN chain.
///
/// Pure data: whether it came from a peer validator's WS feed, a
/// B-operated sovereign validator, or a test fixture is not this type's
/// business — that transport-agnosticism is what lets the producer and the
/// verifier share [`super::derive_remote_epoch`], and what lets the e2e suite
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
/// [`super::msg_leaf`] from the wire record alone, and the leaf commits to
/// the origin-side sender. Execution aliases at the edge via
/// [`super::alias_remote_address`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug))]
pub struct XChainMessage {
    /// [`super::remote_source_hash`] — canonical id, dedup key, and the
    /// receipt's `tx_hash` on the destination.
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
    /// Recompute this message's Outbox commitment. `origin_chain_id` and
    /// `dest_chain_id` come from the enclosing [`RemoteEpochRecord`] and the
    /// deriving chain respectively — they are not duplicated on the wire.
    pub fn leaf(&self, origin_chain_id: u64, dest_chain_id: u64) -> B256 {
        MsgLeaf {
            origin_chain_id,
            dest_chain_id,
            seq: self.seq,
            sender: self.origin_sender,
            target: self.target,
            value: self.value,
            gas_limit: self.gas_limit,
            data_hash: keccak256(&self.input),
            cb_hash: self
                .callback
                .as_ref()
                .map_or_else(no_callback_hash, Callback::commitment),
        }
        .hash()
    }
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
    /// construction ([`super::derive_remote_epoch`] rejects empty input).
    pub messages: Vec<XChainMessage>,
}

impl RemoteEpochRecord {
    /// Sequence number of the last message in the batch.
    ///
    /// Saturates instead of underflowing when the batch is empty (an empty
    /// `RemoteEpochRecord` is not a valid record, but the derived `Default`
    /// still constructs one, so this must not wrap or panic).
    #[must_use]
    pub fn last_seq(&self) -> u64 {
        // `unwrap_or(u64::MAX)` keeps this saturating end to end: a `Vec`
        // longer than `u64::MAX` never occurs in practice, and this method's
        // contract is to never wrap or panic even on a default-constructed,
        // structurally-invalid record.
        let len = u64::try_from(self.messages.len()).unwrap_or(u64::MAX);
        self.first_seq.saturating_add(len).saturating_sub(1)
    }

    /// Canonical id for cluster dedup: racing relayers that observe the same
    /// feed prefix derive byte-identical records, and first-seen dedup
    /// collapses them — the `EpochRecord::canonical_id` property, keyed by
    /// pair position rather than L1 hash.
    ///
    /// The preimage commits to `anchor_number` as well as to the anchor
    /// hash and the seq range. The sealer reads the anchor from the frame
    /// header, and the egress decoder recomputes this id from the body. So
    /// a header whose anchor differs from the body fails the cross-check
    /// instead of moving the sealer's per-peer position.
    #[must_use]
    pub fn canonical_id(&self) -> B256 {
        let mut buf = Vec::with_capacity(8 + 8 + 32 + 16);
        buf.extend_from_slice(&self.origin_chain_id.to_be_bytes());
        buf.extend_from_slice(&self.anchor_number.to_be_bytes());
        buf.extend_from_slice(self.anchor_hash.as_slice());
        buf.extend_from_slice(&self.first_seq.to_be_bytes());
        buf.extend_from_slice(&self.last_seq().to_be_bytes());
        keccak256(&buf)
    }
}
