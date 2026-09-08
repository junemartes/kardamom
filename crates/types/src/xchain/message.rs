//! The wire and record types: one message, one callback, and one origin's
//! batch of messages.

use alloc::vec::Vec;

use alloy_primitives::{Address, B256, Bytes as AlloyBytes, keccak256};
use bytes::Bytes;
use rkyv::with::Map;
use rkyv::{Archive, Deserialize, Serialize};

use super::leaf::{MsgLeaf, no_callback_hash};
use super::{MAX_DATA_BYTES, MAX_MESSAGE_GAS};
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
    /// Check this message against the Outbox's own send-time bounds: no
    /// value (v1 delivery is value-free), a gas limit within
    /// [`MAX_MESSAGE_GAS`], and calldata within [`MAX_DATA_BYTES`]. An
    /// honest origin can never trip these; a message that does came from a
    /// malicious or corrupt feed, never from the shared derivation rule.
    ///
    /// # Errors
    ///
    /// Returns the bound that failed.
    pub fn check_bounds(&self) -> Result<(), BoundsFault> {
        if self.value != 0 {
            return Err(BoundsFault::ValueNotAllowed {
                seq: self.seq,
                value: self.value,
            });
        }
        if self.gas_limit > MAX_MESSAGE_GAS {
            return Err(BoundsFault::GasLimitAboveCap {
                seq: self.seq,
                gas_limit: self.gas_limit,
                cap: MAX_MESSAGE_GAS,
            });
        }
        if self.input.len() > MAX_DATA_BYTES {
            return Err(BoundsFault::DataAboveCap {
                seq: self.seq,
                len: self.input.len(),
                cap: MAX_DATA_BYTES,
            });
        }
        Ok(())
    }

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

impl OutboxMessage {
    /// Check this message against the Outbox's own send-time bounds,
    /// before it is copied into an [`XChainMessage`]. Same limits as
    /// [`XChainMessage::check_bounds`]; running this on the borrowed
    /// message first means a batch that fails never pays for the copy.
    ///
    /// # Errors
    ///
    /// Returns the bound that failed.
    pub fn check_bounds(&self) -> Result<(), BoundsFault> {
        if self.value != 0 {
            return Err(BoundsFault::ValueNotAllowed {
                seq: self.seq,
                value: self.value,
            });
        }
        if self.gas_limit > MAX_MESSAGE_GAS {
            return Err(BoundsFault::GasLimitAboveCap {
                seq: self.seq,
                gas_limit: self.gas_limit,
                cap: MAX_MESSAGE_GAS,
            });
        }
        if self.data.len() > MAX_DATA_BYTES {
            return Err(BoundsFault::DataAboveCap {
                seq: self.seq,
                len: self.data.len(),
                cap: MAX_DATA_BYTES,
            });
        }
        Ok(())
    }
}

/// Why an [`XChainMessage`] fails the Outbox's send-time bounds. The origin
/// `Outbox` rejects each of these at send time, so a message that carries
/// one did not come from an honest origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BoundsFault {
    /// v1 messaging carries no value.
    #[error("message seq {seq} carries value {value}; v1 delivery is value-free")]
    ValueNotAllowed { seq: u64, value: u128 },
    /// `gas_limit` above the cap the `Outbox` accepts.
    #[error("message seq {seq} gas limit {gas_limit} is above the cap {cap}")]
    GasLimitAboveCap { seq: u64, gas_limit: u64, cap: u64 },
    /// `data` longer than the length the `Outbox` accepts.
    #[error("message seq {seq} data length {len} is above the cap {cap}")]
    DataAboveCap { seq: u64, len: usize, cap: usize },
}

/// A [`Vec<T>`] that is never empty, checked once at construction or at
/// the wire boundary. Nothing that reads one needs to check its length
/// first.
#[derive(Clone, Debug, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck(verify))]
pub struct NonEmptyVec<T>(Vec<T>);

impl<T> NonEmptyVec<T> {
    /// Build from a first element plus the rest, in order.
    #[must_use]
    pub fn new(first: T, rest: Vec<T>) -> Self {
        let mut items = Vec::with_capacity(rest.len().saturating_add(1));
        items.push(first);
        items.extend(rest);
        Self(items)
    }

    /// The one place this type's non-empty invariant is read off the
    /// storage. `len`, `first`, and `last` all derive from this instead of
    /// each repeating the same panic path.
    ///
    /// Storage stays a bare `Vec<T>` — not a `(first, rest)` field split —
    /// because the archived bytes of a `NonEmptyVec` must stay identical to
    /// a plain `Vec<T>`'s (see `status-validator.md`, Round B follow-up 2):
    /// changing the shape would change every `RemoteEpochRecord` on the
    /// wire. So the invariant lives here, at read time, instead of in the
    /// type.
    ///
    /// # Panics
    ///
    /// Never, in practice: `new` and the wire decoder both guarantee at
    /// least one element.
    fn split(&self) -> (&T, &[T]) {
        self.0.split_first().expect("non-empty by construction")
    }

    /// The element count. Never zero.
    #[must_use]
    pub fn len(&self) -> core::num::NonZeroUsize {
        let (_, rest) = self.split();
        core::num::NonZeroUsize::MIN.saturating_add(rest.len())
    }

    /// The first element. Always present.
    #[must_use]
    pub fn first(&self) -> &T {
        self.split().0
    }

    /// The last element. Always present.
    #[must_use]
    pub fn last(&self) -> &T {
        let (first, rest) = self.split();
        rest.last().unwrap_or(first)
    }

    /// Every element, in order.
    pub fn iter(&self) -> core::slice::Iter<'_, T> {
        self.0.iter()
    }

    /// Every element, as a slice.
    #[must_use]
    pub fn as_slice(&self) -> &[T] {
        &self.0
    }
}

impl<'a, T> IntoIterator for &'a NonEmptyVec<T> {
    type Item = &'a T;
    type IntoIter = core::slice::Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T: rkyv::Archive> core::fmt::Debug for ArchivedNonEmptyVec<T>
where
    rkyv::Archived<T>: core::fmt::Debug,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(&self.0, f)
    }
}

/// Rejects an archived [`NonEmptyVec`] with zero elements. The derived
/// field check already validates the inner `Vec`'s bytes; this adds the
/// one invariant a byte-level check cannot see.
mod non_empty_verify {
    use rkyv::bytecheck::Verify;
    use rkyv::rancor::{Fallible, Source, fail};

    use super::ArchivedNonEmptyVec;

    #[derive(Debug)]
    struct EmptyNonEmptyVec;

    impl core::fmt::Display for EmptyNonEmptyVec {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("NonEmptyVec archive has zero elements")
        }
    }

    impl core::error::Error for EmptyNonEmptyVec {}

    // SAFETY: `verify` only reads `self.0.is_empty()`; it does not read or
    // rely on the element bytes the derived field check already validated.
    unsafe impl<T, C> Verify<C> for ArchivedNonEmptyVec<T>
    where
        T: rkyv::Archive,
        C: Fallible + ?Sized,
        C::Error: Source,
    {
        fn verify(&self, _: &mut C) -> Result<(), C::Error> {
            if self.0.is_empty() {
                fail!(EmptyNonEmptyVec);
            }
            Ok(())
        }
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
#[derive(Clone, Debug, Eq, PartialEq, Archive, Serialize, Deserialize)]
#[rkyv(derive(Debug), bytecheck(verify))]
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
    /// The batch, in seq order, dense from `first_seq`.
    pub messages: NonEmptyVec<XChainMessage>,
}

/// Rejects an archived [`RemoteEpochRecord`] that leaves no room for the
/// lane cursor after it (`first_seq + messages.len()` overflows `u64` —
/// one more than `last_seq` itself needing to fit). This is the same bound
/// the producer's batch-range check and the validator's `SeqRange::new`
/// both check, so the wire decoder agrees with what can actually be
/// produced or accepted: [`super::derive_remote_epoch`] and the
/// validator's `check_remote_epoch` both reject this before a record is
/// ever built or accepted; this closes the same gap at the wire boundary,
/// where a corrupt or malicious archive skips those checks.
mod remote_epoch_verify {
    use rkyv::bytecheck::Verify;
    use rkyv::rancor::{Fallible, Source, fail};

    use super::ArchivedRemoteEpochRecord;

    #[derive(Debug)]
    struct RemoteEpochSeqOverflow;

    impl core::fmt::Display for RemoteEpochSeqOverflow {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("RemoteEpochRecord archive's seq range overflows u64")
        }
    }

    impl core::error::Error for RemoteEpochSeqOverflow {}

    // SAFETY: `verify` only reads `first_seq` and the message count, both
    // already validated as plain values by the derived field check; it
    // performs no unchecked memory access.
    unsafe impl<C> Verify<C> for ArchivedRemoteEpochRecord
    where
        C: Fallible + ?Sized,
        C::Error: Source,
    {
        fn verify(&self, _: &mut C) -> Result<(), C::Error> {
            let first_seq = self.first_seq.to_native();
            let len = u64::try_from(self.messages.0.len()).ok();
            if len.and_then(|l| first_seq.checked_add(l)).is_none() {
                fail!(RemoteEpochSeqOverflow);
            }
            Ok(())
        }
    }
}

impl RemoteEpochRecord {
    /// Sequence number of the last message in the batch.
    ///
    /// Saturates instead of overflowing: [`super::derive_remote_epoch`],
    /// the validator's `check_remote_epoch`, and the wire decoder's
    /// [`Verify`](rkyv::bytecheck::Verify) impl below all reject a batch
    /// whose seq range does not fit `u64` before a real record reaches
    /// here, but this method's own contract stays total, the same way it
    /// did before `messages` was a [`NonEmptyVec`].
    #[must_use]
    pub fn last_seq(&self) -> u64 {
        let len = u64::try_from(self.messages.len().get()).unwrap_or(u64::MAX);
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
        super::keccak_concat(&[
            &self.origin_chain_id.to_be_bytes(),
            &self.anchor_number.to_be_bytes(),
            self.anchor_hash.as_slice(),
            &self.first_seq.to_be_bytes(),
            &self.last_seq().to_be_bytes(),
        ])
    }
}
