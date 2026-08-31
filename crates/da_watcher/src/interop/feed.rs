//! The interop feed WIRE CONTRACT: what an origin chain's validator serves on
//! `kardamom_subscribeOutbox` and what a destination's watcher consumes.
//!
//! These DTOs are the only thing the two chains share, so they are versioned
//! by shape rather than by a version field: adding a variant or an optional
//! field is compatible, renaming or retyping one is a peer-set-wide migration.
//! The producer side (`crates/validator`, spec §5) does not exist yet — this
//! module and [`crate::interop::mock`] are what the real feed will have to
//! satisfy, which is the point of writing them first.
//!
//! ## Why the shapes are not `kardamom_types::xchain` types directly
//!
//! `OutboxMessage` is an internal type free to change with the protocol; this
//! is a cross-operator interface. Keeping them separate means a peer running an
//! older build fails to decode a field rather than silently mis-deriving one.
//! The conversion is the only place that difference is adjudicated
//! ([`OutboxMessageDto::into_outbox_message`]).
//!
//! ## v1 runs in FEED-TRUST mode
//!
//! Spec §5 stamps every item with a finality tier (`Quorum`/`Anchored`) and
//! carries an `AnchorProof`; §10 makes the L1-anchored tier mandatory before a
//! message executes (and unconditional once value rides along). Neither is on
//! this wire yet: a v1 destination believes its configured feed, which is the
//! spec's T0 tier — development and test only. Anchoring events are a later
//! slice and will arrive as ADDITIONAL [`OutboxEventDto`] variants plus
//! optional fields, so a `Message` decoded today still decodes then.
//!
//! ## Numbers on the wire
//!
//! Block numbers, chain ids, seqs and gas limits ride as JSON numbers: all are
//! bounded well below the 2^53 a JSON consumer can hold exactly. `value` is
//! not — it is a wei amount — so it rides the Ethereum hex-quantity
//! convention as a `U256` string and is narrowed to the protocol's `u128` on
//! decode, where an over-range value is a decode fault rather than a wrap.

use alloy_primitives::{Address, B256, Bytes, U256};
use jsonrpsee::core::SubscriptionResult;
use jsonrpsee::proc_macros::rpc;
use kardamom_types::xchain::{Callback, OutboxMessage};
use serde::{Deserialize, Serialize};

/// Subscription method name. Spelled out as a constant because the client side
/// ([`crate::interop::source::WsRemoteChainSource`]) addresses a server it does
/// not link against.
pub const SUBSCRIBE_OUTBOX_METHOD: &str = "kardamom_subscribeOutbox";

/// Unsubscribe method name, as jsonrpsee derives it from the subscription.
pub const UNSUBSCRIBE_OUTBOX_METHOD: &str = "kardamom_unsubscribeOutbox";

/// Resume point of a subscription: `seq` is the first per-pair sequence number
/// the subscriber has NOT yet canonicalised.
///
/// The server must serve from exactly this seq onward. That contract — not
/// "roughly from here" — is what makes a reconnect byte-identical instead of
/// merely lossless: the same cursor replays the same messages, so the records
/// derived after a drop share their `canonical_id` with the ones derived
/// before it and cluster dedup collapses the overlap.
///
/// A struct rather than a bare `u64` so anchoring can add its own resume
/// dimension (spec §5's `originBlock`) without changing the method's arity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutboxCursor {
    /// First seq not yet canonicalised by the subscriber.
    pub seq: u64,
}

impl OutboxCursor {
    pub fn new(seq: u64) -> Self {
        Self { seq }
    }
}

/// Requested response leg of a message (`kardamom_types::xchain::Callback`).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallbackDto {
    /// Contract on the ORIGIN chain that receives the response.
    pub target: Address,
    /// Gas budget for the response delivery back on the origin.
    pub gas_limit: u64,
    /// Opaque correlation value, echoed back verbatim.
    pub context: B256,
}

/// One outbox message as served by the origin's feed.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutboxMessageDto {
    /// The serving chain's id. Carried per item, not just per subscription, so
    /// a watcher can tell a mis-pointed endpoint from an empty one.
    pub origin_chain_id: u64,
    /// Origin block the send happened in. Load-bearing: it is what groups
    /// messages into batches (see [`crate::interop::source`]).
    pub origin_block_number: u64,
    /// Hash of that origin block.
    pub origin_block_hash: B256,
    /// Destination the sender addressed. NOT assumed to equal the subscriber:
    /// a foreign destination here is a fault the derivation rule must reject,
    /// so it has to survive the wire to be rejected.
    pub dest_chain_id: u64,
    /// Dense per-(origin, dest) sequence number.
    pub seq: u64,
    /// Origin-chain sender, un-aliased.
    pub sender: Address,
    /// Destination-chain call target.
    pub target: Address,
    /// Value burned on the origin, to be minted on the destination. Hex
    /// quantity; see the module docs.
    pub value: U256,
    /// Gas budget for the inner call on the destination.
    pub gas_limit: u64,
    /// Inner-call calldata, `0x`-prefixed hex.
    pub data: Bytes,
    /// Requested response, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub callback: Option<CallbackDto>,
}

/// One item on the feed.
///
/// Internally tagged (`{"type":"message", ...}` / `{"type":"lagged", ...}`),
/// matching the `kardamom_subscribeReceipts` frame shape so one client idiom
/// covers both subscriptions.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum OutboxEventDto {
    /// An extracted outbox message. Boxed so the frame is not sized by its
    /// largest variant — the lag marker is two words.
    Message(Box<OutboxMessageDto>),
    /// The subscriber fell behind the server's retention and `skipped` items
    /// were dropped. NOT recoverable by reading on: the subscriber must
    /// re-subscribe from its own cursor, which is the same recovery
    /// `kardamom_subscribeReceipts` prescribes. Carrying the count makes the
    /// loss measurable rather than merely survivable.
    #[serde(rename_all = "camelCase")]
    Lagged { skipped: u64 },
}

/// Why a feed item could not be turned into an [`OutboxMessage`].
///
/// Every variant means the peer is running something this build does not
/// understand — a misconfigured endpoint or an incompatible protocol version —
/// never a transient.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FeedDecodeError {
    /// The endpoint served a message from a chain other than the one it was
    /// configured as. Rejected rather than trusted: `origin_chain_id` is an
    /// input to every message's `source_hash` and alias, so believing the item
    /// over the config would silently re-identify the whole pair.
    #[error("feed for origin {expected} served a message from chain {found}")]
    ForeignOrigin { expected: u64, found: u64 },
    /// `value` exceeds the `u128` the protocol carries. Truncating would move
    /// a different amount than the origin burned.
    #[error("message value {value} exceeds the u128 the protocol carries")]
    ValueTooLarge { value: U256 },
}

impl OutboxMessageDto {
    /// Encode an observed message for the wire.
    pub fn from_outbox_message(origin_chain_id: u64, m: &OutboxMessage) -> Self {
        Self {
            origin_chain_id,
            origin_block_number: m.origin_block_number,
            origin_block_hash: m.origin_block_hash,
            dest_chain_id: m.dest_chain_id,
            seq: m.seq,
            sender: m.sender,
            target: m.target,
            value: U256::from(m.value),
            gas_limit: m.gas_limit,
            data: m.data.clone(),
            callback: m.callback.map(|cb| CallbackDto {
                target: cb.target,
                gas_limit: cb.gas_limit,
                context: cb.context,
            }),
        }
    }

    /// Decode into the protocol type, checking the item against the origin the
    /// subscriber believes it is talking to.
    pub fn into_outbox_message(
        self,
        expected_origin: u64,
    ) -> Result<OutboxMessage, FeedDecodeError> {
        if self.origin_chain_id != expected_origin {
            return Err(FeedDecodeError::ForeignOrigin {
                expected: expected_origin,
                found: self.origin_chain_id,
            });
        }
        let value = u128::try_from(self.value)
            .map_err(|_| FeedDecodeError::ValueTooLarge { value: self.value })?;
        Ok(OutboxMessage {
            origin_block_number: self.origin_block_number,
            origin_block_hash: self.origin_block_hash,
            // Deliberately passed through unchecked: `derive_remote_epoch`
            // owns the foreign-destination verdict, and it must see the batch
            // to fail-stop on it.
            dest_chain_id: self.dest_chain_id,
            seq: self.seq,
            sender: self.sender,
            target: self.target,
            value,
            gas_limit: self.gas_limit,
            data: self.data,
            callback: self.callback.map(|cb| Callback {
                target: cb.target,
                gas_limit: cb.gas_limit,
                context: cb.context,
            }),
        })
    }
}

/// The origin-side feed surface (spec §5). Defined here, next to the DTOs,
/// because it IS the contract: `crates/validator` implements this trait when
/// the real extractor lands, and [`crate::interop::mock`] implements it now so
/// the destination side can be finished and tested against a real server
/// first.
#[rpc(server, namespace = "kardamom")]
pub trait OutboxFeedApi {
    /// Stream outbox messages addressed to `dest_chain_id`, resuming at
    /// `cursor`. WebSocket only.
    #[subscription(name = "subscribeOutbox", item = OutboxEventDto)]
    async fn subscribe_outbox(
        &self,
        dest_chain_id: u64,
        cursor: OutboxCursor,
    ) -> SubscriptionResult;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> OutboxMessage {
        OutboxMessage {
            origin_block_number: 100,
            origin_block_hash: B256::repeat_byte(0x0B),
            dest_chain_id: 7,
            seq: 3,
            sender: Address::repeat_byte(0xA1),
            target: Address::repeat_byte(0xB2),
            value: 0,
            gas_limit: 200_000,
            data: Bytes::from_static(&[0xCA, 0xFE]),
            callback: Some(Callback {
                target: Address::repeat_byte(0x0C),
                gas_limit: 90_000,
                context: B256::repeat_byte(0x1D),
            }),
        }
    }

    #[test]
    fn message_round_trips_through_json() {
        let m = sample();
        let dto = OutboxEventDto::Message(Box::new(OutboxMessageDto::from_outbox_message(5, &m)));
        let json = serde_json::to_string(&dto).unwrap();
        let back: OutboxEventDto = serde_json::from_str(&json).unwrap();
        assert_eq!(dto, back);
        let OutboxEventDto::Message(dto) = back else {
            panic!("expected a message frame")
        };
        assert_eq!(dto.into_outbox_message(5).unwrap(), m);
    }

    /// The wire shape is the contract; pin the parts a peer implementation in
    /// another language has to reproduce exactly.
    #[test]
    fn wire_shape_is_pinned() {
        let dto = OutboxEventDto::Message(Box::new(OutboxMessageDto::from_outbox_message(
            5,
            &sample(),
        )));
        let v: serde_json::Value = serde_json::to_value(&dto).unwrap();
        assert_eq!(v["type"], "message");
        assert_eq!(v["originChainId"], 5);
        assert_eq!(v["data"], "0xcafe", "calldata rides as 0x-hex");
        assert_eq!(
            v["value"], "0x0",
            "wei is a hex quantity, not a JSON number"
        );
        assert_eq!(v["callback"]["gasLimit"], 90_000);

        let lagged = serde_json::to_value(OutboxEventDto::Lagged { skipped: 12 }).unwrap();
        assert_eq!(lagged["type"], "lagged");
        assert_eq!(lagged["skipped"], 12);
    }

    #[test]
    fn cursor_is_an_object_so_anchoring_can_extend_it() {
        let v = serde_json::to_value(OutboxCursor::new(9)).unwrap();
        assert_eq!(v, serde_json::json!({ "seq": 9 }));
    }

    #[test]
    fn a_message_from_the_wrong_origin_is_rejected_not_relabelled() {
        let dto = OutboxMessageDto::from_outbox_message(5, &sample());
        assert_eq!(
            dto.into_outbox_message(6).unwrap_err(),
            FeedDecodeError::ForeignOrigin {
                expected: 6,
                found: 5
            }
        );
    }

    #[test]
    fn over_range_value_is_a_decode_fault_not_a_truncation() {
        let mut dto = OutboxMessageDto::from_outbox_message(5, &sample());
        dto.value = U256::from(u128::MAX) + U256::from(1u64);
        assert!(matches!(
            dto.into_outbox_message(5),
            Err(FeedDecodeError::ValueTooLarge { .. })
        ));
    }

    /// A foreign destination must survive decoding so `derive_remote_epoch`
    /// can fail-stop on it; dropping it here would silently repair a fault.
    #[test]
    fn a_foreign_destination_survives_decoding() {
        let mut m = sample();
        m.dest_chain_id = 999;
        let dto = OutboxMessageDto::from_outbox_message(5, &m);
        assert_eq!(dto.into_outbox_message(5).unwrap().dest_chain_id, 999);
    }
}
