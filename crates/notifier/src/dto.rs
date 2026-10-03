//! The JSON shapes a client sees: the status event, the subscription
//! filter, the lag marker, and the webhook registration.
//!
//! Keys are `snake_case`. An absent optional value is omitted, never
//! `null`.

use alloy_primitives::{Address, B256};
use kardamom_types::{TxErrorReason, TxStage, TxStageKind, TxStatus};
use serde::{Deserialize, Deserializer, Serialize};

/// One status event as a client sees it. `sender` and `nonce` are
/// present when the pipeline or the notifier's ring holds them; a
/// `sealed` event the ring saw before the `offered` one carries the hash
/// alone. `status` is present for `executed`, `reason` and
/// `expected_nonce` for `rejected`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxStatusEvent {
    pub tx_hash: B256,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<Address>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<u64>,
    pub stage: Stage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_nonce: Option<u64>,
}

/// The `stage` word of an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stage {
    Offered,
    Sealed,
    Executed,
    Rejected,
}

impl From<TxStageKind> for Stage {
    fn from(kind: TxStageKind) -> Self {
        match kind {
            TxStageKind::Offered => Self::Offered,
            TxStageKind::Sealed => Self::Sealed,
            TxStageKind::Executed => Self::Executed,
            TxStageKind::Rejected => Self::Rejected,
        }
    }
}

impl TxStatusEvent {
    /// Shape `status` for a client. `identity` is the sender and nonce
    /// the ring resolved for the hash; it fills a `sealed` event, which
    /// carries none on the wire.
    #[must_use]
    pub fn from_status(status: &TxStatus, identity: Option<(Address, u64)>) -> Self {
        let (sender, nonce) = identity.map_or((None, None), |(s, n)| (Some(s), Some(n)));
        let (receipt_status, reason, expected_nonce) = match &status.stage {
            TxStage::Executed { status, .. } => (Some(u8::from(*status)), None, None),
            TxStage::Rejected { reason, .. } => {
                let (word, expected) = describe_reason(reason);
                (None, Some(word.to_string()), expected)
            }
            TxStage::Offered { .. } | TxStage::Sealed => (None, None, None),
        };
        Self {
            tx_hash: status.tx_hash,
            sender,
            nonce,
            stage: status.kind().into(),
            status: receipt_status,
            reason,
            expected_nonce,
        }
    }
}

/// The rejection words the receipt feed uses, so one client vocabulary
/// serves both feeds. The deadline names a block, not a nonce, so its
/// expected nonce stays empty.
fn describe_reason(reason: &TxErrorReason) -> (&'static str, Option<u64>) {
    match reason {
        TxErrorReason::DuplicatedTx { expected_nonce } => ("duplicated-tx", Some(*expected_nonce)),
        TxErrorReason::Evicted { expected_nonce } => ("evicted", Some(*expected_nonce)),
        TxErrorReason::Expired { expected_nonce } => ("expired", Some(*expected_nonce)),
        TxErrorReason::PastDeadline { .. } => ("past-deadline", None),
    }
}

/// The subscription filter: `{"all": true}`, `{"sender": "0x…"}` or
/// `{"tx_hash": "0x…"}`. The full feed is public, as the chain's blocks
/// are; a filter narrows, it does not protect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StatusFilter {
    Sender { sender: Address },
    TxHash { tx_hash: B256 },
    All { all: True },
}

impl StatusFilter {
    /// Whether `event` passes this filter. A `sealed` event with no
    /// resolved sender passes only the hash and the full filters.
    #[must_use]
    pub fn matches(&self, event: &TxStatusEvent) -> bool {
        match self {
            Self::Sender { sender } => event.sender == Some(*sender),
            Self::TxHash { tx_hash } => event.tx_hash == *tx_hash,
            Self::All { .. } => true,
        }
    }

    /// The full feed.
    #[must_use]
    pub const fn all() -> Self {
        Self::All { all: True }
    }
}

/// The JSON literal `true`, and nothing else. `{"all": false}` is not a
/// filter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct True;

impl Serialize for True {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bool(true)
    }
}

impl<'de> Deserialize<'de> for True {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if bool::deserialize(deserializer)? {
            Ok(Self)
        } else {
            Err(serde::de::Error::custom("expected true"))
        }
    }
}

/// One item of the WebSocket feed: a status event, or the lag marker a
/// slow client gets when the feed buffer rolled past it. After a marker
/// the stream continues from the present; the client fills the gap from
/// the receipt feed or by resubscribing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FeedItem {
    Status(TxStatusEvent),
    Lagged(Lagged),
}

/// The lag marker: `{"stage":"lagged","skipped":N}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lagged {
    pub stage: LaggedStage,
    pub skipped: u64,
}

/// The one word the marker's `stage` field holds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LaggedStage {
    #[default]
    Lagged,
}

impl Lagged {
    #[must_use]
    pub const fn skipped(skipped: u64) -> Self {
        Self {
            stage: LaggedStage::Lagged,
            skipped,
        }
    }
}

/// A webhook registration: `POST /webhooks`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookRequest {
    /// The URL the notifier posts every matching event to.
    pub url: String,
    pub filter: StatusFilter,
    /// The HMAC-SHA256 key of the signature header.
    pub secret: String,
}

/// The answer to a registration: the subscription id and the instance
/// that delivers it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookAck {
    pub id: B256,
    pub owner: u32,
}
