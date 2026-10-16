//! How the sealer turns one ingress frame into one relayed egress frame, and
//! the ingress size cap that follows from it.
//!
//! The sealer relays three ingress kinds as an egress kind-1 frame. Each
//! relay keeps the canonical id and the record type with its fields. It
//! drops the bytes in front of the canonical id and the sealer-only header
//! after it, and puts the egress header `[kind][index:u64][payload_len:u32]`
//! in front. One table holds the shape of each relay. The Java mirror is
//! `RelayShape` in the sealer service.
//!
//! A kind-3 batch relays each entry as a kind-0 frame, so its relay is the
//! kind-0 relay of each entry. The batch header adds to the ingress side
//! only.

use kardamom_cluster_client::message::{MAX_SESSION_MESSAGE_LEN, SESSION_HEADER_LEN};

use super::{
    INGRESS_CANONICAL_ID_OFFSET, KIND_INGRESS_RECORD, KIND_ORIGIN_RECORD, KIND_REMOTE_ORIGIN_RECORD,
};

/// The egress header in front of a relayed payload:
/// `[kind:u8 = 1][index:u64][payload_len:u32]`.
pub(crate) const RELAYED_HEADER_LEN: usize = 1 + 8 + 4;

/// The sealer-only header after the canonical id of a
/// [`KIND_ORIGIN_RECORD`] frame: `[l1_origin:u64][slot_count:u32]`.
const ORIGIN_HEADER_LEN: usize = 8 + 4;

/// The sealer-only header after the canonical id of a
/// [`KIND_REMOTE_ORIGIN_RECORD`] frame:
/// `[origin_chain_id:u64][anchor_number:u64][slot_count:u32][first_seq:u64][last_seq:u64]`.
const REMOTE_ORIGIN_HEADER_LEN: usize = 8 + 8 + 4 + 8 + 8;

/// The relay of one ingress kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RelayShape {
    /// The ingress kind.
    pub(crate) kind: u8,
    /// The ingress bytes in front of the canonical id.
    pub(crate) before_id: usize,
    /// The sealer-only header between the canonical id and the record type.
    pub(crate) after_id: usize,
}

/// The relay of each ingress kind that the sealer relays.
pub(crate) const RELAY_SHAPES: [RelayShape; 3] = [
    RelayShape {
        kind: KIND_INGRESS_RECORD,
        before_id: INGRESS_CANONICAL_ID_OFFSET,
        after_id: 0,
    },
    RelayShape {
        kind: KIND_ORIGIN_RECORD,
        before_id: 1,
        after_id: ORIGIN_HEADER_LEN,
    },
    RelayShape {
        kind: KIND_REMOTE_ORIGIN_RECORD,
        before_id: 1,
        after_id: REMOTE_ORIGIN_HEADER_LEN,
    },
];

/// The longest application payload of one ingress session message.
///
/// A relayed frame of an accepted ingress message must fit one egress
/// session message of [`MAX_SESSION_MESSAGE_LEN`]. So the cap is the
/// longest payload of one session message, less the largest growth of a
/// relay in [`RELAY_SHAPES`].
pub(crate) const MAX_INGRESS_PAYLOAD_LEN: usize =
    MAX_SESSION_MESSAGE_LEN - SESSION_HEADER_LEN - RelayShape::max_growth(0);

impl RelayShape {
    /// The ingress bytes that the relay drops. The terms are constants of a
    /// few bytes each, and a const overflow fails the build.
    const fn dropped(self) -> usize {
        self.before_id + self.after_id
    }

    /// How many bytes the relayed egress frame adds to the ingress frame.
    /// A relay that drops more bytes than its egress header adds makes the
    /// frame shorter, so its growth is 0: `saturating_sub` states that.
    pub(crate) const fn growth(self) -> usize {
        RELAYED_HEADER_LEN.saturating_sub(self.dropped())
    }

    /// The largest growth of the shapes from `from` to the end of
    /// [`RELAY_SHAPES`].
    const fn max_growth(from: usize) -> usize {
        if from >= RELAY_SHAPES.len() {
            return 0;
        }
        let here = RELAY_SHAPES[from].growth();
        let rest = Self::max_growth(from + 1);
        if here > rest { here } else { rest }
    }

    /// The length of the egress frame that the relay of an ingress frame of
    /// `ingress_len` bytes makes, or `None` when the frame is shorter than
    /// its fixed part.
    #[cfg(test)]
    pub(crate) fn egress_len(self, ingress_len: usize) -> Option<usize> {
        ingress_len
            .checked_sub(self.dropped())
            .and_then(|kept| kept.checked_add(RELAYED_HEADER_LEN))
    }

    /// The relayed payload of `ingress`, as the sealer builds it: the
    /// canonical id, then everything after the sealer-only header.
    #[cfg(test)]
    pub(crate) fn relay(self, ingress: &[u8]) -> Vec<u8> {
        let id_end = self.before_id + super::CANONICAL_ID_LEN;
        [
            &ingress[self.before_id..id_end],
            &ingress[id_end + self.after_id..],
        ]
        .concat()
    }

    /// The shape of `kind`.
    #[cfg(test)]
    pub(crate) fn of(kind: u8) -> Self {
        RELAY_SHAPES
            .into_iter()
            .find(|shape| shape.kind == kind)
            .expect("the sealer relays this kind")
    }
}
