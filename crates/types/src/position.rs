//! Position in Aeron's `tx_ordering` recording. This is the canonical L2
//! transaction identifier.

use rkyv::{Archive, Deserialize, Serialize};

/// Aeron's `term_id` is `i32`. `term_offset` is the byte offset within the
/// term. It is always non-negative, but uses `i32` to match Aeron's wire
/// format. Ordering is lexicographic on `(term_id, term_offset)`, so a
/// watermark comparison is a single `cmp` call.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Archive,
    Serialize,
    Deserialize,
)]
#[rkyv(derive(Debug), compare(PartialEq, PartialOrd))]
pub struct BPosition {
    pub term_id: i32,
    pub term_offset: i32,
}

impl BPosition {
    pub const ZERO: Self = Self {
        term_id: 0,
        term_offset: 0,
    };

    /// Encode a logical, always-increasing index into the two position words.
    ///
    /// This is not an Aeron byte position. It is the publisher-independent
    /// block-boundary alignment key carried by `BlockBoundaryStart.end_tx_idx`.
    /// The key is the total count of canonical tx_ordering records (TxRef and
    /// DepositRef) the sealer has republished through the end of a block. The
    /// executor compares it against its own processed-record count. Encoding
    /// it in `BPosition`, instead of adding a wire field, keeps the
    /// tx_ordering and tx_receipts message formats unchanged. `from_index(0)`
    /// equals `ZERO`, so existing zero-initializers still mean "no records
    /// yet". Aeron byte positions are fragile across a multi-publisher merge,
    /// because each publisher has its own term space, and across
    /// offer-return versus frame-start frames. This is why alignment uses
    /// this logical count instead.
    pub const fn from_index(idx: u64) -> Self {
        Self {
            term_id: (idx >> 32) as i32,
            term_offset: (idx & 0xFFFF_FFFF) as i32,
        }
    }

    /// Decode the logical index encoded by [`Self::from_index`].
    pub const fn as_index(self) -> u64 {
        ((self.term_id as u32 as u64) << 32) | (self.term_offset as u32 as u64)
    }
}

/// Location of a `TxEnvelope` fragment on a tx_data stream: the Aeron
/// publisher `session_id` plus the fragment-start [`BPosition`].
///
/// The session id tells apart concurrent ingress publishers on one shard.
/// Aeron positions are per session, so two active ingresses that publish to
/// the same tx_data stream can produce the same `(term_id, term_offset)`.
/// Pairing the position with `session_id` makes the executor's join key
/// `(shard_id, session_id, position)` unique. This is an in-process locator,
/// from the log to the sequencer or executor. Only `session_id` crosses the
/// wire, carried by [`crate::TxRef::tx_data_session_id`]. `BPosition` itself
/// stays unchanged.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TxDataLoc {
    pub session_id: i32,
    pub position: BPosition,
}

impl TxDataLoc {
    pub const fn new(session_id: i32, position: BPosition) -> Self {
        Self {
            session_id,
            position,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BPosition;

    #[test]
    fn index_round_trips() {
        for idx in [
            0u64,
            1,
            41,
            1_000_000,
            u32::MAX as u64,
            (u32::MAX as u64) + 1,
            u64::MAX,
        ] {
            assert_eq!(BPosition::from_index(idx).as_index(), idx, "idx={idx}");
        }
        // The zero index is the canonical zero position, used by existing initializers.
        assert_eq!(BPosition::from_index(0), BPosition::ZERO);
    }
}
