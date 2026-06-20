//! Position in Aeron tx_ordering's recording — the canonical L2 tx identifier.

use rkyv::{Archive, Deserialize, Serialize};

/// Aeron's `term_id` is `i32`; `term_offset` is the byte offset within the term
/// and is always non-negative but typed `i32` to match Aeron's wire format.
/// Ordering is `(term_id, term_offset)` lexicographic so watermark comparisons
/// are a single cmp.
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

    /// Encode a logical monotone index into the two position words.
    ///
    /// This is **not** an Aeron byte position — it is the
    /// publisher-independent block-boundary alignment key carried by
    /// `BlockBoundaryStart.end_tx_idx`: the cumulative count of canonical
    /// tx_ordering records (TxRef + DepositRef) the sealer has republished
    /// through the end of a block. The executor compares it against its own
    /// processed-record count. Encoding it in `BPosition` (rather than adding
    /// a wire field) keeps the tx_ordering / tx_receipts message formats
    /// unchanged; `from_index(0) == ZERO`, so existing zero-initialisers map to
    /// "no records yet". Aeron byte positions are fragile across a
    /// multi-publisher merge (per-publication term spaces) and across the
    /// offer-return vs frame-start frames, which is exactly why alignment uses
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
        // The zero index is the canonical zero position (existing initialisers).
        assert_eq!(BPosition::from_index(0), BPosition::ZERO);
    }
}
