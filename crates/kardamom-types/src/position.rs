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
}
