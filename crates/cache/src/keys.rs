//! The key layout, in one place.
//!
//! | Key | Value | Writer |
//! |---|---|---|
//! | `acct:<addr>` | hash `{nonce, balance, tx_idx}` | mirror |
//! | `rcpt:<addr>:<nonce>` | rkyv `Receipt` | mirror |
//! | `head:<mirror_id>` | position tag | that mirror |
//! | `pending:<addr>` | u64 optimistic floor | sequencer |
//!
//! A position tag is the canonical index (`BPosition::as_index`) as a
//! zero-padded 20-digit decimal, so Redis and Lua compare tags as strings
//! and never lose precision: a Lua number is a double, and an index can
//! exceed 2^53.

use alloy_primitives::Address;
use kardamom_types::BPosition;

/// The width of a position tag. `u64::MAX` has 20 digits.
const TAG_WIDTH: usize = 20;

/// The account hash of `address`.
#[must_use]
pub fn account(address: Address) -> String {
    format!("acct:{address}")
}

/// The receipt index entry of `(sender, nonce)`.
#[must_use]
pub fn receipt(sender: Address, nonce: u64) -> String {
    format!("rcpt:{sender}:{nonce}")
}

/// The liveness head of one mirror. The mirror ids are the executor
/// indexes.
#[must_use]
pub fn head(mirror_id: u32) -> String {
    format!("head:{mirror_id}")
}

/// The sequencer's optimistic floor of `address`. Informational only.
#[must_use]
pub fn pending(address: Address) -> String {
    format!("pending:{address}")
}

/// A position as a fixed-width tag.
#[must_use]
pub fn position_tag(position: BPosition) -> String {
    index_tag(position.as_index())
}

/// A canonical index as a fixed-width tag.
#[must_use]
pub fn index_tag(index: u64) -> String {
    format!("{index:0TAG_WIDTH$}")
}

/// Parse a tag written by [`index_tag`]. `None` for anything else.
#[must_use]
pub fn parse_tag(tag: &str) -> Option<u64> {
    (tag.len() == TAG_WIDTH).then(|| tag.parse().ok()).flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tags_order_as_strings_and_round_trip() {
        let small = index_tag(9);
        let large = index_tag(1 << 60);
        assert!(small < large);
        assert_eq!(parse_tag(&large), Some(1 << 60));
        assert_eq!(parse_tag("9"), None);
        assert_eq!(parse_tag(&index_tag(u64::MAX)), Some(u64::MAX));
    }

    #[test]
    fn keys_carry_the_checksummed_address() {
        let a = Address::repeat_byte(0xab);
        assert_eq!(account(a), format!("acct:{a}"));
        assert_eq!(receipt(a, 7), format!("rcpt:{a}:7"));
        assert_eq!(head(2), "head:2");
        assert_eq!(pending(a), format!("pending:{a}"));
    }
}
