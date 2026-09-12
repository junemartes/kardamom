//! Aeron term-position decode/encode, shared by `refetch.rs` (archive
//! replay positions) and `aeron_live` (live offer-return positions).
//!
//! Aeron packs a stream position as
//! `((term_id - initial_term_id) << bits) + term_offset`, where
//! `bits = log2(term_buffer_length)`. [`TermLayout`] parses
//! `(term_buffer_length, initial_term_id)` once, so `bits` is never
//! re-derived per call, and a non-power-of-two term length cannot reach
//! the shift arithmetic.

use kardamom_types::BPosition;

use crate::error::LogError;

/// A recording's or a live publication's power-of-two term length and
/// initial term id, checked once at construction.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TermLayout {
    /// `log2(term_buffer_length)`.
    bits: u32,
    initial_term_id: i32,
}

impl TermLayout {
    /// Validate `term_buffer_length` and build the layout.
    ///
    /// # Errors
    ///
    /// Returns an error message if `term_buffer_length` is not a positive
    /// power of two.
    pub(crate) fn new(term_buffer_length: i32, initial_term_id: i32) -> Result<Self, String> {
        let term_len = i64::from(term_buffer_length);
        if term_len <= 0 || (term_len & (term_len - 1)) != 0 {
            return Err(format!("non-power-of-two term length {term_len}"));
        }
        Ok(Self {
            bits: term_len.trailing_zeros(),
            initial_term_id,
        })
    }

    /// Build from a live publication's own constants
    /// (`position_bits_to_shift`, `initial_term_id`, `term_buffer_length`),
    /// so the offer-decode path never assumes a fixed term length.
    ///
    /// # Errors
    ///
    /// Returns an error if reading the publication's constants fails, or
    /// if `term_buffer_length` does not fit `i32` or is not a positive
    /// power of two.
    pub(crate) fn from_publication(
        publication: &rusteron_client::AeronPublication,
    ) -> Result<Self, LogError> {
        let constants = publication
            .get_constants()
            .map_err(|e| LogError::Aeron(format!("publication constants: {e}")))?;
        let term_buffer_length = i32::try_from(constants.term_buffer_length()).map_err(|_| {
            LogError::Aeron(format!(
                "publication term_buffer_length overflow: {}",
                constants.term_buffer_length()
            ))
        })?;
        Self::new(term_buffer_length, constants.initial_term_id())
            .map_err(|e| LogError::Aeron(format!("publication term layout: {e}")))
    }

    /// Raw stream position of `pos` within this term layout.
    ///
    /// # Errors
    ///
    /// Returns an error message if `pos`'s term precedes `recording_id`'s
    /// initial term.
    pub(crate) fn position_of(self, pos: BPosition, recording_id: i64) -> Result<i64, String> {
        let term_count = i64::from(pos.term_id) - i64::from(self.initial_term_id);
        if term_count < 0 {
            return Err(format!(
                "position term {} precedes recording {recording_id}'s initial term {}",
                pos.term_id, self.initial_term_id
            ));
        }
        Ok((term_count << self.bits) + i64::from(pos.term_offset))
    }

    /// Decode a raw stream position (Aeron's `offer` return, or a
    /// recording's raw position) into a [`BPosition`]. Errors instead of
    /// wrapping when `p` does not fit `i32` after the term split: at a 16
    /// MiB term (`bits = 24`), that needs a position past 8 exbibytes on
    /// one stream, so this is a defensive bound, not a path any real
    /// deployment reaches.
    ///
    /// # Errors
    ///
    /// Returns an error message if the decoded `term_offset` or `term_id`
    /// does not fit `i32`.
    pub(crate) fn decode(self, p: i64) -> Result<BPosition, String> {
        let mask = (1i64 << self.bits) - 1;
        let term_offset = i32::try_from(p & mask)
            .map_err(|_| format!("decode_position: term_offset overflow for position {p}"))?;
        let term_id = i32::try_from(p >> self.bits)
            .ok()
            .and_then(|term_count| term_count.checked_add(self.initial_term_id))
            .ok_or_else(|| format!("decode_position: term_id overflow for position {p}"))?;
        Ok(BPosition {
            term_id,
            term_offset,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip `decode(position_of(pos)) == pos`, for a plain layout, a
    /// nonzero `initial_term_id` (the common case: Aeron picks a random
    /// initial term id per publication), and a position in a term past
    /// the first.
    #[test]
    fn decode_inverts_position_of() {
        let cases = [
            // (term_buffer_length, initial_term_id, term_id, term_offset)
            (64 * 1024, 0, 0, 0),
            (64 * 1024, 0, 0, 4096),
            (16 * 1024 * 1024, 917_234, 917_234, 12_345),
            (16 * 1024 * 1024, 917_234, 917_237, 0),
            (16 * 1024 * 1024, -5, 3, 999),
        ];
        for (term_buffer_length, initial_term_id, term_id, term_offset) in cases {
            let layout = TermLayout::new(term_buffer_length, initial_term_id)
                .expect("valid power-of-two term length");
            let pos = BPosition {
                term_id,
                term_offset,
            };
            let raw = layout
                .position_of(pos, 1)
                .expect("term_id at or after initial_term_id");
            let decoded = layout.decode(raw).expect("position fits i32 after shift");
            assert_eq!(decoded, pos, "layout {layout:?}, raw position {raw}");
        }
    }

    #[test]
    fn new_rejects_non_power_of_two_term_length() {
        assert!(TermLayout::new(0, 0).is_err());
        assert!(TermLayout::new(-16, 0).is_err());
        assert!(TermLayout::new(3, 0).is_err(), "3 is not a power of two");
    }
}
