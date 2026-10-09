use alloy_primitives::B256;

use super::{L1Cursor, L1CursorError};

fn sample() -> L1Cursor {
    L1Cursor {
        number: 7,
        hash: B256::repeat_byte(0x07),
    }
}

/// The frozen cursor line against the format registry (`formats.toml` at
/// the workspace root).
#[test]
fn the_cursor_layout_matches_the_registry() {
    let cursor = sample();
    let line = cursor.to_string();
    kardamom_formats::Registry::assert_layout("l1-cursor", "L1Cursor", &line);
    assert_eq!(line.parse::<L1Cursor>(), Ok(cursor));
}

/// A later release can add a field after the hash. This release reads
/// the two fields it knows and ignores the rest.
#[test]
fn a_field_after_the_hash_is_ignored() {
    let later = format!("{} extra 42", sample());
    assert_eq!(later.parse::<L1Cursor>(), Ok(sample()));
}

/// A missing or malformed required field is still a refusal: the watcher
/// never guesses a start.
#[test]
fn a_missing_or_malformed_required_field_is_refused() {
    assert_eq!("7".parse::<L1Cursor>(), Err(L1CursorError::Fields(1)));
    assert_eq!("".parse::<L1Cursor>(), Err(L1CursorError::Fields(0)));
    assert!(matches!(
        format!("seven {:#x}", B256::ZERO).parse::<L1Cursor>(),
        Err(L1CursorError::Number(_))
    ));
    assert!(matches!(
        "7 0xnothex".parse::<L1Cursor>(),
        Err(L1CursorError::Hash(_))
    ));
}
