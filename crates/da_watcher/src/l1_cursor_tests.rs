use alloy_primitives::B256;

use super::L1Cursor;

/// The frozen cursor line against the format registry (`formats.toml` at
/// the workspace root).
#[test]
fn the_cursor_layout_matches_the_registry() {
    let cursor = L1Cursor {
        number: 7,
        hash: B256::repeat_byte(0x07),
    };
    let line = cursor.to_string();
    kardamom_formats::Registry::assert_layout("l1-cursor", "L1Cursor", &line);
    assert_eq!(line.parse::<L1Cursor>(), Ok(cursor));
}
