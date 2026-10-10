//! The frozen spool and cursor layouts against the format registry
//! (`formats.toml` at the workspace root).

use kardamom_formats::Registry;
use kardamom_types::xchain::RemoteEpochRecord;

use super::BatchCursor;
use super::spool::SPOOL_VERSION;
use crate::batch::{ClosedBlock, RecordedTx};

#[test]
fn the_spool_layout_matches_the_registry() {
    Registry::assert_exact("batcher-spool", SPOOL_VERSION);
    Registry::assert_rkyv_layout::<ClosedBlock>("batcher-spool", "ClosedBlock");
    Registry::assert_rkyv_layout::<RecordedTx>("batcher-spool", "RecordedTx");
    Registry::assert_rkyv_layout::<RemoteEpochRecord>("batcher-spool", "RemoteEpochRecord");
}

#[test]
fn the_cursor_layout_matches_the_registry() {
    let cursor = BatchCursor {
        next_index: 1,
        next_block: 2,
        last_batch_index: 3,
    };
    let json = serde_json::to_string(&cursor).unwrap();
    Registry::assert_layout("batcher-cursor", "BatchCursor", &json);
}

/// A cursor of a later release carries a field this release does not
/// know. The reader ignores it, so a rollback reads the cursor.
#[test]
fn a_cursor_with_an_unknown_field_parses() {
    let later = r#"{"next_index":1,"next_block":2,"last_batch_index":3,"later_field":4}"#;
    let cursor: BatchCursor = serde_json::from_str(later).unwrap();
    assert_eq!(
        cursor,
        BatchCursor {
            next_index: 1,
            next_block: 2,
            last_batch_index: 3,
        }
    );
    let short = r#"{"next_index":1,"next_block":2}"#;
    assert!(
        serde_json::from_str::<BatchCursor>(short).is_err(),
        "a required field is missing"
    );
}
