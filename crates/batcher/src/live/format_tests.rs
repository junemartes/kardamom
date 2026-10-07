//! The frozen spool and cursor layouts against the format registry
//! (`formats.toml` at the workspace root).

use kardamom_formats::Registry;
use kardamom_types::xchain::RemoteEpochRecord;

use super::BatchCursor;
use crate::batch::{ClosedBlock, RecordedTx};

#[test]
fn the_spool_layout_matches_the_registry() {
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
