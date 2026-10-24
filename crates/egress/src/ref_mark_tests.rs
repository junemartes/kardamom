use kardamom_cluster_adapter::wire::{encode_egress_boundary, encode_egress_record};

use super::RefMark;
use crate::test_support::ref_record;

#[test]
fn the_top_of_a_batch_names_the_block_of_each_reference() {
    let frames = [
        ref_record(4),
        encode_egress_boundary(3, 5, 0, 0),
        encode_egress_record(5, &[7; 40]).unwrap(),
        ref_record(6),
        encode_egress_boundary(4, 7, 0, 0),
    ];
    assert_eq!(
        RefMark::top(&frames, 3),
        Some(RefMark { index: 6, block: 4 })
    );
    assert_eq!(RefMark::top(&frames[1..3], 3), None, "no reference");
}

#[test]
fn a_written_mark_reads_back_and_a_short_file_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mark = RefMark { index: 9, block: 3 };
    mark.write(dir.path()).unwrap();
    assert_eq!(RefMark::read(dir.path()).unwrap(), mark);
    let bytes = std::fs::read(dir.path().join("ref-mark")).unwrap();
    kardamom_formats::Registry::assert_layout(
        "egress-ref-mark",
        "RefMark",
        &alloy_primitives::hex::encode(bytes),
    );
    std::fs::write(dir.path().join("ref-mark"), [1, 2, 3]).unwrap();
    assert!(RefMark::read(dir.path()).is_err());
}
