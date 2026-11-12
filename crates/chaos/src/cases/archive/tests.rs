use super::*;

#[test]
fn a_mirror_segment_is_found_by_its_bytes() {
    let mirror =
        SegmentHashes::parse("aaa  0-0.rec\nzero  1-0.rec\nbbb  16-0.rec\nzero  2-0.rec\n");
    assert_eq!(
        mirror.name_for("aaa", "0-0.rec").as_deref(),
        Some("0-0.rec")
    );
    assert_eq!(
        mirror.name_for("bbb", "24-0.rec").as_deref(),
        Some("16-0.rec")
    );
    assert_eq!(
        mirror.name_for("zero", "2-0.rec").as_deref(),
        Some("2-0.rec")
    );
    assert_eq!(mirror.name_for("ccc", "24-0.rec"), None);
}

#[test]
fn a_clean_verify_has_an_ok_recording_and_tolerates_stale_entries() {
    let out = "recordingId=3 (checksum: Crc32) OK, length=4096\n\
        ERR: recordingId=4 invalid Catalog checksum\n";
    assert!(verify_is_clean(out));
    assert!(!verify_is_clean(
        "recordingId=3 ... OK\nERR: recordingId=5 file missing\n"
    ));
    assert!(!verify_is_clean(
        "Exception in thread main\nrecordingId=3 OK\n"
    ));
    assert!(!verify_is_clean(""));
}
