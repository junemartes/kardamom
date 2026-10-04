use super::VerdictFile;

#[test]
fn no_file_means_no_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let file = VerdictFile::beside(dir.path());
    assert_eq!(file.standing().unwrap(), None);
    assert!(!file.clear().unwrap());
}

#[test]
fn record_then_standing_then_clear() {
    let dir = tempfile::tempdir().unwrap();
    let file = VerdictFile::beside(dir.path().join("state").as_path());
    file.record("write-set mismatch at block 7").unwrap();
    assert_eq!(
        file.standing().unwrap().as_deref(),
        Some("write-set mismatch at block 7")
    );
    assert_eq!(file.path(), dir.path().join("state").join("verdict"));
    assert!(file.clear().unwrap());
    assert_eq!(file.standing().unwrap(), None);
}

#[test]
fn a_second_record_replaces_the_first() {
    let dir = tempfile::tempdir().unwrap();
    let file = VerdictFile::beside(dir.path());
    file.record("first").unwrap();
    file.record("second").unwrap();
    assert_eq!(file.standing().unwrap().as_deref(), Some("second"));
    assert!(!dir.path().join("verdict.tmp").exists());
}
