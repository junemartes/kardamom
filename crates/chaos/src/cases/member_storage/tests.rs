use std::fs;
use std::process::Command;

use super::MemberStorage;

#[test]
fn blank_member_keeps_the_shared_driver_archive_and_other_services() {
    let root = tempfile::tempdir().unwrap();
    let files = [
        "cluster/member/snapshot.rec",
        "cluster/.metadata",
        "archive/17.rec",
        "archive/.catalog",
        "archive/member/snapshot.rec",
        "archive/dir/catalog.dat",
        "archive/dir/19.rec",
        "state/mdbx.dat",
        "egress/frames.log",
    ];
    for file in &files {
        let path = root.path().join(file);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, file).unwrap();
    }
    let status = Command::new("sh")
        .args(["-c", MemberStorage::WIPE])
        .current_dir(root.path())
        .status()
        .unwrap();
    assert!(status.success());
    assert_eq!(
        fs::read_dir(root.path().join("cluster")).unwrap().count(),
        0
    );
    files[..5].iter().for_each(|file| {
        assert!(
            !root.path().join(file).exists(),
            "member file survives: {file}"
        );
    });
    files[5..].iter().for_each(|file| {
        assert_eq!(fs::read_to_string(root.path().join(file)).unwrap(), *file);
    });
    assert!(root.path().join("seed").is_dir());
}
