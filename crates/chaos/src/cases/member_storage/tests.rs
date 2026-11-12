use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

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
        "seed/old/snapshot.bin",
        "seed/.metadata",
        "archive/dir/catalog.dat",
        "archive/dir/19.rec",
        "archive/dir/archive-mark.dat",
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
    files[..7].iter().for_each(|file| {
        assert!(
            !root.path().join(file).exists(),
            "member file survives: {file}"
        );
    });
    files[7..].iter().for_each(|file| {
        assert_eq!(fs::read_to_string(root.path().join(file)).unwrap(), *file);
    });
    assert_eq!(fs::read_dir(root.path().join("seed")).unwrap().count(), 0);
}

#[test]
fn an_offline_copy_keeps_raft_bytes_and_neither_marks_nor_shared_archives() {
    let donor = tempfile::tempdir().unwrap();
    let victim = tempfile::tempdir().unwrap();
    let copied = [
        "cluster/recording.log",
        "cluster/nested/retained.rec",
        "archive/archive.catalog",
        "archive/17.rec",
    ];
    let excluded = [
        "cluster/cluster-mark.dat",
        "cluster/cluster-mark-service-0.dat",
        "archive/archive-mark.dat",
        "cluster/cluster-mark-service-0.lnk",
        "archive/dir/catalog.dat",
        "archive/dir/19.rec",
    ];
    copied.iter().chain(&excluded).for_each(|file| {
        let path = donor.path().join(file);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, file).unwrap();
    });
    fs::create_dir_all(victim.path().join("archive/dir")).unwrap();
    fs::write(
        victim.path().join("archive/dir/catalog.dat"),
        "victim executor",
    )
    .unwrap();
    let archive = Command::new("sh")
        .args(["-c", MemberStorage::COPY])
        .current_dir(donor.path())
        .output()
        .unwrap();
    assert!(archive.status.success());
    assert!(run_with_input(victim.path(), "tar -xf -", &archive.stdout));
    for file in &copied {
        assert_eq!(
            fs::read(victim.path().join(file)).unwrap(),
            fs::read(donor.path().join(file)).unwrap()
        );
    }
    excluded[..4]
        .iter()
        .for_each(|file| assert!(!victim.path().join(file).exists()));
    assert_eq!(
        fs::read_to_string(victim.path().join("archive/dir/catalog.dat")).unwrap(),
        "victim executor"
    );
    assert!(!victim.path().join("archive/dir/19.rec").exists());
    let manifest = Command::new("sh")
        .args(["-c", MemberStorage::MANIFEST])
        .current_dir(donor.path())
        .output()
        .unwrap();
    assert!(manifest.status.success());
    assert_eq!(
        String::from_utf8_lossy(&manifest.stdout).lines().count(),
        copied.len()
    );
    assert!(run_with_input(
        victim.path(),
        "sha256sum --quiet -c -",
        &manifest.stdout
    ));
    fs::write(victim.path().join("archive/17.rec"), "corrupted").unwrap();
    assert!(!run_with_input(
        victim.path(),
        "sha256sum --quiet -c -",
        &manifest.stdout
    ));
}

fn run_with_input(root: &Path, script: &str, input: &[u8]) -> bool {
    let mut child = Command::new("sh")
        .args(["-c", script])
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait().unwrap().success()
}
