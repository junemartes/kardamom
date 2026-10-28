//! Actual archive recording, offline corruption/wipe, repair and exact replay.

#![cfg(all(feature = "docker-e2e", target_os = "linux"))]

#[path = "archive_recovery/support.rs"]
mod support;

use std::time::Instant;

use kardamom_batcher::rereplicate::{diff_mirror, heal_from_mirror, mirror_archive};

use support::Fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires one owned Docker archive; run the archive-recovery component target"]
async fn a_corrupt_recording_is_detected_repaired_and_replayed_exactly() {
    let started = Instant::now();
    let mut fixture = Fixture::recorded().await;
    fixture.stop().await;
    let mirror = tempfile::tempdir().unwrap();
    let report = mirror_archive(fixture.archive(), mirror.path()).unwrap();
    assert_eq!(report.segments_copied, 1);
    assert!(report.catalog_copied);
    assert!(!mirror.path().join("archive-mark.dat").exists());
    fixture.verify_clean().await;
    let segment = fixture.segment();
    let baseline = std::fs::read(fixture.archive().join(&segment)).unwrap();
    assert_eq!(baseline.len(), 65_536);
    fixture.corrupt(&segment);
    let corrupted = std::fs::read(fixture.archive().join(&segment)).unwrap();
    assert_eq!(baseline.len(), corrupted.len());
    assert_ne!(baseline, corrupted);
    fixture.verify_corrupt().await;
    let diff = diff_mirror(mirror.path(), fixture.archive()).unwrap();
    assert_eq!(diff.diverged, vec![segment.clone()]);
    let catalog = std::fs::read(fixture.archive().join("archive.catalog")).unwrap();
    let repair = heal_from_mirror(
        mirror.path(),
        fixture.archive(),
        std::slice::from_ref(&segment),
    )
    .unwrap();
    assert_eq!(repair.segments_healed, 1);
    assert_eq!(repair.bytes_copied, 65_536);
    assert!(
        std::fs::read(fixture.archive().join(&segment)).unwrap() == baseline,
        "repair restores the exact segment bytes"
    );
    assert_eq!(
        std::fs::read(fixture.archive().join("archive.catalog")).unwrap(),
        catalog
    );
    fixture.mark_valid().await;
    fixture.verify_clean().await;
    fixture.restart_and_replay().await;
    println!(
        "ARCHIVE_RECOVERY corruption seconds={:.3}",
        started.elapsed().as_secs_f64()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires one owned Docker archive; run the archive-recovery component target"]
async fn a_wiped_archive_restores_its_catalog_and_replays_the_exact_recording() {
    let started = Instant::now();
    let mut fixture = Fixture::recorded().await;
    fixture.stop().await;
    let mirror = tempfile::tempdir().unwrap();
    mirror_archive(fixture.archive(), mirror.path()).unwrap();
    let segment = fixture.segment();
    let expected = std::fs::read(mirror.path().join(&segment)).unwrap();
    std::fs::remove_file(fixture.archive().join(&segment)).unwrap();
    std::fs::remove_file(fixture.archive().join("archive.catalog")).unwrap();
    assert!(!fixture.archive().join(&segment).exists());
    assert!(!fixture.archive().join("archive.catalog").exists());
    let repair = mirror_archive(mirror.path(), fixture.archive()).unwrap();
    assert_eq!(repair.segments_copied, 1);
    assert!(repair.catalog_copied);
    assert!(
        std::fs::read(fixture.archive().join(&segment)).unwrap() == expected,
        "restore recovers the exact segment bytes"
    );
    fixture.verify_clean().await;
    fixture.restart_and_replay().await;
    println!(
        "ARCHIVE_RECOVERY wipe seconds={:.3}",
        started.elapsed().as_secs_f64()
    );
}
