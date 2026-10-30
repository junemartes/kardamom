use super::{FreezeAttempt, Frozen, SPOOL_ROOT};

const CTX: &str = "batcher-outage-past-retention";

#[test]
fn a_freeze_attempt_holds_only_a_stopped_batcher_with_a_spool() {
    let held = Frozen::parse("T 12 00000000000000002037\n", "batcher-1".into(), CTX)
        .unwrap()
        .unwrap();
    assert_eq!(
        (held.inner.as_str(), held.blocks, held.last_block),
        ("batcher-1", 12, 2037)
    );
    assert!(
        Frozen::parse("T 0 0", "batcher-1".into(), CTX)
            .unwrap()
            .is_none()
    );
    assert!(
        Frozen::parse("gone 0 0", "batcher-1".into(), CTX)
            .unwrap()
            .is_none()
    );
    assert!(Frozen::parse("S 4 9", "batcher-1".into(), CTX).is_err());
    assert!(Frozen::parse("T 4", "batcher-1".into(), CTX).is_err());
    assert!(Frozen::parse("garbage", "batcher-1".into(), CTX).is_err());
}

/// The freeze holds only a spool with a block past the covered block. A
/// spool of only the batch in flight at the freeze ends at the covered
/// block once L1 confirms that batch, and the restart drops it.
#[test]
fn a_freeze_holds_only_a_spool_past_the_covered_block() {
    let frozen = Frozen {
        inner: "batcher-1".into(),
        blocks: 3,
        last_block: 2025,
    };
    assert!(frozen.holds_unposted(2024));
    assert!(!frozen.holds_unposted(2025));
    assert!(!frozen.holds_unposted(2030));
}

/// The freeze counts the block files of the spool that the batcher of
/// this build writes: `v<N>` under the spool root, with N from the
/// `batcher-spool` entry of the registry.
#[test]
fn the_freeze_reads_the_versioned_spool_directory() {
    let attempt = FreezeAttempt::new("aux").unwrap();
    let version = attempt
        .spool
        .strip_prefix(&format!("{SPOOL_ROOT}/v"))
        .expect("the spool directory is v<N> under the spool root");
    assert!(version.parse::<u32>().is_ok(), "{}", attempt.spool);
    assert!(attempt.script("batcher-1").contains(&attempt.spool));
}

/// The count command counts the block files of the versioned directory
/// only: not the spool root, and not a temporary file. A missing
/// directory counts 0.
#[test]
fn the_count_command_counts_the_block_files_of_the_spool() {
    let root = tempfile::tempdir().unwrap();
    let spool = root.path().join("v1");
    std::fs::create_dir_all(&spool).unwrap();
    for name in [
        "00000000000000000007.block",
        "00000000000000000008.block",
        "00000000000000000009.tmp",
    ] {
        std::fs::write(spool.join(name), b"x").unwrap();
    }
    std::fs::write(root.path().join("00000000000000000001.block"), b"x").unwrap();
    let count = |dir: &std::path::Path| {
        let attempt = FreezeAttempt {
            aux: "aux",
            spool: dir.display().to_string(),
        };
        let out = std::process::Command::new("sh")
            .arg("-c")
            .arg(attempt.count_command())
            .output()
            .unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    assert_eq!(count(&spool), "2 00000000000000000008");
    assert_eq!(count(&root.path().join("v9")), "0 0");
}
