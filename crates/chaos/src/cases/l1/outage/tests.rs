use super::{AfterThaw, AtFreeze, FreezeAttempt, Frozen, Lines, SPOOL_ROOT};
use crate::l1::Posted;

const CTX: &str = "batcher-outage-past-retention";

fn batch(index: u64, start: u64, end: u64) -> Posted {
    Posted {
        index,
        l2_block_start: start,
        l2_block_end: end,
    }
}

fn at_freeze(covered: u64) -> AtFreeze {
    AtFreeze {
        covered,
        lines: Lines {
            starts: 3,
            restored: 1,
            exits: 2,
        },
    }
}

#[test]
fn the_recovered_batch_is_the_first_past_the_covered_block_not_the_last() {
    let at = at_freeze(1747);
    let posted = [
        batch(40, 1700, 1747),
        batch(41, 1748, 1760),
        batch(42, 1761, 1769),
    ];
    assert_eq!(at.recovered_batch(&posted), Some(batch(41, 1748, 1760)));
    assert_eq!(at.recovered_batch(&posted[..1]), None);
    assert_eq!(at.recovered_batch(&[]), None);
}

/// The log read after the thaw, with the counts `lines`, and no post of
/// the frozen group by the old process.
fn from_spool(lines: Lines) -> AfterThaw {
    AfterThaw {
        lines,
        old_posted: false,
    }
}

#[test]
fn a_recovered_batch_with_a_gap_or_an_overlap_fails() {
    let at = at_freeze(1747);
    let gap = at
        .judge(batch(41, 1749, 1760), from_spool(at.lines), CTX)
        .unwrap_err();
    assert!(
        gap.to_string()
            .contains("the recovered batch 41 starts at 1749, not at 1748"),
        "{gap}"
    );
    let overlap = at
        .judge(batch(41, 1747, 1760), from_spool(at.lines), CTX)
        .unwrap_err();
    assert!(overlap.to_string().contains("starts at 1747"), "{overlap}");
}

#[test]
fn only_a_restart_that_restores_the_spool_passes() {
    let at = at_freeze(1747);
    let recovered = batch(41, 1748, 1760);
    let restored = Lines {
        starts: 4,
        restored: 2,
        exits: 3,
    };
    at.judge(recovered, from_spool(restored), CTX).unwrap();
    let no_timeout = at.judge(recovered, from_spool(at.lines), CTX).unwrap_err();
    assert!(
        no_timeout
            .to_string()
            .contains("no Aeron client timeout fired"),
        "{no_timeout}"
    );
    let hung = Lines {
        exits: 3,
        ..at.lines
    };
    let hung = at.judge(recovered, from_spool(hung), CTX).unwrap_err();
    assert!(
        hung.to_string().contains("the process hung in its exit"),
        "{hung}"
    );
    let lost = Lines {
        starts: 4,
        restored: 1,
        exits: 3,
    };
    let error = at.judge(recovered, from_spool(lost), CTX).unwrap_err();
    assert!(
        error.to_string().contains("its spool was not restored"),
        "{error}"
    );
    let posted_by_old = AfterThaw {
        lines: lost,
        old_posted: true,
    };
    at.judge(recovered, posted_by_old, CTX).unwrap();
}

/// The old process posts the frozen group between the thaw and its
/// exit: its confirmed post of the block after the covered block comes
/// before the last start line. A post by the new process, a post of
/// another block, and a log with no restart do not count.
#[test]
fn only_a_post_of_the_next_block_before_the_last_start_is_by_the_old_process() {
    let at = at_freeze(1914);
    let start = |t: &str| {
        format!(
            "{t}  INFO kardamom_batcher::live::run: live batcher starting covered_through_block=1\n"
        )
    };
    let post = |t: &str, first: u64| {
        format!(
            "{t}  INFO kardamom_batcher::live::sender: batch confirmed on L1 batch_index=9 \x1b[3ml2_block_start\x1b[0m\x1b[2m=\x1b[0m{first} l2_block_end=1922\n"
        )
    };
    let first = start("2026-10-18T22:42:21.490016Z");
    let old = post("2026-10-18T23:33:41.127346Z", 1915);
    let restart = start("2026-10-18T23:33:47.528715Z");
    assert!(at.old_process_posted(&format!("{first}{old}{restart}")));
    // Another allocation's log comes first in the concatenation.
    assert!(at.old_process_posted(&format!("{restart}{first}{old}")));
    assert!(!at.old_process_posted(&format!("{first}{old}")));
    let new = post("2026-10-18T23:33:50.000000Z", 1915);
    assert!(!at.old_process_posted(&format!("{first}{restart}{new}")));
    let other = post("2026-10-18T23:33:41.127346Z", 19150);
    assert!(!at.old_process_posted(&format!("{first}{other}{restart}")));
}

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
