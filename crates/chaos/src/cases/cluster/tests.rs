use super::*;

#[test]
fn the_catchup_block_counts_only_lines_after_the_last_fresh_start() {
    let logs = "sealer snapshot TAKEN memberId=1 block=90\n\
            sealer state FRESH at genesis memberId=1\n\
            sealer snapshot TAKEN memberId=1 block=30\n\
            sealer snapshot TAKEN memberId=2 block=120\n\
            sealer snapshot TAKEN memberId=1 block=60\n";
    assert_eq!(BlankStart::of(1).catchup_block(logs, 1), 60);
    assert_eq!(BlankStart::of(2).catchup_block(logs, 2), 0);
    let mut track = Catchup::default();
    track.observe(0, false);
    track.observe(30, true);
    assert!(!track.moved);
    assert!(track.blank);
    track.observe(60, true);
    assert!(track.moved);
}

#[test]
fn the_catchup_block_counts_only_lines_after_the_last_seed() {
    let logs = "sealer state FRESH at genesis memberId=1\n\
            sealer snapshot TAKEN memberId=1 block=90\n\
            cluster SEED from-peer memberId=1 snapshotPosition=4096 round=1\n\
            sealer snapshot RESTORED memberId=1 block=80 canonicalCount=7\n\
            sealer snapshot TAKEN memberId=1 block=95\n";
    assert_eq!(BlankStart::of(1).catchup_block(logs, 1), 95);
    let start = BlankStart::of(1);
    assert!(start.matches("cluster SEED from-peer memberId=1 snapshotPosition=-1 round=2"));
    assert!(!start.matches("cluster SEED from-peer memberId=10 snapshotPosition=-1 round=2"));
    assert!(!start.matches("cluster SEED waiting-for-peer memberId=1 round=1"));
}

#[test]
fn an_offline_copy_needs_a_new_resume_and_a_post_resume_snapshot() {
    let start = BlankStart::copied(1);
    let old =
        "cluster START mode=RESUME memberId=1 old\nsealer snapshot TAKEN memberId=1 block=800\n";
    let current = format!(
        "{old}cluster START mode=RESUME memberId=1 new\nsealer snapshot RESTORED memberId=1 block=750\n"
    );
    assert_eq!(start.catchup_block(&current, 1), 0);
    let caught_up = format!("{current}sealer snapshot TAKEN memberId=1 block=810\n");
    assert_eq!(start.catchup_block(&caught_up, 1), 810);
    assert!(!start.matches("cluster START mode=RESUME memberId=10 new"));
    assert!(!start.matches("cluster SEED from-peer memberId=1 snapshotPosition=99"));
    assert_eq!(BlankStart::of(1).catchup_block(&caught_up, 1), 0);
}
#[test]
fn only_a_new_start_in_the_same_allocation_certifies_a_snapshot() {
    let before = HashMap::from([("old".to_string(), 1)]);
    let observed = |old_count, new_count, new_block| {
        StartLogs(HashMap::from([
            (
                "old".to_string(),
                StartSample {
                    count: old_count,
                    block: 999,
                },
            ),
            (
                "new".to_string(),
                StartSample {
                    count: new_count,
                    block: new_block,
                },
            ),
        ]))
    };
    assert_eq!(observed(1, 0, 888).new_block(&before), None);
    assert_eq!(observed(1, 1, 0).new_block(&before), Some(0));
    assert_eq!(observed(1, 1, 810).new_block(&before), Some(810));
    assert_eq!(observed(2, 0, 888).new_block(&before), Some(999));
}
