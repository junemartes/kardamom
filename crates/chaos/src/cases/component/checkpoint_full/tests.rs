use super::*;
use crate::cases::component::checkpoint_full::proof::Progress;

#[test]
fn only_the_victim_checkpoint_source_changes() {
    let task = serde_json::json!({
        "Name":"executor", "Env":{"KARDAMOM_CHECKPOINT_LOG":"true"},
        "Config":{"image":"pinned", "readonly_rootfs":true,
            "args":["--checkpoint-interval-secs","20"],
            "volumes":["/opt/kardamom/state:/opt/kardamom/state",
                "/opt/kardamom/checkpoints:/opt/kardamom/checkpoints",
                "/opt/kardamom/aeron-mount:/opt/kardamom/aeron-mount"]}
    });
    let saved = serde_json::json!({"TaskGroups":[
        {"Name":"executor-0","Count":1,"Tasks":[task.clone(), {"Name":"recorder"}]},
        {"Name":"executor-1","Count":1,"Tasks":[task.clone()]},
        {"Name":"executor-2","Count":1,"Tasks":[task]}
    ]});
    let volume = Volume { token: 123 };
    let mut expected = saved.clone();
    expected["TaskGroups"][0]["Tasks"][0]["Config"]["volumes"][1] =
        format!("{}:{ORIGINAL}", volume.root()).into();
    assert_eq!(volume.job(&saved).unwrap(), expected);
    assert!(
        volume.job(&expected).is_err(),
        "cannot stack a fault over another volume"
    );
    let mut no_log = saved.clone();
    no_log["TaskGroups"][0]["Tasks"][0]["Env"]["KARDAMOM_CHECKPOINT_LOG"] = "false".into();
    assert!(volume.job(&no_log).is_err());
    let mut replicated = saved;
    replicated["TaskGroups"][0]["Count"] = 3.into();
    assert!(volume.job(&replicated).is_err());
}

#[test]
fn enospc_proof_needs_both_failures_after_baseline_and_victim_transaction_progress() {
    let metrics = "kardamom_executor_block_number 10\nkardamom_executor_tx_applied_total 20\n";
    let before = Progress::read(metrics, "").unwrap();
    let log = "error=No space left on device (os error 28) the block log failed; the next batch starts a new segment\n";
    let checkpoint = "error=No space left on device checkpoint base round failed\n";
    let both = format!("{log}{checkpoint}");
    let failed_metrics =
        format!("{metrics}kardamom_checkpoint_log_failed_total{{step=\"append\"}} 1\n");
    let failed = Progress::read(&failed_metrics, &both).unwrap();
    assert!(failed.failed_after(&before));
    assert!(
        !failed.failed_after(&failed),
        "old logs cannot prove this fault"
    );
    assert!(!failed.advanced_after(&before));
    for incomplete in [
        log.to_owned(),
        checkpoint.to_owned(),
        both.replace("No space left on device", "permission denied")
            .replace("os error 28", "os error 13"),
    ] {
        assert!(
            !Progress::read(&failed_metrics, &incomplete)
                .unwrap()
                .failed_after(&before)
        );
    }
    assert!(
        !Progress::read(metrics, &both)
            .unwrap()
            .failed_after(&before)
    );
    assert!(Progress::read("", &both).is_err());
    let moved = Progress::read(
        &failed_metrics
            .replace("number 10", "number 11")
            .replace("total 20", "total 21"),
        &both,
    )
    .unwrap();
    assert!(moved.advanced_after(&failed));
    assert!(
        !moved.continued_during(&failed),
        "progress after an old failure does not prove the fault persists"
    );
    let continued = Progress::read(
        &failed_metrics
            .replace("number 10", "number 11")
            .replace("total 20", "total 21")
            .replace("append\"} 1", "append\"} 2"),
        &format!("{both}{log}"),
    )
    .unwrap();
    assert!(continued.continued_during(&failed));
    assert!(
        !Progress::read(&failed_metrics.replace("number 10", "number 11"), &both)
            .unwrap()
            .advanced_after(&failed)
    );
}

#[test]
fn exact_fault_script_refuses_unbounded_storage_and_requires_enospc() {
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), SCRIPT).unwrap();
    let output = std::process::Command::new("python3")
        .arg("-c")
        .arg(include_str!("test_volume.py"))
        .arg(source.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
