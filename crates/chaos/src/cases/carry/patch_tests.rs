use serde_json::json;

use super::{Edit, JobPatch};

fn job() -> serde_json::Value {
    json!({
        "ID": "executor",
        "Meta": {"relayed_full_ref": "true"},
        "TaskGroups": [
            {"Name": "executor-0", "Tasks": [{"Config": {"args": ["--metrics-addr", "0.0.0.0:9004"]}}]},
            {"Name": "executor-1", "Tasks": [{"Config": {"args": ["--metrics-addr", "0.0.0.0:9004", "--lockbox", "0x00"]}}]},
        ]
    })
}

fn args(definition: &serde_json::Value, group: usize) -> Vec<&str> {
    definition["TaskGroups"][group]["Tasks"][0]["Config"]["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap())
        .collect()
}

#[test]
fn a_group_patch_appends_once_and_leaves_the_other_groups() {
    let patch = JobPatch::new("executor")
        .in_group("executor-0".into())
        .edit(Edit::Append {
            marker: "--metrics-addr",
            args: vec!["--carry-bytes".into(), "--egress".into(), "http://e".into()],
        });
    let mut definition = job();
    patch.apply_to(&mut definition).unwrap();
    patch.apply_to(&mut definition).unwrap();
    assert_eq!(
        args(&definition, 0),
        [
            "--metrics-addr",
            "0.0.0.0:9004",
            "--carry-bytes",
            "--egress",
            "http://e"
        ]
    );
    assert_eq!(args(&definition, 1), args(&job(), 1));
}

#[test]
fn a_value_patch_sets_the_value_after_its_flag_and_the_meta() {
    let patch = JobPatch::new("executor")
        .meta("reads_egress")
        .edit(Edit::Value {
            flag: "--lockbox",
            value: "0xab".into(),
        });
    let mut definition = job();
    patch.apply_to(&mut definition).unwrap();
    assert_eq!(args(&definition, 1)[3], "0xab");
    assert_eq!(definition["Meta"]["reads_egress"], "true");
    assert_eq!(definition["Meta"]["relayed_full_ref"], "true");
}

#[test]
fn an_edit_that_selects_no_task_fails() {
    let patch = JobPatch::new("executor")
        .in_group("executor-0".into())
        .edit(Edit::Value {
            flag: "--lockbox",
            value: "0xab".into(),
        });
    let err = patch.apply_to(&mut job()).unwrap_err().to_string();
    assert!(err.contains("no task"), "{err}");
}
