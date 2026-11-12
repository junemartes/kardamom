use super::AllocSpec;

/// An allocation of `group` in a job of three pinned executor groups.
/// Only group executor-0 runs the image `new`.
fn spec(group: &str) -> AllocSpec {
    let task = |image: &str| {
        serde_json::json!([
            {"Name":"log-shipper","Driver":"raw_exec","Config":{"command":"/bin/true"}},
            {"Name":"executor","Driver":"docker","Config":{"image":image,"args":["--recorder-id","0"]}}
        ])
    };
    serde_json::from_value(serde_json::json!({
        "ID":"0123456789ab","TaskGroup":group,"JobVersion":1,
        "Job":{"Version":1,"TaskGroups":[
            {"Name":"executor-0","Tasks":task("new")},
            {"Name":"executor-1","Tasks":task("old")},
            {"Name":"executor-2","Tasks":task("old")}
        ]}
    }))
    .unwrap()
}

#[test]
fn the_image_comes_from_the_group_of_the_allocation() {
    assert_eq!(spec("executor-0").image("executor").unwrap(), "new");
    assert_eq!(spec("executor-1").image("executor").unwrap(), "old");
    assert_eq!(spec("executor-2").image("executor").unwrap(), "old");
}

#[test]
fn a_task_without_an_image_or_a_missing_group_is_an_error() {
    assert!(spec("executor-1").image("log-shipper").is_err());
    assert!(spec("executor-1").image("validator").is_err());
    assert!(spec("executor").image("executor").is_err());
}
