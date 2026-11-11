use serde_json::json;

use super::*;

#[test]
fn the_short_window_preserves_l1_floors_and_the_saved_job() {
    let original = json!({"TaskGroups":[{"Count":3,"Tasks":[
        {"Name":"egress","Config":{"args":["--keep-blocks","43200",
        "--withdrawal-oracle","0x1234","--l1-rpc-url","http://l1:8545","--segment-blocks","1000"]}},
        {"Name":"other","Config":{"args":["--keep-blocks","999"]}}
    ]}]});
    let mut expected = original.clone();
    expected["TaskGroups"][0]["Tasks"][0]["Config"]["args"][1] = json!("64");
    expected["TaskGroups"][0]["Tasks"][0]["Config"]["args"][7] = json!(SEGMENT_BLOCKS.to_string());
    let job = WindowJob::new(&original, 64).unwrap();
    assert_eq!(job.definition(), &expected);
    assert_eq!(
        original["TaskGroups"][0]["Tasks"][0]["Config"]["args"][1],
        "43200"
    );
}

#[test]
fn missing_or_malformed_egress_arguments_fail_before_registration() {
    for task in [
        json!({"Name":"other"}),
        json!({"Name":"egress","Config":{"args":["--keep-blocks"]}}),
    ] {
        assert!(WindowJob::new(&json!({"TaskGroups":[{"Tasks":[task]}]}), 64).is_err());
    }
}
