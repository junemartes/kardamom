use alloy_primitives::{Address, B256};
use kardamom_reconstruct::{SealerSeed, SeedSender};
use serde_json::json;

use super::*;

/// A seed as `kardamom-reconstruct --sealer-seed` writes it.
fn seed_bytes() -> Vec<u8> {
    SealerSeed {
        chain_id: 412_346,
        block: 239,
        end_tx_idx: 5_356,
        l2_timestamp: 1_700_000_000_250,
        l1_origin: 1_042,
        state_root: B256::repeat_byte(0x11),
        senders: vec![SeedSender {
            address: Address::repeat_byte(0xAA),
            next_nonce: 3,
        }],
    }
    .encode()
    .unwrap()
}

#[test]
fn the_head_parses_from_the_bytes_the_producer_writes() {
    let head = SeedHead::parse(&seed_bytes()).unwrap();
    assert_eq!(
        head,
        SeedHead {
            block: 239,
            end_tx_idx: 5_356,
            l1_origin: 1_042,
        }
    );
    assert_eq!(head.resume_cursor(), "(5356,240)");
}

#[test]
fn a_file_that_is_not_a_version_1_seed_is_refused() {
    let bytes = seed_bytes();
    let mut magic = bytes.clone();
    magic[0] = b'X';
    let mut version = bytes.clone();
    version[7] = 2;
    assert!(SeedHead::parse(&magic).is_err());
    assert!(SeedHead::parse(&version).is_err());
    assert!(SeedHead::parse(&bytes[..44]).is_err(), "the origin is cut");
    assert!(SeedHead::parse(&bytes[..48]).is_ok());
}

/// The cluster job as Nomad returns it: one group per member, with the
/// properties of `cluster.nomad.hcl`.
fn cluster_job() -> Value {
    let member = |id: u32| {
        json!({
            "Name": format!("cluster-{id}"),
            "Tasks": [{
                "Name": "cluster",
                "Env": {
                    "JAVA_TOOL_OPTIONS": format!(
                        "-Daeron.mtu.length=1344 -Dkardamom.cluster.memberId={id} -Dkardamom.cluster.remoteOrigins=412347,412399 -Dkardamom.cluster.seedSnapshot= -Dkardamom.cluster.adminPort=40205"
                    )
                }
            }]
        })
    };
    json!({ "ID": "cluster", "TaskGroups": [member(0), member(1), member(2)] })
}

fn options(job: &Value, group: usize) -> Vec<String> {
    job["TaskGroups"][group]["Tasks"][0]["Env"]["JAVA_TOOL_OPTIONS"]
        .as_str()
        .unwrap()
        .split_whitespace()
        .map(str::to_string)
        .collect()
}

#[test]
fn every_member_starts_from_the_seed_with_interop_off() {
    let seeded = JobDefinition(&cluster_job())
        .seeded("/opt/kardamom/seed/seed.bin")
        .unwrap();
    for (group, id) in [(0, "0"), (1, "1"), (2, "2")] {
        let options = options(&seeded, group);
        let has = |o: &str| options.iter().filter(|x| *x == o).count();
        assert_eq!(
            has("-Dkardamom.cluster.seedSnapshot=/opt/kardamom/seed/seed.bin"),
            1
        );
        assert_eq!(has("-Dkardamom.cluster.remoteOrigins="), 1);
        assert_eq!(has(&format!("-Dkardamom.cluster.memberId={id}")), 1);
        assert_eq!(has("-Daeron.mtu.length=1344"), 1);
        assert_eq!(has("-Dkardamom.cluster.adminPort=40205"), 1);
        assert_eq!(options.len(), 5, "no option is lost or doubled");
    }
    assert_eq!(seeded["ID"], "cluster");
}

#[test]
fn a_job_without_the_seed_option_gets_it_appended() {
    let job = json!({ "TaskGroups": [{ "Tasks": [{ "Env": { "JAVA_TOOL_OPTIONS": "-Da=1" } }] }] });
    let seeded = JobDefinition(&job).seeded("/s").unwrap();
    assert_eq!(
        options(&seeded, 0),
        [
            "-Da=1",
            "-Dkardamom.cluster.seedSnapshot=/s",
            "-Dkardamom.cluster.remoteOrigins="
        ]
    );
    let no_options = json!({ "TaskGroups": [{ "Tasks": [{ "Env": {} }] }] });
    assert!(JobDefinition(&no_options).seeded("/s").is_err());
}

#[test]
fn the_da_watcher_resumes_after_the_seed_origin() {
    let job = json!({ "TaskGroups": [{ "Tasks": [{ "Config": { "args": ["--l1-rpc", "http://l1"] } }] }] });
    let resumed = JobDefinition(&job).resumed_after(1_042).unwrap();
    assert_eq!(
        resumed["TaskGroups"][0]["Tasks"][0]["Config"]["args"],
        json!(["--l1-rpc", "http://l1", "--l1-resume-after", "1042"])
    );
    assert_eq!(
        job["TaskGroups"][0]["Tasks"][0]["Config"]["args"],
        json!(["--l1-rpc", "http://l1"]),
        "the saved definition stays as it was"
    );
    let no_args = json!({ "TaskGroups": [{ "Tasks": [{ "Config": {} }] }] });
    assert!(JobDefinition(&no_args).resumed_after(1).is_err());
}
