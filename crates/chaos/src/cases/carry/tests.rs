use std::time::Duration;

use serde_json::json;

use super::members::Window;
use super::upgrade::Upgrade;
use super::{Run, STEPS, Step};
use crate::cases::Case;
use crate::knobs::Knobs;

#[test]
fn every_step_is_a_case_of_the_carry_shard_in_run_order() {
    let names: Vec<&str> = STEPS.iter().map(|s| s.name()).collect();
    assert_eq!(names, crate::Shard::Carry.cases());
    for s in STEPS {
        assert!(
            Case::parse(s.name()).unwrap() == Case::Carry(*s) && s.name().starts_with("carry-"),
            "{}",
            s.name()
        );
    }
}

#[test]
fn every_load_window_covers_its_step() {
    let k = Knobs::read(&[]).unwrap();
    for s in STEPS {
        assert!(
            Case::Carry(*s).window(&k) >= s.floor(&k) && s.floor(&k) > k.inject_delay,
            "{}",
            s.name()
        );
    }
    assert!(Step::VersionsOn.floor(&k) >= Duration::from_mins(10));
}

#[test]
fn the_window_states_match_the_gauges_of_the_sealer() {
    assert!(Window::On.carry_window() && !Window::Off.carry_window());
    assert_eq!((Window::On.feature_state(), Window::On.exact()), (2, 1));
    assert_eq!((Window::Off.feature_state(), Window::Off.exact()), (1, 0));
}

#[test]
fn an_upgrade_off_holds_the_feature_with_a_time_that_never_comes() {
    assert_eq!(Upgrade::on(5).to_string(), "initiateUpgrade(5, 0)");
    assert_eq!(
        Upgrade::off(4).to_string(),
        format!("initiateUpgrade(4, {})", u64::MAX)
    );
}

#[test]
fn the_lockbox_is_the_proxy_of_the_first_listed_entry() {
    let listing = json!([{"id": "ETHLockbox", "proxy": "0xab"}, {"proxy": "0xcd"}]).to_string();
    assert_eq!(Run::proxy_in(&listing).unwrap().as_deref(), Some("0xab"));
    assert_eq!(Run::proxy_in("[]\n").unwrap(), None);
    assert!(Run::proxy_in("not json").is_err());
}

#[test]
fn a_stopped_group_keeps_the_other_groups() {
    let mut job = json!({"TaskGroups": [
        {"Name": "cluster-0", "Count": 1},
        {"Name": "cluster-1", "Count": 1},
    ]});
    Run::stop_group(&mut job, "cluster-1").unwrap();
    assert_eq!(job["TaskGroups"][0]["Count"], 1);
    assert_eq!(job["TaskGroups"][1]["Count"], 0);
    assert!(Run::stop_group(&mut job, "cluster-7").is_err());
}

#[test]
fn every_step_gets_a_funded_account() {
    let k = Knobs::read(crate::Shard::Carry.env()).unwrap();
    let mut accounts = crate::accounts::Accounts::new(k.account_base, k.run_load);
    for s in STEPS {
        accounts
            .take(Case::Carry(*s).pin(), s.name(), |_| false)
            .unwrap();
    }
}
