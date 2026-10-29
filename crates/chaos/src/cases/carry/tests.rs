use serde_json::json;

use super::{Run, STEPS};
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
}

#[test]
fn the_lockbox_is_the_proxy_of_the_first_listed_entry() {
    let listing = json!([{"id": "ETHLockbox", "proxy": "0xab"}, {"proxy": "0xcd"}]).to_string();
    assert_eq!(Run::proxy_in(&listing).unwrap().as_deref(), Some("0xab"));
    assert_eq!(Run::proxy_in("[]\n").unwrap(), None);
    assert!(Run::proxy_in("not json").is_err());
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

#[test]
fn live_rollout_cases_are_not_selectable() {
    for name in [
        "carry-versions-on",
        "carry-held-offer-after-downgrade",
        "carry-readers-on-egress",
        "carry-window-on",
        "carry-executor-first",
        "carry-executors-rest",
        "carry-window-off-on",
        "carry-leader-kill-at-switch",
    ] {
        assert!(Case::parse(name).is_err(), "{name}");
    }
}
