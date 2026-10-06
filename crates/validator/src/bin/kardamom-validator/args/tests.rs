use super::*;

/// A well-formed private key of the dev mnemonic's first account.
const KEY: &str = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
const ORACLE: &str = "0x00000000000000000000000000000000000000aa";
const LOCKBOX: &str = "0x00000000000000000000000000000000000000bb";
const L1: &str = "http://127.0.0.1:8545";

fn args(extra: &[&str]) -> Args {
    let base = ["kardamom-validator", "--config", "validator.toml"];
    Args::try_parse_from(base.iter().chain(extra)).unwrap()
}

#[test]
fn no_attester_flags_leave_the_attester_off() {
    assert!(args(&[]).attester().unwrap().is_none());
}

#[test]
fn the_epoch_check_alone_leaves_the_attester_off() {
    let epoch_check = args(&["--l1-rpc-url", L1, "--lockbox", LOCKBOX]);
    assert!(epoch_check.attester().unwrap().is_none());
}

#[test]
fn oracle_and_key_with_an_l1_url_turn_the_attester_on() {
    let attester = args(&[
        "--l1-rpc-url",
        L1,
        "--output-oracle",
        ORACLE,
        "--attester-key",
        KEY,
    ])
    .attester()
    .unwrap()
    .unwrap();
    assert_eq!(
        attester.oracle,
        ORACLE.parse::<alloy_primitives::Address>().unwrap()
    );
    assert_eq!(attester.l1_rpc_url.as_str(), "http://127.0.0.1:8545/");
}

#[test]
fn oracle_and_key_without_an_l1_url_are_refused() {
    let err = args(&["--output-oracle", ORACLE, "--attester-key", KEY])
        .attester()
        .unwrap_err();
    assert!(err.to_string().contains("--l1-rpc-url"), "{err}");
}

#[test]
fn one_of_oracle_and_key_is_refused() {
    let oracle_only = args(&["--l1-rpc-url", L1, "--output-oracle", ORACLE]);
    assert!(oracle_only.attester().is_err());
    let key_only = args(&["--l1-rpc-url", L1, "--attester-key", KEY]);
    assert!(key_only.attester().is_err());
}
