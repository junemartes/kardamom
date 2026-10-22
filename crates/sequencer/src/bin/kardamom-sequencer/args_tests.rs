use clap::Parser;

use super::Args;
use crate::ingress::Embedded;

fn parse(extra: &[&str]) -> Args {
    let base = ["kardamom-sequencer", "--config", "/local/sequencer.toml"];
    Args::try_parse_from(base.iter().chain(extra)).expect("the flags parse")
}

#[test]
fn the_ingress_is_off_without_the_subcommand() {
    let args = parse(&["--lane", "1"]);
    assert!(args.embedded.is_none());
}

#[test]
fn the_ingress_subcommand_takes_the_ingress_flags() {
    let args = parse(&[
        "--lane",
        "1",
        "ingress",
        "--jsonrpc-bind",
        "0.0.0.0:8551",
        "--ingress-id",
        "21",
        "--shards",
        "2",
        "--archive-durability",
        "--cluster-egress-endpoint",
        "10.0.0.5:40123",
    ]);
    let Some(Embedded::Ingress(settings)) = args.embedded else {
        panic!("the ingress subcommand is present");
    };
    assert_eq!(settings.ingress_id(), 21);
    assert_eq!(settings.ack_policy(), kardamom_types::AckPolicy::OnOffer);
}

#[test]
fn a_sequencer_flag_after_the_subcommand_is_refused() {
    let base = [
        "kardamom-sequencer",
        "--config",
        "c.toml",
        "ingress",
        "--lane",
        "1",
    ];
    assert!(Args::try_parse_from(base).is_err());
}
