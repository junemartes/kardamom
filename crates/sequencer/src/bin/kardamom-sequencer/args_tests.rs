use clap::Parser;

use super::Args;

fn parse(floor: &str) -> Result<Args, clap::Error> {
    Args::try_parse_from([
        "kardamom-sequencer",
        "--config",
        "sequencer.toml",
        "--sealer-version-floor",
        floor,
    ])
}

#[test]
fn the_floor_takes_the_versions_up_to_five() {
    assert_eq!(parse("0").unwrap().sealer_version_floor, 0);
    assert_eq!(parse("5").unwrap().sealer_version_floor, 5);
}

#[test]
fn a_floor_above_five_is_refused() {
    assert!(parse("6").is_err());
}

/// The two twins of a lane differ only in their allocation index, so they
/// get two `tx_events` origins.
#[test]
fn two_replicas_of_one_lane_get_two_origins() {
    let replica = |index: &str| {
        Args::try_parse_from([
            "kardamom-sequencer",
            "--config",
            "sequencer.toml",
            "--tx-events",
            "on",
            "--tx-events-origin",
            index,
        ])
        .unwrap()
    };
    let (first, second) = (replica("0"), replica("1"));
    assert_eq!(first.tx_events, super::TxEventsArg::On);
    assert_eq!(first.tx_events_origin(3), 0x0003);
    assert_eq!(second.tx_events_origin(3), 0x0103);
}
