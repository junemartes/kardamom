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
