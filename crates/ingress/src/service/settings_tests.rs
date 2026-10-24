use clap::Parser;

use super::IngressSettings;

#[derive(Parser)]
struct Flags {
    #[command(flatten)]
    settings: IngressSettings,
}

#[test]
fn the_defaults_of_the_flags_hold() {
    let settings = Flags::try_parse_from(["kardamom-ingress"])
        .expect("every flag has a default")
        .settings;
    assert_eq!(settings.jsonrpc_bind.to_string(), "127.0.0.1:8545");
    assert_eq!(settings.shards.get(), 8);
    assert_eq!(settings.ack_policy(), kardamom_types::AckPolicy::OnOffer);
    assert_eq!(settings.ingress_id(), 0);
    assert_eq!(settings.pending_receipt_timeout_ms, 30_000);
    assert_eq!(settings.inclusion_horizon_blocks.get(), 64);
    assert_eq!(settings.twins.get(), 2);
    assert!(settings.admission_checks);
    assert!(!settings.verdicts && !settings.archive_durability);
    assert_eq!(settings.ack, super::AckArg::Receipt);
    assert_eq!(settings.duplicate_cache_entries.get(), 65_536);
}

#[test]
fn the_validated_ack_parses() {
    let settings = Flags::try_parse_from([
        "kardamom-ingress",
        "--ack",
        "validated",
        "--duplicate-cache-entries",
        "16",
    ])
    .expect("the flags parse")
    .settings;
    assert_eq!(settings.ack, super::AckArg::Validated);
    assert_eq!(settings.duplicate_cache_entries.get(), 16);
    assert!(Flags::try_parse_from(["kardamom-ingress", "--duplicate-cache-entries", "0"]).is_err());
}
