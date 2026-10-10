//! Tests for the shared binary wiring.
use super::*;

// The factory gates on config. No archive endpoints means no recovery,
// just a plain bounded join. Endpoints without local transport means it
// is disabled, with a loud warning. It never builds a half-configured
// client.
#[test]
fn recovery_factory_gates_on_config() {
    let mut plane = kardamom_log::discovery::StreamPlane::static_only(
        kardamom_log::config::ChannelsConfig::default(),
    );
    let mut aeron = AeronConfig::default();
    assert!(
        archive_join_recovery(
            &mut plane,
            &aeron,
            None,
            Some("10.0.0.1:40140"),
            Some("10.0.0.1:40130")
        )
        .is_none(),
        "no endpoints configured ⇒ None"
    );
    aeron.tx_data_archive_endpoints = vec!["192.168.56.31:8010".into()];
    assert!(
        archive_join_recovery(&mut plane, &aeron, None, None, None).is_none(),
        "endpoints but no local transport ⇒ None"
    );
    let f = archive_join_recovery(
        &mut plane,
        &aeron,
        None,
        Some("10.0.0.1:40140"),
        Some("10.0.0.1:40130"),
    )
    .expect("endpoints plus local transport ⇒ Some");
    assert_eq!(
        f.cfg.tx_data_endpoints.current(),
        vec!["192.168.56.31:8010".to_string()]
    );
}
