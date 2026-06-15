//! Contract test: the deployed cluster `channels.toml.tpl` must deserialize
//! into a valid `LogConfig` AND enable tx_ordering MDC with a publisher list
//! whose subscriber URIs are well-formed. This is the integration contract the
//! cluster relies on — if a field is renamed/removed in `ChannelsConfig`
//! without updating the template (or vice versa), this fails at `cargo test`
//! instead of at cluster bring-up.
//!
//! (Replaces the old `crates/recorder/tests/cluster_channels_config.rs`, which
//! validated the now-removed recorder/quorum config.)

use std::path::PathBuf;

use kardamom_log::config::LogConfig;

fn cluster_channels_tpl() -> PathBuf {
    // crates/log/tests/ -> repo root is three parents up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/cluster/config/channels.toml.tpl")
}

#[test]
fn cluster_channels_tpl_parses_as_logconfig() {
    let path = cluster_channels_tpl();
    let cfg = LogConfig::from_toml_path(&path)
        .unwrap_or_else(|e| panic!("parse {}: {e}", path.display()));
    // tx_ordering must be on MDC in the cluster.
    assert!(
        cfg.channels.tx_ordering_mdc_enabled(),
        "cluster channels.toml.tpl must enable tx_ordering MDC"
    );
}

#[test]
fn cluster_channels_tpl_mdc_publishers_and_uris() {
    let cfg = LogConfig::from_toml_path(&cluster_channels_tpl()).expect("parse");
    let pubs = &cfg.channels.tx_ordering_mdc_publishers;
    // seq0@w1, seq1@w2, sealer@w2.
    assert_eq!(pubs.len(), 3, "expected 3 tx_ordering MDC publishers");
    assert!(
        pubs.iter().all(|e| e.contains(':')),
        "every MDC publisher must be ip:port: {pubs:?}"
    );
    // Subscriber URIs are well-formed MDC dynamic-control URIs.
    let uris = cfg.channels.tx_ordering_mdc_subscriber_uris();
    assert_eq!(uris.len(), pubs.len());
    for (uri, ep) in uris.iter().zip(pubs.iter()) {
        assert!(uri.contains("control-mode=dynamic"), "MDC URI: {uri}");
        assert!(uri.contains(&format!("control={ep}")), "control endpoint: {uri}");
    }
}
