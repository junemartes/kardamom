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
    // Sequencer INPUT publishers only: seq0@.21, seq1@.22. The sealer is the
    // sole CANONICAL publisher (tx_ordering_canonical_publisher), NOT in this
    // list.
    assert_eq!(pubs.len(), 2, "expected 2 tx_ordering MDC sequencer inputs");
    assert!(
        pubs.iter().all(|e| e.contains(':')),
        "every MDC publisher must be ip:port: {pubs:?}"
    );
    let canonical = &cfg.channels.tx_ordering_canonical_publisher;
    assert!(
        canonical.contains(':') && !pubs.contains(canonical),
        "canonical (sealer) publisher must be ip:port and distinct from the inputs: {canonical:?}"
    );
    // The sealer's input subscriber URIs are well-formed MDC dynamic-control
    // URIs, one per sequencer.
    let uris = cfg.channels.tx_ordering_input_subscriber_uris();
    assert_eq!(uris.len(), pubs.len());
    for (uri, ep) in uris.iter().zip(pubs.iter()) {
        assert!(uri.contains("control-mode=dynamic"), "MDC URI: {uri}");
        assert!(uri.contains(&format!("control={ep}")), "control endpoint: {uri}");
    }
    // The canonical subscriber URI (executor/durability/bootstrap) targets the
    // single sealer endpoint.
    let canon_uri = cfg
        .channels
        .tx_ordering_canonical_subscriber_uri()
        .expect("canonical subscriber URI present");
    assert!(canon_uri.contains("control-mode=dynamic"));
    assert!(canon_uri.contains(&format!("control={canonical}")));
}
