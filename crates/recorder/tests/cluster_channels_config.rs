//! Guards the deployed cluster `channels.toml` against schema drift: it must
//! parse as a `LogConfig` (the services + recorder load it via `--log-config`).
//! `deny_unknown_fields` means a typo'd key here would fail every alloc at
//! startup — this test catches that at `cargo test` time instead.

use std::path::PathBuf;

use kardamom_log::config::LogConfig;

fn cluster_file(rel: &str) -> PathBuf {
    // crates/recorder -> repo root -> deploy/cluster/config/...
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("deploy/cluster/config")
        .join(rel)
}

#[test]
fn cluster_channels_tpl_is_a_valid_log_config() {
    let path = cluster_file("channels.toml.tpl");
    let cfg = LogConfig::from_toml_path(&path)
        .unwrap_or_else(|e| panic!("deploy/cluster channels.toml.tpl must parse: {e}"));

    // Spot-check the multi-host intent landed: UDP everywhere, quorum 2/3,
    // and the recorder's archive control endpoint is set.
    assert!(cfg.channels.tx_ordering_channel.starts_with("aeron:udp?"));
    assert_eq!(cfg.channels.tx_ordering_stream_id, 1001);
    assert_eq!(cfg.quorum.n, 3);
    assert_eq!(cfg.quorum.q, 2);
    // Archive control is IPC: the recorder is co-located with its node's
    // ArchivingMediaDriver and shares its aeron.dir.
    assert!(
        cfg.aeron
            .archive_control_request_channel
            .starts_with("aeron:ipc"),
        "archive control should ride IPC (co-located recorder), got {}",
        cfg.aeron.archive_control_request_channel
    );
}
