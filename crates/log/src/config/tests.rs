use super::*;
use std::io::Write;

fn write_tmp(contents: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().expect("tempfile");
    f.write_all(contents.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

/// Write `toml` to a temp file and load it. The `write_tmp` plus
/// `from_toml_path` pair repeats across most tests in this module; this
/// is the one place that owns the temp file's lifetime past the call.
fn load(toml: &str) -> Result<LogConfig, LogError> {
    LogConfig::from_toml_path(write_tmp(toml).path())
}

/// A minimal MDS `[channels]` block: fixed control channel and host,
/// caller-chosen base port and executor count. Several tests differ from
/// each other only in these two values.
fn mds_toml(base_port: &str, count: u32) -> String {
    format!(
        r#"
            [channels]
            tx_receipts_control_channel = "aeron:udp?control-mode=manual"
            tx_receipts_endpoint_host = "192.168.56.31"
            tx_receipts_endpoint_base_port = {base_port}
            tx_receipts_executor_count = {count}
            "#
    )
}

#[test]
fn empty_file_yields_defaults() {
    let cfg = load("").expect("load empty");
    // Must match the built-in defaults exactly.
    let d = LogConfig::default();
    assert_eq!(cfg.recorder_id, d.recorder_id);
    assert_eq!(cfg.channels.tx_errors_channel, d.channels.tx_errors_channel);
    assert_eq!(cfg.aeron.file_sync_level, d.aeron.file_sync_level);
}

#[test]
fn partial_channels_section_inherits_other_fields() {
    // Only one channel field is set; everything else must default.
    let f = write_tmp(
        r#"
            [channels]
            tx_errors_channel = "aeron:udp?endpoint=239.192.56.17:40030"
            tx_errors_stream_id = 1015
            "#,
    );
    let cfg = LogConfig::from_toml_path(f.path()).expect("load partial");
    assert_eq!(
        cfg.channels.tx_errors_channel,
        "aeron:udp?endpoint=239.192.56.17:40030"
    );
    // Untouched channel fields fall back to IPC defaults.
    assert_eq!(
        cfg.channels.tx_receipts_channel,
        "aeron:ipc?alias=tx-receipts"
    );
    assert_eq!(cfg.channels.tx_data_stream_id_base, 2000);
    // Untouched sections fall back wholesale.
    assert_eq!(cfg.recorder_id, 0);
}

#[test]
fn recorder_id_override() {
    let cfg = load("recorder_id = 2\n").expect("load");
    assert_eq!(cfg.recorder_id, 2);
}

#[test]
fn quorum_section_is_rejected() {
    // `[quorum]` is not a config section. An unknown section must fail
    // loudly, not load with the section ignored.
    let err = load("[quorum]\nn = 5\nq = 3\n").expect_err("quorum section must be rejected");
    assert!(matches!(err, LogError::Config(_)), "got {err:?}");
}

#[test]
fn unknown_field_is_rejected() {
    let f = write_tmp(
        r#"
            [channels]
            tx_errors_channLE = "typo"
            "#,
    );
    let err = LogConfig::from_toml_path(f.path()).expect_err("typo must be rejected");
    assert!(matches!(err, LogError::Config(_)), "got {err:?}");
}

#[test]
fn missing_file_is_a_config_error() {
    let err =
        LogConfig::from_toml_path(Path::new("/no/such/log-config.toml")).expect_err("missing file");
    assert!(matches!(err, LogError::Config(_)), "got {err:?}");
}

#[test]
fn resolve_none_is_default() {
    let cfg = LogConfig::resolve(None).expect("resolve none");
    assert_eq!(
        cfg.channels.tx_errors_channel,
        LogConfig::default().channels.tx_errors_channel
    );
}

#[test]
fn tx_bal_defaults_present() {
    let ch = ChannelsConfig::default();
    assert_eq!(ch.tx_bal_stream_id, 1004);
    assert!(ch.tx_bal_channel.contains("tx-bal"));
    // Must not collide with the receipt block or other channels.
    for other in [
        ch.tx_receipts_stream_id,
        ch.tx_receipts_stream_id + 1,
        ch.tx_errors_stream_id,
        ch.tx_deposits_stream_id,
    ] {
        assert_ne!(ch.tx_bal_stream_id, other);
    }
}

#[test]
fn tx_remote_epochs_defaults_present() {
    let ch = ChannelsConfig::default();
    assert_eq!(ch.tx_remote_epochs_stream_id, 1017);
    assert!(ch.tx_remote_epochs_channel.contains("tx-remote-epochs"));
    // Aeron IPC routes by stream id (the alias is a debug label only), so a
    // collision silently delivers another stream's frames to be rkyv-decoded
    // as a RemoteEpochRecord.
    for other in [
        ch.tx_receipts_stream_id,
        ch.tx_receipts_stream_id + 1,
        ch.tx_bal_stream_id,
        ch.tx_errors_stream_id,
        ch.tx_deposits_stream_id,
        ch.fsync_watermark_stream_id,
    ] {
        assert_ne!(ch.tx_remote_epochs_stream_id, other);
    }
}

#[test]
fn round_trips_through_toml() {
    // A fully serialized config must parse back identically. This guards
    // the serde attributes against a field that serializes but will not
    // deserialize.
    let original = LogConfig::default();
    let s = toml::to_string(&original).expect("serialize");
    let f = write_tmp(&s);
    let back = LogConfig::from_toml_path(f.path()).expect("reparse");
    assert_eq!(
        back.channels.fsync_watermark_stream_id,
        original.channels.fsync_watermark_stream_id
    );
    assert_eq!(
        back.aeron.archive_control_request_channel,
        original.aeron.archive_control_request_channel
    );
}

#[test]
fn tx_receipts_mds_off_by_default() {
    let ch = ChannelsConfig::default();
    assert!(!ch.tx_receipts_mds_enabled(), "default must be legacy IPC");
    assert_eq!(ch.tx_receipts_endpoint(0), None);
}

#[test]
fn tx_receipts_endpoint_offsets_port_by_replica() {
    let ch = ChannelsConfig {
        tx_receipts_control_channel: "aeron:udp?control-mode=manual".into(),
        tx_receipts_endpoint_host: "192.168.56.31".into(),
        tx_receipts_endpoint_base_port: Some(BasePort::try_from(40020).unwrap()),
        ..Default::default()
    };
    assert!(ch.tx_receipts_mds_enabled());
    // Receipts use base + 2*r, boundaries use base + 2*r + 1. These are
    // distinct ports, so ingress's two manual subscriptions do not bind
    // the same socket.
    assert_eq!(
        ch.tx_receipts_endpoint(0).as_deref(),
        Some("aeron:udp?endpoint=192.168.56.31:40020")
    );
    assert_eq!(
        ch.tx_receipts_boundary_endpoint(0).as_deref(),
        Some("aeron:udp?endpoint=192.168.56.31:40021")
    );
    assert_eq!(
        ch.tx_receipts_endpoint(2).as_deref(),
        Some("aeron:udp?endpoint=192.168.56.31:40024"),
        "replica i receipts at base_port + 2*i"
    );
    assert_eq!(
        ch.tx_receipts_boundary_endpoint(2).as_deref(),
        Some("aeron:udp?endpoint=192.168.56.31:40025"),
        "replica i boundaries at base_port + 2*i + 1"
    );
    // The receipt and boundary endpoints for the same replica must differ.
    assert_ne!(
        ch.tx_receipts_endpoint(1),
        ch.tx_receipts_boundary_endpoint(1)
    );
}

#[test]
fn mds_contract_parses_from_toml_and_aligns_both_sides() {
    // The deploy channels.toml MDS contract. The executor publishes to
    // `tx_receipts_endpoint(replica)`, and ingress attaches the same
    // `tx_receipts_endpoint(i)` for i in 0..executor_count. This single
    // helper is the source of truth on both sides, so a round-trip parse
    // must yield identical endpoints for a given index.
    let f = write_tmp(
        r#"
            [channels]
            tx_receipts_control_channel = "aeron:udp?control-mode=manual|interface=192.168.56.0/24"
            tx_receipts_endpoint_host = "192.168.56.31"
            tx_receipts_endpoint_base_port = 40020
            tx_receipts_executor_count = 3
            tx_receipts_stream_id = 1002
            "#,
    );
    let ch = LogConfig::from_toml_path(f.path())
        .expect("load MDS")
        .channels;
    assert!(ch.tx_receipts_mds_enabled());
    assert_eq!(
        ch.tx_receipts_executor_count,
        Some(std::num::NonZeroU32::new(3).unwrap())
    );
    // Executor side (replica 1) and ingress side (destination index 1)
    // resolve to the exact same endpoint: base + 2*1 = 40022 (receipts).
    assert_eq!(
        ch.tx_receipts_endpoint(1).as_deref(),
        Some("aeron:udp?endpoint=192.168.56.31:40022")
    );
    // The boundary stream (`tx_receipts_stream_id + 1`) uses a distinct
    // endpoint, base + 2*1 + 1 = 40023, so ingress's two manual
    // subscriptions do not bind the same socket.
    assert_eq!(
        ch.tx_receipts_boundary_endpoint(1).as_deref(),
        Some("aeron:udp?endpoint=192.168.56.31:40023")
    );
    assert_eq!(ch.tx_receipts_stream_id, 1002);
}

#[test]
fn mds_nonpositive_base_port_rejected() {
    // A non-positive base must fail at load time with a config error, not
    // wrap into a nonsense port through the unsigned endpoint arithmetic.
    for port in ["-40020", "0"] {
        let err =
            load(&mds_toml(port, 3)).expect_err("non-positive MDS base port must be rejected");
        assert!(matches!(err, LogError::Config(_)), "got {err:?}");
        assert!(
            err.to_string().contains("tx_receipts_endpoint_base_port"),
            "got {err}"
        );
    }
}

#[test]
fn mds_base_port_overflowing_u16_rejected() {
    // Highest replica endpoint (base + 2*count + 1) must stay a valid port.
    load(&mds_toml("65530", 3)).expect_err("overflowing MDS base port rejected");
}

#[test]
fn mds_valid_base_port_accepted() {
    // The deploy-shaped MDS config still loads.
    load(&mds_toml("40020", 3)).expect("valid MDS config loads");
}

#[test]
fn mds_executor_count_zero_is_rejected() {
    // `tx_receipts_executor_count` is a `NonZeroU32` at the config
    // boundary. A `0` in the file must fail to parse, not silently
    // become "no executors".
    let err = load(&mds_toml("40020", 0)).expect_err("a zero executor count must be rejected");
    assert!(matches!(err, LogError::Config(_)), "got {err:?}");
}

#[test]
fn non_mds_base_port_zero_loads_as_unset() {
    // The deployed `channels.toml` renders `0` when MDS is off. It loads
    // as `None`, the same as an absent key.
    let f = write_tmp("[channels]\ntx_receipts_endpoint_base_port = 0\n");
    let ch = LogConfig::from_toml_path(f.path())
        .expect("a zero base port loads as unset")
        .channels;
    assert_eq!(ch.tx_receipts_endpoint_base_port, None);
}

#[test]
fn non_mds_negative_base_port_is_rejected() {
    // A garbage port value is an error, MDS on or off.
    let err = load("[channels]\ntx_receipts_endpoint_base_port = -1\n")
        .expect_err("a negative base port must be rejected");
    assert!(matches!(err, LogError::Config(_)), "got {err:?}");
}

#[test]
fn non_mds_base_port_absent_loads() {
    // Omitting the field entirely (the common case: single-host IPC
    // deployments never set it) still loads, defaulting to `None`.
    let f = write_tmp("[channels]\n");
    let ch = LogConfig::from_toml_path(f.path())
        .expect("config without a base port loads")
        .channels;
    assert_eq!(ch.tx_receipts_endpoint_base_port, None);
}

#[test]
fn executor_count_defaults_to_none() {
    // Default (IPC) config never attaches MDS destinations.
    assert_eq!(ChannelsConfig::default().tx_receipts_executor_count, None);
}

#[test]
fn tx_data_stream_id_base_too_close_to_max_is_rejected() {
    let ch = ChannelsConfig {
        tx_data_stream_id_base: i32::MAX - 10,
        ..Default::default()
    };
    assert!(
        ch.validate().is_err(),
        "a base within 255 of i32::MAX must fail validate, since \
         tx_data_stream_id(255) would overflow"
    );
}

#[test]
fn tx_receipts_stream_id_at_max_is_rejected() {
    let ch = ChannelsConfig {
        tx_receipts_stream_id: i32::MAX,
        ..Default::default()
    };
    assert!(
        ch.validate().is_err(),
        "tx_receipts_stream_id == i32::MAX must fail validate, since \
         tx_receipts_boundary_stream_id() would overflow"
    );
}

#[test]
fn tx_receipts_boundary_stream_id_is_one_past_the_receipt_stream() {
    let ch = ChannelsConfig::default();
    assert_eq!(
        ch.tx_receipts_boundary_stream_id(),
        ch.tx_receipts_stream_id + 1
    );
}

#[test]
fn the_deployed_channels_template_loads() {
    // `deploy/cluster/config/channels.toml.tpl` is what every service in
    // the container cluster reads. It must parse with this crate's types,
    // zero sentinels included.
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../deploy/cluster/config/channels.toml.tpl"
    );
    let cfg = LogConfig::from_toml_path(Path::new(path)).expect("deployed channels.toml.tpl loads");
    assert_eq!(cfg.channels.tx_receipts_endpoint_base_port, None);
}

#[test]
fn discovery_section_parses_and_validates() {
    let cfg = load(
        r#"
            [discovery]
            enabled = true
            cluster_id = "dev"
            chain_id = 412346
            advertise_interface = "192.168.56.0/24"
            "#,
    )
    .expect("a complete discovery section loads");
    assert!(cfg.discovery.enabled);
    assert_eq!(
        cfg.discovery.advertise_interface,
        Some(InterfaceSelector::Network {
            net: "192.168.56.0".parse().unwrap(),
            prefix: 24
        })
    );
    assert_eq!(cfg.discovery.consul_http_addr, "http://127.0.0.1:8500");
}

#[test]
fn enabled_discovery_needs_a_cluster_id_and_an_interface() {
    let no_cluster = load("[discovery]\nenabled = true\nadvertise_interface = \"eth1\"\n");
    assert!(matches!(no_cluster, Err(LogError::Config(_))));
    let no_interface = load("[discovery]\nenabled = true\ncluster_id = \"dev\"\n");
    assert!(matches!(no_interface, Err(LogError::Config(_))));
    let bad_prefix = load("[discovery]\nadvertise_interface = \"10.0.0.0/40\"\n");
    assert!(matches!(bad_prefix, Err(LogError::Config(_))));
    let disabled =
        load("[discovery]\nenabled = false\n").expect("a disabled section needs nothing");
    assert!(!disabled.discovery.enabled);
}
