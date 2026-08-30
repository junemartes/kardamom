use super::*;
use std::io::Write;

fn write_tmp(contents: &str) -> tempfile::NamedTempFile {
    let mut f = tempfile::NamedTempFile::new().expect("tempfile");
    f.write_all(contents.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

#[test]
fn empty_file_yields_defaults() {
    let f = write_tmp("");
    let cfg = LogConfig::from_toml_path(f.path()).expect("load empty");
    // Must match the built-in defaults exactly.
    let d = LogConfig::default();
    assert_eq!(cfg.recorder_id, d.recorder_id);
    assert_eq!(
        cfg.channels.tx_ordering_channel,
        d.channels.tx_ordering_channel
    );
    assert_eq!(cfg.quorum.n, d.quorum.n);
    assert_eq!(cfg.aeron.file_sync_level, d.aeron.file_sync_level);
}

#[test]
fn partial_channels_section_inherits_other_fields() {
    // Only one channel field is set; everything else must default.
    let f = write_tmp(
        r#"
            [channels]
            tx_ordering_channel = "aeron:udp?endpoint=239.192.56.11:40010"
            tx_ordering_stream_id = 1001
            "#,
    );
    let cfg = LogConfig::from_toml_path(f.path()).expect("load partial");
    assert_eq!(
        cfg.channels.tx_ordering_channel,
        "aeron:udp?endpoint=239.192.56.11:40010"
    );
    // Untouched channel fields fall back to IPC defaults.
    assert_eq!(
        cfg.channels.tx_receipts_channel,
        "aeron:ipc?alias=tx-receipts"
    );
    assert_eq!(cfg.channels.tx_data_stream_id_base, 2000);
    // Untouched sections fall back wholesale.
    assert_eq!(cfg.recorder_id, 0);
    assert_eq!(cfg.quorum, QuorumConfig::default());
}

#[test]
fn recorder_id_and_quorum_override() {
    let f = write_tmp(
        r#"
            recorder_id = 2
            [quorum]
            n = 5
            q = 3
            "#,
    );
    let cfg = LogConfig::from_toml_path(f.path()).expect("load");
    assert_eq!(cfg.recorder_id, 2);
    assert_eq!(cfg.quorum.n, 5);
    assert_eq!(cfg.quorum.q, 3);
}

#[test]
fn unknown_field_is_rejected() {
    let f = write_tmp(
        r#"
            [channels]
            tx_ordering_channLE = "typo"
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
        cfg.channels.tx_ordering_channel,
        LogConfig::default().channels.tx_ordering_channel
    );
}

#[test]
fn tx_bal_defaults_present() {
    let ch = ChannelsConfig::default();
    assert_eq!(ch.tx_bal_stream_id, 1004);
    assert!(ch.tx_bal_channel.contains("tx-bal"));
    // Must not collide with the receipt block or other channels.
    for other in [
        ch.tx_ordering_stream_id,
        ch.tx_receipts_stream_id,
        ch.tx_receipts_stream_id + 1,
        ch.tx_errors_stream_id,
        ch.tx_deposits_stream_id,
    ] {
        assert_ne!(ch.tx_bal_stream_id, other);
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
        back.channels.quorum_watermark_stream_id,
        original.channels.quorum_watermark_stream_id
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
        tx_receipts_endpoint_base_port: 40020,
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
    assert_eq!(ch.tx_receipts_executor_count, 3);
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
    // A negative base used to wrap through `as u32` into a nonsense port.
    // It must now fail at load time with a config error.
    for port in ["-40020", "0"] {
        let f = write_tmp(&format!(
            r#"
                [channels]
                tx_receipts_control_channel = "aeron:udp?control-mode=manual"
                tx_receipts_endpoint_host = "192.168.56.31"
                tx_receipts_endpoint_base_port = {port}
                tx_receipts_executor_count = 3
                "#
        ));
        let err = LogConfig::from_toml_path(f.path())
            .expect_err("non-positive MDS base port must be rejected");
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
    let f = write_tmp(
        r#"
            [channels]
            tx_receipts_control_channel = "aeron:udp?control-mode=manual"
            tx_receipts_endpoint_host = "192.168.56.31"
            tx_receipts_endpoint_base_port = 65530
            tx_receipts_executor_count = 3
            "#,
    );
    LogConfig::from_toml_path(f.path()).expect_err("overflowing MDS base port rejected");
}

#[test]
fn mds_valid_base_port_accepted_and_non_mds_port_unchecked() {
    // The deploy-shaped MDS config still loads.
    let f = write_tmp(
        r#"
            [channels]
            tx_receipts_control_channel = "aeron:udp?control-mode=manual"
            tx_receipts_endpoint_host = "192.168.56.31"
            tx_receipts_endpoint_base_port = 40020
            tx_receipts_executor_count = 3
            "#,
    );
    LogConfig::from_toml_path(f.path()).expect("valid MDS config loads");
    // The base port is only validated when MDS is actually enabled.
    let f = write_tmp("[channels]\ntx_receipts_endpoint_base_port = 0\n");
    LogConfig::from_toml_path(f.path()).expect("port unchecked without MDS");
}

#[test]
fn executor_count_defaults_to_zero() {
    // Default (IPC) config never attaches MDS destinations.
    assert_eq!(ChannelsConfig::default().tx_receipts_executor_count, 0);
}
