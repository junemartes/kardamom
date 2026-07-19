//! `init` must fail fast — not return Ok with a dead `/metrics` — when the
//! exporter port is already taken (e.g. two services sharing
//! `KARDAMOM_METRICS_ADDR`, the collision observability.md warns about).
//!
//! This holds because metrics-exporter-prometheus (0.18) binds the TCP
//! listener synchronously inside `PrometheusBuilder::build()`, so a bind
//! failure reaches `init`'s ready channel before it returns; this test pins
//! that behavior so a dependency upgrade that defers the bind to the first
//! poll of the exporter future (as older versions did) fails loudly here
//! instead of silently un-exporting a healthy-looking service.

#[test]
fn init_fails_fast_when_port_already_bound() {
    // Hold the port for the whole test so init cannot bind it.
    let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = blocker.local_addr().unwrap();

    let err = kardamom_obs::init("obs-collision", addr, "host", "0.0.0", "deadbeef")
        .expect_err("init must surface the bind failure instead of returning Ok");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("PrometheusBuilder::build"),
        "bind failure should surface through the build/ready hand-off: {msg}"
    );
}
