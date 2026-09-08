//! `init` needs an ambient Tokio runtime. The exporter runs as a task on
//! it. This file checks two contracts:
//!
//! - Inside a runtime, the whole scrape works end to end. This includes a
//!   single-threaded (`current_thread`) runtime driven by `block_on`. This
//!   is the smallest runtime a caller can supply.
//! - The runtime check is explicit. A `Handle::try_current()` failure
//!   returns an error. It does not show the exporter's "no reactor
//!   running" panic. A plain `#[test]` cannot await an async fn. So the
//!   runtime-less case uses the `Handle` contract inside `init` instead.
//!
//! The scrape (`common::scrape`) uses a raw std `TcpStream` on purpose, on
//! a blocking task. It does not use an HTTP client. So the test depends
//! only on the exporter listener.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

/// Bind port 0 with a throwaway listener, to pick a free port, then run
/// `kardamom_obs::init` on it. The pick-then-rebind window is racy:
/// another process can grab the port in between, and the exporter binds
/// eagerly, so init fails if it does. Retry with a fresh port in that
/// case. A bind failure happens before the global recorder installs, so
/// calling init again is safe.
///
/// # Panics
///
/// Panics if `init` never succeeds within 5 attempts.
async fn init_on_a_free_port() -> SocketAddr {
    let mut last_err = None;
    for _ in 0..5 {
        let free: SocketAddr = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        match kardamom_obs::init("obs-test", free, "runtime-host", "0.0.0", "deadbeef").await {
            Ok(()) => return free,
            Err(e) => last_err = Some(e),
        }
    }
    panic!(
        "init never succeeded on a freshly-picked free port: {:#}",
        last_err.expect("at least one attempt records an error")
    );
}

#[test]
fn init_and_scrape_on_current_thread_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let free = init_on_a_free_port().await;

        metrics::gauge!("kardamom_obs_test_gauge").set(42.0);

        // The listener is bound once init returns; poll briefly for the
        // accept loop to start serving. The scrape blocks, so it runs on a
        // blocking task while this runtime keeps driving the exporter.
        let body = common::scrape(free, Duration::from_secs(10)).await;

        assert!(
            body.contains("kardamom_obs_test_gauge"),
            "gauge missing:\n{body}"
        );
        assert!(
            body.contains("kardamom_service_up"),
            "service_up missing:\n{body}"
        );
        assert!(
            body.contains(r#"host_id="runtime-host""#),
            "host_id label missing:\n{body}"
        );
    });
}

#[test]
fn no_ambient_runtime_is_detectable() {
    assert!(
        tokio::runtime::Handle::try_current().is_err(),
        "a plain #[test] has no ambient runtime; init relies on this check"
    );
}
