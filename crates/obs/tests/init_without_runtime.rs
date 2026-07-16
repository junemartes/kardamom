//! `init` must work from a plain, non-async `main` — no ambient Tokio
//! runtime. da-watcher calls it exactly this way, and the naive
//! `PrometheusBuilder::build()` panics with "there is no reactor running"
//! outside a runtime context (the regression that crash-looped every
//! service gate on the first version of the dedicated-exporter change).
//! The scrape below is raw std TcpStream on purpose: the whole test runs
//! runtime-free, end to end.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

#[test]
fn init_and_scrape_without_ambient_runtime() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    // Bind port 0 via a throwaway listener to pick a free port, then reuse it.
    let free = std::net::TcpListener::bind(addr)
        .unwrap()
        .local_addr()
        .unwrap();
    kardamom_obs::init("obs-test", free, "runtime-free-host", "0.0.0", "deadbeef").unwrap();

    metrics::gauge!("kardamom_obs_test_gauge").set(42.0);

    // The exporter thread binds asynchronously; poll briefly.
    let deadline = Instant::now() + Duration::from_secs(10);
    let body = loop {
        match TcpStream::connect(free) {
            Ok(mut s) => {
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                write!(s, "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n").unwrap();
                let mut out = String::new();
                s.read_to_string(&mut out).unwrap();
                break out;
            }
            Err(e) if Instant::now() < deadline => {
                let _ = e;
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => panic!("exporter never came up on {free}: {e}"),
        }
    };

    assert!(
        body.contains("kardamom_obs_test_gauge"),
        "gauge missing:\n{body}"
    );
    assert!(
        body.contains("kardamom_service_up"),
        "service_up missing:\n{body}"
    );
    assert!(
        body.contains(r#"host_id="runtime-free-host""#),
        "host_id label missing:\n{body}"
    );
}
