//! `init` must work from a plain, non-async `main`, with no ambient Tokio
//! runtime. da-watcher calls it exactly this way. Outside a runtime
//! context, a naive `PrometheusBuilder::build()` panics with "there is no
//! reactor running". The scrape below uses a raw std TcpStream on purpose,
//! so the whole test runs runtime-free, end to end.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

#[test]
fn init_and_scrape_without_ambient_runtime() {
    // Bind port 0 via a throwaway listener to pick a free port, then reuse it.
    // The pick-then-rebind window is racy: another process can grab the port
    // in between, and the exporter binds eagerly, so init fails if it does.
    // Retry with a fresh port in that case. A bind failure happens before
    // the global recorder is installed, so calling init again is safe.
    let mut free: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut last_err = None;
    for _ in 0..5 {
        free = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        match kardamom_obs::init("obs-test", free, "runtime-free-host", "0.0.0", "deadbeef") {
            Ok(()) => {
                last_err = None;
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }
    if let Some(e) = last_err {
        panic!("init never succeeded on a freshly-picked free port: {e:#}");
    }

    metrics::gauge!("kardamom_obs_test_gauge").set(42.0);

    // The listener is bound once init returns. Poll briefly for the accept
    // loop to start serving.
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
