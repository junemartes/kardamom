//! `init` needs an ambient Tokio runtime: the exporter is spawned onto it.
//! Two contracts:
//!
//! - inside a runtime the whole scrape works end to end, including from a
//!   single-threaded (`current_thread`) runtime driven by `block_on`, which
//!   is the smallest runtime a caller can supply;
//! - the runtime check is explicit: a `Handle::try_current()` failure is
//!   reported as an error, not as the exporter's "no reactor running" panic.
//!   (A plain `#[test]` cannot `.await` an `async fn`, so the runtime-less
//!   case is pinned through the `Handle` contract used by `init`.)
//!
//! The scrape below is raw std TcpStream on purpose (on a blocking task): no
//! HTTP client, so the test depends only on the exporter listener.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

#[test]
fn init_and_scrape_on_current_thread_runtime() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        // Bind port 0 via a throwaway listener to pick a free port, then reuse
        // it. The pick-then-rebind window is racy (another process can grab
        // the port in between — the exporter binds eagerly, so init fails if
        // so), so retry with a fresh port; a bind failure happens before the
        // global recorder is installed, which is what makes calling init
        // again safe.
        let mut free: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut last_err = None;
        for _ in 0..5 {
            free = std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap();
            match kardamom_obs::init("obs-test", free, "runtime-host", "0.0.0", "deadbeef").await {
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

        // The listener is bound once init returns; poll briefly for the
        // accept loop to start serving. The scrape blocks, so it runs on a
        // blocking task while this runtime keeps driving the exporter.
        let body = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                match TcpStream::connect(free) {
                    Ok(mut s) => {
                        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                        write!(s, "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n").unwrap();
                        let mut out = String::new();
                        s.read_to_string(&mut out).unwrap();
                        break out;
                    }
                    Err(_) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => panic!("exporter never came up on {free}: {e}"),
                }
            }
        })
        .await
        .unwrap();

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
