//! Shared test helpers: pick a free port, and scrape `/metrics` with a
//! raw std `TcpStream` on a blocking task. This depends on nothing but
//! the exporter's own listener, not an HTTP client crate, so a broken
//! HTTP client integration can never mask (or fake) a broken exporter.
//! `cargo test` builds each `tests/*.rs` file as its own binary, so this
//! file must live under `tests/common/` (not `tests/`) to avoid becoming
//! its own no-op test binary; each file that uses it adds `mod common;`.

#![allow(dead_code)] // Not every test file in this crate uses every helper.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

/// Bind an ephemeral port, then release it. Racy (another process can grab
/// it before the caller rebinds), so a caller that needs the port to stay
/// free retries on a bind failure.
pub fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = l.local_addr().expect("local_addr");
    drop(l); // release the port before the caller rebinds it
    addr
}

/// Scrape `/metrics` at `addr`, retrying the connect until `budget`
/// elapses (the exporter binds asynchronously, so the first attempts can
/// race its accept loop). Runs on a blocking task, since `TcpStream` is
/// blocking I/O.
///
/// # Panics
///
/// Panics if the connect never succeeds within `budget`, or if the
/// blocking task itself panics.
pub async fn scrape(addr: SocketAddr, budget: Duration) -> String {
    tokio::task::spawn_blocking(move || {
        let deadline = Instant::now() + budget;
        loop {
            match TcpStream::connect(addr) {
                Ok(mut s) => {
                    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                    write!(s, "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n").unwrap();
                    let mut out = String::new();
                    s.read_to_string(&mut out).unwrap();
                    return out;
                }
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("exporter never came up on {addr}: {e}"),
            }
        }
    })
    .await
    .expect("scrape task panicked")
}
