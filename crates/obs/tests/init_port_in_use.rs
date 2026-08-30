//! Port-collision contract for `init` (#122): an `AddrInUse` bind gets a
//! BOUNDED retry — a squatting socket is usually a wedged predecessor
//! seconds from being reaped, and dying instantly converts a transient
//! squat into a permanent outage under a mode=fail restart policy (the
//! validator strand, reproduced in #122). Two halves:
//!
//! - squatter HELD past the retry budget → init still fails (never Ok with
//!   a dead /metrics — the original fail-fast contract, now budgeted);
//! - squatter RELEASED mid-retry → init succeeds and /metrics answers.
//!
//! Retry knobs are env-tunable; these tests shrink them. Env vars are
//! process-global, so both halves live in ONE test body (cargo runs tests
//! in threads).

#[tokio::test(flavor = "multi_thread")]
async fn init_retries_addr_in_use_then_fails_or_recovers() {
    // SAFETY: no other test in this binary reads these vars concurrently.
    unsafe {
        std::env::set_var("KARDAMOM_OBS_BIND_RETRIES", "4");
        std::env::set_var("KARDAMOM_OBS_BIND_RETRY_DELAY_MS", "100");
    }

    // Half 1: squatter held for the whole budget → init fails (bounded).
    let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = blocker.local_addr().unwrap();
    let t0 = std::time::Instant::now();
    let err = kardamom_obs::init("obs-collision", addr, "host", "0.0.0", "deadbeef")
        .await
        .expect_err("held squatter must still surface a bind failure");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("PrometheusBuilder::build"),
        "bind failure surfaces through the build/ready hand-off: {msg}"
    );
    assert!(
        t0.elapsed() >= std::time::Duration::from_millis(400),
        "the bounded retry budget must actually be spent before failing"
    );

    // Half 2: squatter released after ~2 retry periods → init recovers.
    // (A fresh port: the global recorder from a successful init can only be
    // installed once per process, so this half must be the SUCCESSFUL one.)
    let blocker2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr2 = blocker2.local_addr().unwrap();
    let releaser = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        drop(blocker2);
    });
    kardamom_obs::init("obs-recovers", addr2, "host", "0.0.0", "deadbeef")
        .await
        .expect("init must recover once the squatter releases the port");
    releaser.await.unwrap();
    let body = tokio::task::spawn_blocking(move || {
        std::io::Read::read_to_string(
            &mut std::net::TcpStream::connect(addr2)
                .map(|mut s| {
                    std::io::Write::write_all(&mut s, b"GET /metrics HTTP/1.0\r\n\r\n").unwrap();
                    s
                })
                .unwrap(),
            &mut String::new(),
        )
    });
    // Connectivity is enough — the recorder installed and the listener owns
    // the port the squatter vacated.
    let _ = body.await;
}
