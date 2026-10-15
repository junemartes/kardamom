use std::cell::Cell;
use std::time::Duration;

use reqwest::StatusCode;
use tokio_util::sync::CancellationToken;

use super::*;

/// The limit of the tests.
const LIMIT: Duration = Duration::from_secs(10);

fn backoff() -> WatchTiming {
    WatchTiming {
        wait: Duration::from_secs(1),
        backoff_min: Duration::from_millis(100),
        backoff_max: Duration::from_secs(2),
    }
}

fn retry(stop: CancellationToken) -> StartRetry {
    StartRetry::new("record alloc-1:tx_errors:1015", LIMIT, backoff(), stop)
}

fn unavailable() -> LogError {
    LogError::catalog_status("register a", StatusCode::SERVICE_UNAVAILABLE, "")
}

/// A registration that fails with `error` for its first `failures` tries,
/// then returns the number of its try. `tries` counts every try.
fn flaky(tries: &Cell<u32>, failures: u32, error: fn() -> LogError) -> Result<u32, LogError> {
    tries.set(tries.get() + 1);
    if tries.get() > failures {
        Ok(tries.get())
    } else {
        Err(error())
    }
}

#[test]
fn only_an_unavailable_catalog_is_transient() {
    assert!(unavailable().is_transient());
    assert!(
        !LogError::catalog_status("register a", StatusCode::FORBIDDEN, "denied").is_transient()
    );
    assert!(!LogError::Aeron("add_publication x: TimedOut".into()).is_transient());
    assert!(!LogError::Discovery("x".into()).is_transient());
}

#[test]
fn a_classified_error_keeps_its_text() {
    let e = LogError::catalog_status("register a", StatusCode::BAD_GATEWAY, "down");
    assert_eq!(
        e.to_string(),
        "discovery: register a: 502 Bad Gateway: down"
    );
}

#[tokio::test(start_paused = true)]
async fn the_registration_succeeds_after_transient_failures() {
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let got = retry(CancellationToken::new())
        .run(|| std::future::ready(flaky(&tries, 3, unavailable)))
        .await
        .unwrap();
    assert_eq!(got, 4);
    // The pauses double from the minimum: 100 + 200 + 400 ms.
    assert_eq!(start.elapsed(), Duration::from_millis(700));
}

#[tokio::test(start_paused = true)]
async fn the_registration_gives_up_after_the_limit() {
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let got = retry(CancellationToken::new())
        .run(|| std::future::ready(flaky(&tries, u32::MAX, unavailable)))
        .await;
    let error = got.unwrap_err();
    assert_eq!(error.to_string(), unavailable().to_string());
    // The last pause ends at the limit, and the try there is the last.
    assert_eq!(start.elapsed(), LIMIT);
    // Nine pauses reach 10 s: 100, 200, 400, 800 and 1600 ms, three
    // pauses at the 2 s cap, and the 900 ms rest.
    assert_eq!(tries.get(), 10);
}

#[tokio::test(start_paused = true)]
async fn a_non_transient_error_returns_at_once() {
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let got = retry(CancellationToken::new())
        .run(|| {
            std::future::ready(flaky(&tries, u32::MAX, || {
                LogError::catalog_status("register a", StatusCode::FORBIDDEN, "denied")
            }))
        })
        .await;
    assert!(matches!(got, Err(LogError::Discovery(_))));
    assert_eq!(tries.get(), 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// Whether `got` is the "stopped" error of [`retry`].
fn is_stopped(got: &Result<u32, LogError>) -> bool {
    matches!(got, Err(LogError::Discovery(m))
        if m == "stopped during start-up open of record alloc-1:tx_errors:1015")
}

#[tokio::test(start_paused = true)]
async fn a_stop_during_a_pause_ends_the_registration_at_once() {
    let stop = CancellationToken::new();
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let cancel = stop.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(250)).await;
        cancel.cancel();
    });
    let got = retry(stop)
        .run(|| std::future::ready(flaky(&tries, u32::MAX, unavailable)))
        .await;
    assert!(is_stopped(&got), "{got:?}");
    // Tries at 0 and 100 ms; the cancel lands in the 200 ms pause.
    assert_eq!(tries.get(), 2);
    assert_eq!(start.elapsed(), Duration::from_millis(250));
}

#[tokio::test(start_paused = true)]
async fn a_stop_before_a_pause_ends_the_registration_with_no_pause() {
    let stop = CancellationToken::new();
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let got = retry(stop.clone())
        .run(|| {
            stop.cancel();
            std::future::ready(flaky(&tries, u32::MAX, unavailable))
        })
        .await;
    assert!(is_stopped(&got), "{got:?}");
    assert_eq!(tries.get(), 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
}
