use std::cell::Cell;
use std::time::Duration;

use rusteron_client::{AeronCError, AeronErrorType};

use super::*;
use crate::driver_budget::DriverBudget;

/// The stall budget of the tests: a 10 s limit.
const BUDGET: Duration = Duration::from_secs(5);

fn backoff() -> WatchTiming {
    WatchTiming {
        wait: Duration::from_secs(1),
        backoff_min: Duration::from_millis(100),
        backoff_max: Duration::from_secs(2),
    }
}

fn retry() -> StartRetry {
    StartRetry::new("tx_receipts publication", BUDGET, backoff())
}

fn timed_out() -> LogError {
    LogError::aeron_add(
        "add_publication",
        "aeron:udp?control-mode=dynamic",
        &AeronCError::from_code(AeronErrorType::TimedOut.code()),
    )
}

/// An open that fails with `error` for its first `failures` tries, then
/// returns the number of its try. `tries` counts every try.
fn flaky(tries: &Cell<u32>, failures: u32, error: fn() -> LogError) -> Result<u32, LogError> {
    tries.set(tries.get() + 1);
    if tries.get() > failures {
        Ok(tries.get())
    } else {
        Err(error())
    }
}

#[test]
fn the_limit_is_two_stall_budgets_of_the_stall_tolerance() {
    let limit = |driver_timeout_ms| {
        let budget = DriverBudget::from_driver_timeout_ms(driver_timeout_ms)
            .unwrap()
            .duration();
        StartRetry::new("x", budget, backoff()).limit()
    };
    assert_eq!(limit(10_000), Duration::from_secs(30), "production");
    assert_eq!(limit(30_000), Duration::from_secs(70), "CI");
    assert_eq!(limit(1_000), Duration::from_secs(20), "the budget floor");
    assert_eq!(
        StartRetry::new("x", Duration::MAX, backoff()).limit(),
        Duration::MAX
    );
}

#[test]
fn only_a_timeout_or_an_unavailable_catalog_is_transient() {
    assert!(timed_out().is_transient());
    assert!(matches!(timed_out(), LogError::AeronTimedOut(_)));
    let refused = LogError::aeron_add(
        "add_subscription",
        "aeron:udp?endpoint=bad",
        &AeronCError::from_code(AeronErrorType::GenericError.code()),
    );
    assert!(!refused.is_transient());
    assert!(
        LogError::catalog_status("register a", reqwest::StatusCode::SERVICE_UNAVAILABLE, "")
            .is_transient()
    );
    assert!(
        !LogError::catalog_status("register a", reqwest::StatusCode::FORBIDDEN, "denied")
            .is_transient()
    );
    assert!(!LogError::Discovery("x".into()).is_transient());
}

#[test]
fn a_classified_error_keeps_its_text() {
    let e = timed_out();
    assert!(
        e.to_string()
            .starts_with("aeron: add_publication aeron:udp?control-mode=dynamic: "),
        "{e}"
    );
    let e = LogError::catalog_status("register a", reqwest::StatusCode::BAD_GATEWAY, "down");
    assert_eq!(
        e.to_string(),
        "discovery: register a: 502 Bad Gateway: down"
    );
}

#[tokio::test(start_paused = true)]
async fn the_open_succeeds_after_transient_failures() {
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let got = retry()
        .run(|| std::future::ready(flaky(&tries, 3, timed_out)))
        .await
        .unwrap();
    assert_eq!(got, 4);
    // The pauses double from the minimum: 100 + 200 + 400 ms.
    assert_eq!(start.elapsed(), Duration::from_millis(700));
}

#[tokio::test(start_paused = true)]
async fn the_open_gives_up_after_the_limit() {
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let got = retry()
        .run(|| std::future::ready(flaky(&tries, u32::MAX, timed_out)))
        .await;
    let error = got.unwrap_err();
    assert_eq!(error.to_string(), timed_out().to_string());
    assert!(matches!(error, LogError::AeronTimedOut(_)));
    // The last pause ends at the limit, and the try there is the last.
    assert_eq!(start.elapsed(), Duration::from_secs(10));
    // Nine pauses reach 10 s: 100, 200, 400, 800 and 1600 ms, three
    // pauses at the 2 s cap, and the 900 ms rest.
    assert_eq!(tries.get(), 10);
}

#[tokio::test(start_paused = true)]
async fn a_non_transient_error_returns_at_once() {
    let tries = Cell::new(0);
    let start = tokio::time::Instant::now();
    let got = retry()
        .run(|| {
            std::future::ready(flaky(&tries, u32::MAX, || {
                LogError::Aeron("add_publication aeron:udp?bad: invalid channel".into())
            }))
        })
        .await;
    assert!(matches!(got, Err(LogError::Aeron(_))));
    assert_eq!(tries.get(), 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

#[test]
fn the_blocking_open_succeeds_after_transient_failures() {
    let quick = WatchTiming {
        backoff_min: Duration::from_millis(1),
        backoff_max: Duration::from_millis(1),
        ..backoff()
    };
    let tries = Cell::new(0);
    let got = StartRetry::new("tx_data subscription", BUDGET, quick)
        .run_blocking(|| {
            flaky(&tries, 2, || {
                LogError::catalog_status(
                    "register a",
                    reqwest::StatusCode::INTERNAL_SERVER_ERROR,
                    "",
                )
            })
        })
        .unwrap();
    assert_eq!(got, 3);
}

#[test]
fn the_blocking_open_returns_a_non_transient_error_at_once() {
    let tries = Cell::new(0);
    let got = retry().run_blocking(|| {
        flaky(&tries, u32::MAX, || {
            LogError::catalog_status("register a", reqwest::StatusCode::FORBIDDEN, "denied")
        })
    });
    assert!(matches!(got, Err(LogError::Discovery(_))));
    assert_eq!(tries.get(), 1);
}
