use std::time::{Duration, Instant};

use super::*;

/// With no media driver, the whole archive connect ends at its one
/// deadline: the driver wait spends it, and the connect gets nothing more.
/// The error names the wait and the reason of the last probe.
#[test]
fn an_archive_connect_with_no_driver_ends_at_its_one_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let started = Instant::now();
    let Err(e) = connect_archive_with_timeout(
        Some(dir.path()),
        &AeronConfig::default(),
        Duration::from_secs(1),
    ) else {
        panic!("no driver, no session");
    };
    let elapsed = started.elapsed();
    let message = e.to_string();
    assert!(message.contains("no active media driver"), "{message}");
    assert!(
        message.contains("CnC file"),
        "the last probe error follows: {message}"
    );
    assert!(elapsed >= Duration::from_secs(1), "{elapsed:?}");
    assert!(
        elapsed < Duration::from_secs(3),
        "one deadline, not two: {elapsed:?}"
    );
}

#[test]
fn the_deadline_shrinks_and_stops_at_zero() {
    let deadline = ConnectDeadline::start(Duration::from_millis(50));
    assert!(deadline.remaining() <= Duration::from_millis(50));
    std::thread::sleep(Duration::from_millis(60));
    assert_eq!(deadline.remaining(), Duration::ZERO);
}
