use std::io;

use super::*;

const TIMEOUT: Duration = Duration::from_millis(20);

fn io_error(kind: io::ErrorKind) -> CacheError {
    CacheError::Redis(io::Error::from(kind).into())
}

fn redis_error(kind: ErrorKind) -> CacheError {
    CacheError::Redis((kind, "test").into())
}

/// A timeout right after a reply keeps the connection. A timeout after
/// a stall of three timeouts with no reply replaces it, and a reply
/// resets the stall.
#[tokio::test]
async fn only_a_long_stall_replaces_the_connection() {
    let link = Link::new(TIMEOUT);
    link.replied();
    assert!(!link.must_replace(&CacheError::Timeout));
    tokio::time::sleep(TIMEOUT.saturating_mul(STALL_TIMEOUTS)).await;
    assert!(link.must_replace(&CacheError::Timeout));
    link.replied();
    assert!(!link.must_replace(&CacheError::Timeout));
    assert!(!link.lost(), "a stall does not lose the connection");
}

/// A connection error replaces the connection at once and marks it
/// lost. A server error on a live connection keeps it, except the
/// errors of a node that is no longer the primary.
#[test]
fn a_connection_error_loses_the_connection_and_a_server_error_does_not() {
    let link = Link::new(TIMEOUT);
    link.replied();
    assert!(!link.must_replace(&redis_error(ErrorKind::UnexpectedReturnType)));
    assert!(link.must_replace(&redis_error(ErrorKind::Server(ServerErrorKind::ReadOnly))));
    assert!(!link.lost());
    assert!(link.must_replace(&io_error(io::ErrorKind::ConnectionReset)));
    assert!(link.lost());
    link.replaced();
    assert!(!link.lost());
}

/// One replacement at a time. Each failed one doubles the pause before
/// the next claim, up to the last backoff, and a success resets it.
#[test]
fn a_failed_replacement_backs_off_and_a_success_resets_it() {
    let link = Link::new(TIMEOUT);
    assert!(link.claim());
    assert!(!link.claim(), "a second replacement waits for the first");
    let pauses: Vec<u128> = (0..6).map(|_| link.next_pause().as_millis()).collect();
    assert_eq!(pauses, [100, 200, 400, 800, 1600, 2000]);
    link.replaced();
    assert_eq!(link.next_pause(), BACKOFF_FIRST);
    assert!(link.claim(), "a success releases the claim");
}
