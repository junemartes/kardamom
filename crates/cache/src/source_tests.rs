use std::net::SocketAddr;
use std::time::Duration;

use super::*;

/// A bound that a local lookup meets.
const BOUND: Duration = Duration::from_secs(5);

fn infos(urls: &[&str]) -> Vec<ConnectionInfo> {
    urls.iter()
        .map(|url| url.into_connection_info().unwrap())
        .collect()
}

fn addrs(infos: &[ConnectionInfo]) -> Vec<String> {
    infos.iter().map(|info| info.addr().to_string()).collect()
}

/// Each address appears once, in the configured order, and a Sentinel
/// keeps the credentials of its URL.
#[tokio::test]
async fn an_address_appears_once_and_keeps_its_credentials() {
    let sentinels = infos(&[
        "redis://user:secret@127.0.0.1:26379",
        "redis://127.0.0.2:26379",
        "redis://127.0.0.1:26379",
    ]);
    let expanded = Source::expand(&sentinels, BOUND).await;
    assert_eq!(addrs(&expanded), ["127.0.0.1:26379", "127.0.0.2:26379"]);
    assert_eq!(expanded[0].redis_settings().password(), Some("secret"));
}

/// A name that does not resolve stays as it is, so the connection
/// attempt reports the error.
#[tokio::test]
async fn a_name_that_does_not_resolve_stays() {
    let sentinels = infos(&["redis://no-such-sentinel.invalid:26379"]);
    let expanded = Source::expand(&sentinels, BOUND).await;
    assert_eq!(addrs(&expanded), ["no-such-sentinel.invalid:26379"]);
}

/// A lookup that never answers ends after the bound, and the name stays
/// as it is, as for a lookup that fails.
#[tokio::test(start_paused = true)]
async fn a_lookup_that_never_answers_ends_after_the_bound_and_the_name_stays() {
    let sentinels = infos(&["redis://sentinel.service.consul:26379"]);
    let started = tokio::time::Instant::now();
    let expanded = Source::expand_by(&sentinels, BOUND, |_, _| {
        std::future::pending::<std::io::Result<std::vec::IntoIter<SocketAddr>>>()
    })
    .await;
    assert_eq!(started.elapsed(), BOUND);
    assert_eq!(addrs(&expanded), ["sentinel.service.consul:26379"]);
}
