use super::*;

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
    let expanded = Source::expand(&sentinels).await;
    assert_eq!(addrs(&expanded), ["127.0.0.1:26379", "127.0.0.2:26379"]);
    assert_eq!(expanded[0].redis_settings().password(), Some("secret"));
}

/// A name that does not resolve stays as it is, so the connection
/// attempt reports the error.
#[tokio::test]
async fn a_name_that_does_not_resolve_stays() {
    let sentinels = infos(&["redis://no-such-sentinel.invalid:26379"]);
    let expanded = Source::expand(&sentinels).await;
    assert_eq!(addrs(&expanded), ["no-such-sentinel.invalid:26379"]);
}
