use super::*;

fn request() -> WebhookRequest {
    WebhookRequest {
        url: "https://example.org/hook".to_string(),
        filter: StatusFilter::all(),
        secret: "s3cret".to_string(),
    }
}

#[test]
fn the_same_request_names_the_same_subscription() {
    let a = Subscription::parse(request()).unwrap();
    let b = Subscription::parse(request()).unwrap();
    assert_eq!(a.id, b.id);
    let mut other = request();
    other.secret = "other".to_string();
    assert_ne!(Subscription::parse(other).unwrap().id, a.id);
}

#[test]
fn bad_registrations_are_refused() {
    let mut ftp = request();
    ftp.url = "ftp://example.org/x".to_string();
    assert!(matches!(
        Subscription::parse(ftp),
        Err(RegisterError::BadUrl(_))
    ));
    let mut empty = request();
    empty.secret = String::new();
    assert_eq!(Subscription::parse(empty), Err(RegisterError::EmptySecret));
}

#[test]
fn the_signature_is_hmac_sha256_of_the_body() {
    let sub = Subscription::parse(request()).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(b"s3cret").unwrap();
    mac.update(b"{}");
    let expected = format!(
        "sha256={}",
        alloy_primitives::hex::encode(mac.finalize().into_bytes())
    );
    assert_eq!(sub.sign(b"{}"), expected);
}
